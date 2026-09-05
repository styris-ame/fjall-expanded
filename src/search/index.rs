use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Bound;

use anyhow::Result;
use fjall::{OptimisticTxDatabase, OptimisticTxKeyspace, OptimisticWriteTx, Slice};

use crate::Readable;

use super::tokenizer::tokenize_positions;

pub(super) const SEPARATOR: u8 = 0;

/// Version-agnostic inverted index operating on raw `id_body` byte keys.
///
/// Postings are laid out one key per `(term, id_body)` pair so updates are
/// independent point inserts and removes. A term's posting list is a prefix
/// scan, already sorted by id body, which lets multi-term queries intersect by
/// merge. Each document's term set is stored separately so reindexing writes
/// only the postings that actually changed. Callers that need versioning fold
/// the version into `id_body`; this core never inspects it.
pub(super) struct Index {
    pub(super) postings: OptimisticTxKeyspace,
    pub(super) documents: OptimisticTxKeyspace,
    pub(super) database: OptimisticTxDatabase,
}

impl Clone for Index {
    fn clone(&self) -> Self {
        Self {
            postings: self.postings.clone(),
            documents: self.documents.clone(),
            database: self.database.clone(),
        }
    }
}

impl Index {
    pub(super) fn index(&self, tx: &mut OptimisticWriteTx, id_body: &[u8], content: &str, min_token_length: usize) -> Result<()> {
        let new_positions = tokenize_positions(content, min_token_length);
        let old_counts = self.document_counts(&*tx, id_body)?;

        for term in old_counts.keys() {
            if !new_positions.contains_key(term) {
                tx.remove(&self.postings, posting_key(term, id_body));
            }
        }
        // Postings carry positions, not just a count, so a term whose count is
        // unchanged can still have moved. Every current term's posting is rewritten
        // to keep its positions fresh.
        for (term, positions) in &new_positions {
            tx.insert(&self.postings, posting_key(term, id_body), encode_posting(positions));
        }

        if new_positions.is_empty() {
            tx.remove(&self.documents, id_body);
        } else {
            let new_counts: BTreeMap<String, u32> =
                new_positions.iter().map(|(term, positions)| (term.clone(), positions.len() as u32)).collect();
            tx.insert(&self.documents, id_body, encode_counts(&new_counts));
        }

        Ok(())
    }

    pub(super) fn remove(&self, tx: &mut OptimisticWriteTx, id_body: &[u8]) -> Result<()> {
        if let Some(bytes) = tx.take(&self.documents, id_body)? {
            for term in decode_counts(bytes.as_ref()).keys() {
                tx.remove(&self.postings, posting_key(term, id_body));
            }
        }
        Ok(())
    }

    /// Scored AND/optional/exclusion search over pre-tokenized clauses.
    ///
    /// Each clause is a set of terms that must all be present (AND). `required`
    /// clauses gate membership — a document must satisfy every one. With no
    /// required clauses the optionals are OR'd instead, and a document must
    /// satisfy at least one to appear. Documents satisfying any negative clause
    /// are dropped.
    ///
    /// Ranking is, in priority order:
    ///   1. coverage — how many optional clauses the document matches. A strict
    ///      gate above the score: matching more of the requested optional words
    ///      always wins (every survivor already matches all required clauses).
    ///   2. relevance — an additive score summed over the matched query terms:
    ///        * word match: `WORD_MATCH_BASE` (10) + the term's length, so any
    ///          match is worth at least 10 and a longer, more specific word more;
    ///        * frequency: +1 per extra occurrence of a term, capped at
    ///          `FREQUENCY_CAP`, a mild tiebreak that can't outweigh matching a
    ///          whole other word;
    ///        * proximity: query terms found in order add `PROXIMITY_ADJACENT_BONUS`
    ///          (8) minus the tokens skipped between them, so adjacency lifts
    ///          "mckinley swab" over the same words apart or reversed, yet stays
    ///          below a single word match so it only refines, never dominates.
    ///   3. id ascending, for a stable order.
    pub(super) fn search_scored(
        &self,
        tx: &impl Readable,
        required: &[Vec<String>],
        optional: &[Vec<String>],
        negative: &[Vec<String>],
    ) -> Result<Vec<Slice>> {
        let mut coverage: HashMap<Vec<u8>, u32> = HashMap::new();

        if required.is_empty() {
            for clause in optional.iter().filter(|clause| !clause.is_empty()) {
                for id in self.intersect_terms(tx, clause)? {
                    *coverage.entry(id).or_insert(0) += 1;
                }
            }
        } else {
            let terms: Vec<String> = required.iter().flatten().cloned().collect::<BTreeSet<_>>().into_iter().collect();
            for id in self.intersect_terms(tx, &terms)? {
                coverage.insert(id, 0);
            }
            for clause in optional.iter().filter(|clause| !clause.is_empty()) {
                for id in self.intersect_terms(tx, clause)? {
                    if let Some(hits) = coverage.get_mut(&id) {
                        *hits += 1;
                    }
                }
            }
        }

        for clause in negative.iter().filter(|clause| !clause.is_empty()) {
            for id in self.intersect_terms(tx, clause)? {
                coverage.remove(&id);
            }
        }

        if coverage.is_empty() {
            return Ok(Vec::new());
        }

        // Relevance is computed per surviving candidate rather than by rescanning
        // whole posting lists: coverage has already done the coarse sorting, and each
        // candidate's postings are a handful of point reads. Terms are visited in
        // query order so proximity can score in-order closeness.
        let ordered = ordered_terms(required, optional);
        let mut ranked: Vec<(Vec<u8>, u32, u64)> = Vec::with_capacity(coverage.len());
        for (id, hits) in coverage {
            let mut relevance = 0u64;
            let mut positions_in_order: Vec<Vec<u32>> = Vec::with_capacity(ordered.len());
            for term in &ordered {
                match tx.get(&self.postings, posting_key(term, &id))? {
                    Some(value) => {
                        let count = decode_count(value.as_ref());
                        relevance += WORD_MATCH_BASE + term.len() as u64;
                        relevance += u64::from(count.saturating_sub(1).min(FREQUENCY_CAP));
                        positions_in_order.push(decode_positions(value.as_ref()));
                    }
                    None => positions_in_order.push(Vec::new()),
                }
            }
            relevance += proximity_boost(&positions_in_order);
            ranked.push((id, hits, relevance));
        }

        ranked.sort_unstable_by(|a, b| b.1.cmp(&a.1).then_with(|| b.2.cmp(&a.2)).then_with(|| a.0.cmp(&b.0)));
        Ok(ranked.into_iter().map(|(id, ..)| Slice::from(id)).collect())
    }

    /// Every id body whose document contains all of `terms`, sorted ascending,
    /// intersecting the per-term posting lists by k-way merge-join.
    fn intersect_terms(&self, tx: &impl Readable, terms: &[String]) -> Result<Vec<Vec<u8>>> {
        if terms.is_empty() {
            return Ok(Vec::new());
        }

        let mut cursors = Vec::with_capacity(terms.len());
        for term in terms {
            let cursor = TermCursor::open(tx, &self.database, &self.postings, term, Bound::Included(term_prefix(term)))?;
            if cursor.current.is_none() {
                return Ok(Vec::new());
            }
            cursors.push(cursor);
        }

        let mut results = Vec::new();
        loop {
            let mut max = cursors[0].current.clone().unwrap();
            for cursor in &cursors[1..] {
                let value = cursor.current.as_ref().unwrap();
                if value > &max {
                    max = value.clone();
                }
            }

            let mut all_equal = true;
            for cursor in cursors.iter_mut() {
                if cursor.current.as_ref().unwrap() < &max {
                    cursor.seek(tx, &self.database, &self.postings, &max)?;
                    match &cursor.current {
                        None => return Ok(results),
                        Some(value) if value != &max => all_equal = false,
                        Some(_) => {}
                    }
                }
            }

            if all_equal {
                results.push(max);
                for cursor in cursors.iter_mut() {
                    cursor.advance()?;
                    if cursor.current.is_none() {
                        return Ok(results);
                    }
                }
            }
        }
    }

    fn document_counts(&self, tx: &impl Readable, id_body: &[u8]) -> Result<BTreeMap<String, u32>> {
        match tx.get(&self.documents, id_body)? {
            Some(bytes) => Ok(decode_counts(bytes.as_ref())),
            None => Ok(BTreeMap::new()),
        }
    }
}

pub(super) fn posting_key(term: &str, id_body: &[u8]) -> Slice {
    let mut key = Vec::with_capacity(term.len() + 1 + id_body.len());
    key.extend_from_slice(term.as_bytes());
    key.push(SEPARATOR);
    key.extend_from_slice(id_body);
    Slice::from(key)
}

fn term_prefix(term: &str) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(term.len() + 1);
    prefix.extend_from_slice(term.as_bytes());
    prefix.push(SEPARATOR);
    prefix
}

fn term_upper(term: &str) -> Vec<u8> {
    let mut upper = Vec::with_capacity(term.len() + 1);
    upper.extend_from_slice(term.as_bytes());
    upper.push(SEPARATOR + 1);
    upper
}

struct TermCursor {
    prefix_len: usize,
    prefix: Vec<u8>,
    upper: Vec<u8>,
    iter: fjall::Iter,
    current: Option<Vec<u8>>,
}

impl TermCursor {
    fn open(
        tx: &impl Readable,
        database: &OptimisticTxDatabase,
        keyspace: &OptimisticTxKeyspace,
        term: &str,
        lower: Bound<Vec<u8>>,
    ) -> Result<Self> {
        let prefix = term_prefix(term);
        let upper = term_upper(term);
        let iter = tx.range(database, keyspace, (lower, Bound::Excluded(upper.clone())));
        let mut cursor = Self {
            prefix_len: prefix.len(),
            prefix,
            upper,
            iter,
            current: None,
        };
        cursor.pull()?;
        Ok(cursor)
    }

    fn seek(
        &mut self,
        tx: &impl Readable,
        database: &OptimisticTxDatabase,
        keyspace: &OptimisticTxKeyspace,
        target: &[u8],
    ) -> Result<()> {
        let mut lower = Vec::with_capacity(self.prefix.len() + target.len());
        lower.extend_from_slice(&self.prefix);
        lower.extend_from_slice(target);
        self.iter = tx.range(database, keyspace, (Bound::Included(lower), Bound::Excluded(self.upper.clone())));
        self.pull()
    }

    fn advance(&mut self) -> Result<()> {
        self.pull()
    }

    fn pull(&mut self) -> Result<()> {
        self.current = match self.iter.next() {
            Some(guard) => Some(guard.key()?.as_ref()[self.prefix_len..].to_vec()),
            None => None,
        };
        Ok(())
    }
}

/// The distinct positive query terms in query order (required clauses first,
/// then optional), first occurrence wins. Order matters so proximity can score
/// terms in the sequence the user typed them.
fn ordered_terms(required: &[Vec<String>], optional: &[Vec<String>]) -> Vec<String> {
    let mut seen = HashMap::new();
    let mut ordered = Vec::new();
    for term in required.iter().chain(optional).flatten() {
        if seen.insert(term.clone(), ()).is_none() {
            ordered.push(term.clone());
        }
    }
    ordered
}

/// Points for matching a query term, before its length is added. Every match is
/// therefore worth at least this much, so no proximity or frequency bonus can let
/// a document win without matching more (or longer) query words.
const WORD_MATCH_BASE: u64 = 10;

/// Ceiling on the extra-occurrence (frequency) bonus a single term contributes, so
/// keyword stuffing can nudge ties but never dominate distinct word matches.
const FREQUENCY_CAP: u32 = 10;

/// Adjacency bonus for two query terms found in order with nothing between them;
/// each skipped token drops the bonus by one, to a floor of zero. Kept below the
/// minimum word match (`WORD_MATCH_BASE` + shortest token) so proximity only
/// refines the ranking of equally-matched documents.
const PROXIMITY_ADJACENT_BONUS: u64 = 8;

/// In-order closeness score for one document, given each ordered query term's
/// positions in it (empty when the term is absent). Anchored at the first present
/// term's earliest position, each following term is matched to its earliest
/// position after the anchor; the bonus decays with the tokens skipped and the
/// anchor advances. A term with no later position (or an absent term) breaks the
/// chain, which re-anchors at that term's earliest position. This is purely
/// additive on top of the base score, so reversed or scattered matches simply
/// earn no bonus rather than being penalized.
fn proximity_boost(positions_in_order: &[Vec<u32>]) -> u64 {
    let mut boost = 0u64;
    let mut anchor: Option<u32> = None;
    for positions in positions_in_order {
        let Some(&first) = positions.first() else {
            anchor = None;
            continue;
        };
        match anchor {
            None => anchor = Some(first),
            Some(previous) => match positions.iter().copied().find(|&pos| pos > previous) {
                Some(next) => {
                    let skipped = u64::from(next - previous - 1);
                    boost += PROXIMITY_ADJACENT_BONUS.saturating_sub(skipped);
                    anchor = Some(next);
                }
                None => anchor = Some(first),
            },
        }
    }
    boost
}

/// Posting value: `varint(count) ++ delta-encoded ascending positions`. The
/// leading count keeps [`decode_count`] (and legacy count-only postings) valid;
/// positions follow for proximity scoring.
pub(super) fn encode_posting(positions: &[u32]) -> Slice {
    let mut buf = Vec::with_capacity(positions.len() + 1);
    write_varint(&mut buf, positions.len() as u64);
    let mut previous = 0u32;
    for &position in positions {
        write_varint(&mut buf, u64::from(position - previous));
        previous = position;
    }
    Slice::from(buf)
}

/// Posting value: the term's occurrence count in that document as a varint,
/// still used by the version-migration path which has no positions to carry.
pub(super) fn encode_count(count: u32) -> Slice {
    let mut buf = Vec::with_capacity(5);
    write_varint(&mut buf, count as u64);
    Slice::from(buf)
}

/// Decodes a posting value back to a count (its leading varint). Legacy empty
/// postings (written before counts existed) decode as 1.
pub(super) fn decode_count(bytes: &[u8]) -> u32 {
    let mut pos = 0;
    match read_varint(bytes, &mut pos) {
        Some(value) => value.min(u32::MAX as u64) as u32,
        None => 1,
    }
}

/// Decodes a posting value's positions. The leading count is skipped, then the
/// delta-encoded positions are accumulated. Legacy count-only postings have no
/// deltas and yield an empty list (no proximity contribution until reindexed).
pub(super) fn decode_positions(bytes: &[u8]) -> Vec<u32> {
    let mut pos = 0;
    if read_varint(bytes, &mut pos).is_none() {
        return Vec::new();
    }
    let mut positions = Vec::new();
    let mut current = 0u64;
    while let Some(delta) = read_varint(bytes, &mut pos) {
        current += delta;
        positions.push(current.min(u64::from(u32::MAX)) as u32);
    }
    positions
}

/// Documents value: each `(term, count)` as `varint(len) ++ term ++ varint(count)`,
/// length-prefixed so terms containing any byte stay unambiguous.
pub(super) fn encode_counts(counts: &BTreeMap<String, u32>) -> Slice {
    let mut buf = Vec::new();
    for (term, count) in counts {
        write_varint(&mut buf, term.len() as u64);
        buf.extend_from_slice(term.as_bytes());
        write_varint(&mut buf, *count as u64);
    }
    Slice::from(buf)
}

pub(super) fn decode_counts(bytes: &[u8]) -> BTreeMap<String, u32> {
    let mut counts = BTreeMap::new();
    let mut pos = 0;
    while pos < bytes.len() {
        let Some(len) = read_varint(bytes, &mut pos) else {
            break;
        };
        let end = pos + len as usize;
        if end > bytes.len() {
            break;
        }
        let term = String::from_utf8_lossy(&bytes[pos..end]).into_owned();
        pos = end;
        let Some(count) = read_varint(bytes, &mut pos) else {
            break;
        };
        counts.insert(term, count.min(u32::MAX as u64) as u32);
    }
    counts
}

fn write_varint(buf: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            buf.push(byte);
            return;
        }
        buf.push(byte | 0x80);
    }
}

fn read_varint(bytes: &[u8], pos: &mut usize) -> Option<u64> {
    let mut result = 0u64;
    let mut shift = 0;
    loop {
        let byte = *bytes.get(*pos)?;
        *pos += 1;
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Some(result);
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
}
