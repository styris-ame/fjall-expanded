use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

use anyhow::{Context, Result, ensure};
use fjall::{OptimisticTxDatabase, OptimisticTxKeyspace, OptimisticWriteTx, Slice};

use crate::Readable;

use super::tokenizer::tokenize_positions;

pub(super) const SEPARATOR: u8 = 0;

/// Bytes the length of a document's id takes at the front of each of its bodies
pub(super) const HEAD_LEN: usize = 2;

/// Decides whether a part of a document counts for a search, given the document's
/// id and the part's name. A document's main text always counts and is never
/// asked about.
pub(super) type Visible<'a> = dyn FnMut(&[u8], &[u8]) -> bool + 'a;

/// A term's postings in one document, one for each part that counts and holds it
type Found = Vec<(Vec<u8>, Slice)>;

/// Version-agnostic inverted index operating on raw document ids.
///
/// A document is made of parts: its main text, which every search counts, and
/// any number of named parts a search can leave out, so one document can show
/// each searcher only what they may see. Each part is stored under its own id
/// body, `u16 length of the document id ++ document id ++ part name`, where the
/// main text's name is empty. The length keeps a document's parts next to each
/// other and apart from any other document's, so a document can always be
/// split back out of a body.
///
/// Postings are laid out one key per `(term, id_body)` pair so updates are
/// independent point inserts and removes. A term's posting list is a prefix
/// scan, already sorted by id body, which lets multi-term queries intersect by
/// merge. Each part's term set is stored separately so reindexing writes only
/// the postings that actually changed. Callers that need versioning fold the
/// version into the document id; this core never inspects it.
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
    /// Indexes a document as `parts`, each a part's name and its text, where the
    /// empty name is the main text. Any part the document held before that isn't
    /// among them is removed, and a part without words isn't kept.
    pub(super) fn index(
        &self,
        tx: &mut OptimisticWriteTx,
        doc: &[u8],
        parts: &[(&[u8], &str)],
        min_token_length: usize,
    ) -> Result<()> {
        let head = head_of(doc)?;
        for (at, (part, _)) in parts.iter().enumerate() {
            ensure!(parts[..at].iter().all(|(other, _)| other != part), "a document can't hold the same part twice");
        }
        let mut stale = self.bodies(&*tx, &head)?;
        for (part, content) in parts {
            let body = [head.as_slice(), part].concat();
            stale.retain(|stale| stale.as_ref() != body.as_slice());
            self.index_body(tx, &body, content, min_token_length)?;
        }
        for body in stale {
            self.remove_body(tx, &body)?;
        }
        Ok(())
    }

    /// Removes every part of a document
    pub(super) fn remove(&self, tx: &mut OptimisticWriteTx, doc: &[u8]) -> Result<()> {
        for body in self.bodies(&*tx, &head_of(doc)?)? {
            self.remove_body(tx, &body)?;
        }
        Ok(())
    }

    /// The bodies of every part a document holds, found by its head
    fn bodies(&self, tx: &impl Readable, head: &[u8]) -> Result<Vec<Slice>> {
        let mut bodies = Vec::new();
        for guard in tx.prefix(&self.database, &self.documents, head) {
            bodies.push(guard.key()?);
        }
        Ok(bodies)
    }

    fn index_body(&self, tx: &mut OptimisticWriteTx, id_body: &[u8], content: &str, min_token_length: usize) -> Result<()> {
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

    fn remove_body(&self, tx: &mut OptimisticWriteTx, id_body: &[u8]) -> Result<()> {
        if let Some(bytes) = tx.take(&self.documents, id_body)? {
            for term in decode_counts(bytes.as_ref()).keys() {
                tx.remove(&self.postings, posting_key(term, id_body));
            }
        }
        Ok(())
    }

    /// Scored AND/optional/exclusion search over pre-tokenized clauses, giving the
    /// ids of the documents found.
    ///
    /// Each clause is a set of terms that must all be present (AND). `required`
    /// clauses gate membership — a document must satisfy every one. With no
    /// required clauses the optionals are OR'd instead, and a document must
    /// satisfy at least one to appear. Documents satisfying any negative clause
    /// are dropped.
    ///
    /// A document is searched as the parts of it that count, as if they were one
    /// text: a term is in it when any of them holds it, so a clause can be met by
    /// terms from different parts, and a part that doesn't count can neither find
    /// the document nor keep it out.
    ///
    /// Ranking is, in priority order:
    ///   1. coverage — how many optional clauses the document matches. A strict
    ///      gate above the score: matching more of the requested optional words
    ///      always wins (every survivor already matches all required clauses).
    ///   2. relevance — an additive score summed over the matched query terms:
    ///        * word match: `WORD_MATCH_BASE` (10) + the term's length, so any
    ///          match is worth at least 10 and a longer, more specific word more;
    ///        * frequency: +1 per extra occurrence of a term over the parts that
    ///          count, capped at `FREQUENCY_CAP`, a mild tiebreak that can't
    ///          outweigh matching a whole other word;
    ///        * proximity: query terms found in order add `PROXIMITY_ADJACENT_BONUS`
    ///          (8) minus the tokens skipped between them, so adjacency lifts
    ///          "mckinley swab" over the same words apart or reversed, yet stays
    ///          below a single word match so it only refines, never dominates.
    ///          Positions only compare within a part, so the closest part counts.
    ///   3. id ascending, for a stable order.
    pub(super) fn search_scored(
        &self,
        tx: &impl Readable,
        required: &[Vec<String>],
        optional: &[Vec<String>],
        negative: &[Vec<String>],
        visible: &mut Visible,
    ) -> Result<Vec<Slice>> {
        // Positive terms come first, in query order so proximity can score in-order
        // closeness, then the terms only negatives use.
        let scored = distinct_terms(required.iter().chain(optional)).len();
        let terms = distinct_terms(required.iter().chain(optional).chain(negative));
        let column = |term: &String| terms.iter().position(|t| t == term).unwrap();

        // The required terms' postings come along with the documents holding them
        // all, so only the other terms' lists are walked again
        let required_terms: Vec<String> = required.iter().flatten().cloned().collect::<BTreeSet<_>>().into_iter().collect();
        let (candidates, mut postings) = if required.is_empty() {
            let mut union = BTreeSet::new();
            for clause in optional.iter().filter(|clause| !clause.is_empty()) {
                union.extend(self.intersect_terms(tx, clause, visible)?.into_iter().map(|(head, _)| head));
            }
            let candidates: Vec<Vec<u8>> = union.into_iter().collect();
            let postings = vec![Vec::new(); candidates.len() * terms.len()];
            (candidates, postings)
        } else {
            let found = self.intersect_terms(tx, &required_terms, visible)?;
            let mut postings = vec![Vec::new(); found.len() * terms.len()];
            let mut candidates = Vec::with_capacity(found.len());
            for (row, (head, each)) in found.into_iter().enumerate() {
                for (term, found) in required_terms.iter().zip(each) {
                    postings[row * terms.len() + column(term)] = found;
                }
                candidates.push(head);
            }
            (candidates, postings)
        };
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let walked: Vec<usize> = (0..terms.len()).filter(|&at| !required_terms.contains(&terms[at])).collect();
        self.candidate_postings(tx, &terms, &walked, &candidates, &mut postings, visible)?;
        let columns = |clauses: &[Vec<String>]| -> Vec<Vec<usize>> {
            clauses.iter().filter(|clause| !clause.is_empty()).map(|clause| clause.iter().map(column).collect()).collect()
        };
        let (optional_columns, negative_columns) = (columns(optional), columns(negative));

        let mut ranked: Vec<(Vec<u8>, usize, u64)> = Vec::with_capacity(candidates.len());
        for (row, head) in postings.chunks_exact(terms.len()).zip(candidates) {
            let matches = |clause: &Vec<usize>| clause.iter().all(|&column| !row[column].is_empty());
            if negative_columns.iter().any(matches) {
                continue;
            }
            let hits = optional_columns.iter().filter(|clause| matches(clause)).count();
            ranked.push((head, hits, relevance(&terms[..scored], &row[..scored])));
        }

        ranked.sort_unstable_by(|a, b| b.1.cmp(&a.1).then_with(|| b.2.cmp(&a.2)).then_with(|| a.0.cmp(&b.0)));
        Ok(ranked.into_iter().map(|(head, ..)| Slice::from(&head[HEAD_LEN..])).collect())
    }

    /// Fills in the postings of the terms at `columns` for each of the documents
    /// `heads` lead, sorted, a row of terms per document. Every term's list is
    /// walked once alongside the documents rather than read a point at a time. A
    /// point read checks the table's bloom filter, and a filter too big for the
    /// block cache is reloaded on every read.
    fn candidate_postings(
        &self,
        tx: &impl Readable,
        terms: &[String],
        columns: &[usize],
        heads: &[Vec<u8>],
        postings: &mut [Found],
        visible: &mut Visible,
    ) -> Result<()> {
        for &column in columns {
            let mut cursor = TermCursor::open(tx, &self.database, &self.postings, &terms[column])?;
            for (row, head) in heads.iter().enumerate() {
                cursor.seek(head)?;
                postings[row * terms.len() + column] = cursor.take_document(head, visible)?;
                if cursor.current.is_none() {
                    break;
                }
            }
        }
        Ok(())
    }

    /// Every document whose parts that count hold all of `terms` between them,
    /// sorted by head, with each term's postings in it, intersecting the per-term
    /// posting lists document by document with a k-way merge-join.
    fn intersect_terms(&self, tx: &impl Readable, terms: &[String], visible: &mut Visible) -> Result<Vec<(Vec<u8>, Vec<Found>)>> {
        if terms.is_empty() {
            return Ok(Vec::new());
        }

        let mut cursors = Vec::with_capacity(terms.len());
        for term in terms {
            let mut cursor = TermCursor::open(tx, &self.database, &self.postings, term)?;
            cursor.settle(visible)?;
            if cursor.head().is_none() {
                return Ok(Vec::new());
            }
            cursors.push(cursor);
        }

        let mut results = Vec::new();
        loop {
            let mut max = cursors[0].head().unwrap().to_vec();
            for cursor in &cursors[1..] {
                let head = cursor.head().unwrap();
                if head > max.as_slice() {
                    max = head.to_vec();
                }
            }

            let mut all_equal = true;
            for cursor in cursors.iter_mut() {
                if cursor.head().is_some_and(|head| head < max.as_slice()) {
                    cursor.seek(&max)?;
                    cursor.settle(visible)?;
                }
                match cursor.head() {
                    None => return Ok(results),
                    Some(head) if head != max.as_slice() => all_equal = false,
                    Some(_) => {}
                }
            }

            if all_equal {
                let mut found = Vec::with_capacity(cursors.len());
                for cursor in cursors.iter_mut() {
                    found.push(cursor.take_document(&max, visible)?);
                    cursor.settle(visible)?;
                }
                let done = cursors.iter().any(|cursor| cursor.head().is_none());
                results.push((max, found));
                if done {
                    return Ok(results);
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

/// What every body of a document starts with: its id behind the id's length
pub(super) fn head_of(doc: &[u8]) -> Result<Vec<u8>> {
    let len = u16::try_from(doc.len()).context("a document's id can't be longer than 64 KiB")?;
    let mut head = Vec::with_capacity(HEAD_LEN + doc.len());
    head.extend_from_slice(&len.to_be_bytes());
    head.extend_from_slice(doc);
    Ok(head)
}

/// A document's main `content` and its named `parts`, as [`Index::index`] takes them
pub(super) fn named_parts<'a, S: AsRef<str>>(content: &'a str, parts: &'a [(Slice, S)]) -> Result<Vec<(&'a [u8], &'a str)>> {
    let mut named: Vec<(&[u8], &str)> = Vec::with_capacity(parts.len() + 1);
    named.push((&[], content));
    for (part, text) in parts {
        ensure!(!part.is_empty(), "a document's part needs a name");
        named.push((part.as_ref(), text.as_ref()));
    }
    Ok(named)
}

/// A body split into its document's head and the part's name
pub(super) fn split_body(body: &[u8]) -> (&[u8], &[u8]) {
    let len = match body {
        [high, low, ..] => usize::from(u16::from_be_bytes([*high, *low])),
        _ => 0,
    };
    body.split_at((HEAD_LEN + len).min(body.len()))
}

/// Whether a part of the document `head` leads counts for a search
fn counts(visible: &mut Visible, head: &[u8], part: &[u8]) -> bool {
    part.is_empty() || visible(&head[HEAD_LEN.min(head.len())..], part)
}

/// How many postings [`TermCursor::seek`] steps over before reopening its range
/// at the target instead. Opening a range costs a dozen steps or so with its
/// index block cached, and far more when the block has to be read again.
const SEEK_STEPS: usize = 64;

/// Walks one term's posting list in id order, holding the current id body and posting.
struct TermCursor<'a, R: Readable> {
    tx: &'a R,
    database: &'a OptimisticTxDatabase,
    keyspace: &'a OptimisticTxKeyspace,
    prefix: Vec<u8>,
    upper: Vec<u8>,
    iter: fjall::Iter,
    current: Option<(Vec<u8>, Slice)>,
}

impl<'a, R: Readable> TermCursor<'a, R> {
    fn open(tx: &'a R, database: &'a OptimisticTxDatabase, keyspace: &'a OptimisticTxKeyspace, term: &str) -> Result<Self> {
        let mut prefix = Vec::with_capacity(term.len() + 1);
        prefix.extend_from_slice(term.as_bytes());
        prefix.push(SEPARATOR);
        let mut upper = prefix.clone();
        *upper.last_mut().unwrap() = SEPARATOR + 1;
        let iter = tx.range(database, keyspace, (Bound::Included(prefix.clone()), Bound::Excluded(upper.clone())));
        let mut cursor = Self {
            tx,
            database,
            keyspace,
            prefix,
            upper,
            iter,
            current: None,
        };
        cursor.pull()?;
        Ok(cursor)
    }

    fn id(&self) -> Option<&[u8]> {
        self.current.as_ref().map(|(id, _)| id.as_slice())
    }

    /// The head of the document the current posting is in
    fn head(&self) -> Option<&[u8]> {
        self.id().map(|body| split_body(body).0)
    }

    /// Moves to the first posting at or after `target`, never backwards. Near
    /// targets are stepped to and far ones reopen the range there.
    fn seek(&mut self, target: &[u8]) -> Result<()> {
        for _ in 0..SEEK_STEPS {
            if self.id().is_none_or(|id| id >= target) {
                return Ok(());
            }
            self.pull()?;
        }
        if self.id().is_none_or(|id| id >= target) {
            return Ok(());
        }
        let mut lower = Vec::with_capacity(self.prefix.len() + target.len());
        lower.extend_from_slice(&self.prefix);
        lower.extend_from_slice(target);
        self.iter = self.tx.range(self.database, self.keyspace, (Bound::Included(lower), Bound::Excluded(self.upper.clone())));
        self.pull()
    }

    /// Steps over postings in parts that don't count, to rest on one that does
    fn settle(&mut self, visible: &mut Visible) -> Result<()> {
        while let Some(body) = self.id() {
            let (head, part) = split_body(body);
            if counts(visible, head, part) {
                return Ok(());
            }
            self.pull()?;
        }
        Ok(())
    }

    /// The postings in parts that count of the document `head` leads, from where
    /// the cursor is, moving past all of that document's
    fn take_document(&mut self, head: &[u8], visible: &mut Visible) -> Result<Found> {
        let mut found = Vec::new();
        while let Some((body, posting)) = &self.current {
            let (at, part) = split_body(body);
            if at != head {
                break;
            }
            if counts(visible, at, part) {
                found.push((part.to_vec(), posting.clone()));
            }
            self.pull()?;
        }
        Ok(found)
    }

    fn pull(&mut self) -> Result<()> {
        self.current = match self.iter.next() {
            Some(guard) => {
                let (key, value) = guard.into_inner()?;
                Some((key.as_ref()[self.prefix.len()..].to_vec(), value))
            }
            None => None,
        };
        Ok(())
    }
}

/// The distinct terms of `clauses` in the order given, first occurrence wins.
fn distinct_terms<'a>(clauses: impl IntoIterator<Item = &'a Vec<String>>) -> Vec<String> {
    let mut terms: Vec<String> = Vec::new();
    for term in clauses.into_iter().flatten() {
        if !terms.contains(term) {
            terms.push(term.clone());
        }
    }
    terms
}

/// A document's relevance, given each positive query term in query order and the
/// postings the document's parts that count have for it.
fn relevance(terms: &[String], postings: &[Found]) -> u64 {
    let mut relevance = 0u64;
    for (term, found) in terms.iter().zip(postings) {
        if found.is_empty() {
            continue;
        }
        let count = found.iter().map(|(_, posting)| decode_count(posting.as_ref())).fold(0, u32::saturating_add);
        relevance += WORD_MATCH_BASE + term.len() as u64;
        relevance += u64::from(count.saturating_sub(1).min(FREQUENCY_CAP));
    }
    let parts: BTreeSet<&[u8]> = postings.iter().flatten().map(|(part, _)| part.as_slice()).collect();
    let closest = parts
        .into_iter()
        .map(|part| {
            let positions_in_order: Vec<Vec<u32>> = postings
                .iter()
                .map(|found| match found.iter().find(|(other, _)| other.as_slice() == part) {
                    Some((_, posting)) => decode_positions(posting.as_ref()),
                    None => Vec::new(),
                })
                .collect();
            proximity_boost(&positions_in_order)
        })
        .max()
        .unwrap_or(0);
    relevance + closest
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
