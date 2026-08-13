use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Bound;

use anyhow::Result;
use fjall::{OptimisticTxDatabase, OptimisticTxKeyspace, OptimisticWriteTx, Slice};

use crate::Readable;

use super::tokenizer::tokenize_counts;

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
        let new_counts = tokenize_counts(content, min_token_length);
        let old_counts = self.document_counts(&*tx, id_body)?;

        for term in old_counts.keys() {
            if !new_counts.contains_key(term) {
                tx.remove(&self.postings, posting_key(term, id_body));
            }
        }
        // A term still present but with a changed count needs its posting value
        // rewritten, so write whenever the count differs (or is new).
        for (term, count) in &new_counts {
            if old_counts.get(term) != Some(count) {
                tx.insert(&self.postings, posting_key(term, id_body), encode_count(*count));
            }
        }

        if new_counts.is_empty() {
            tx.remove(&self.documents, id_body);
        } else {
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
    /// Ranking is, in strict priority order:
    ///   1. coverage — how many optional clauses the document matches (every
    ///      survivor already matches all required ones, so they don't separate);
    ///   2. length — total length of the distinct query terms it matched, so a hit
    ///      on a long, specific word outweighs one on a short, common word;
    ///   3. frequency — total occurrences of those terms, the last resort, so mere
    ///      repetition only breaks ties between otherwise-equal matches;
    ///   4. id ascending, for a stable order.
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

        // Frequency is the lowest-priority tiebreak, so it's read per surviving
        // candidate rather than by rescanning whole posting lists: a popular term
        // elsewhere never costs us, and coverage has already done the real sorting.
        let positive: BTreeSet<&String> = required.iter().chain(optional).flatten().collect();
        let mut ranked: Vec<(Vec<u8>, u32, u64, u64)> = Vec::with_capacity(coverage.len());
        for (id, hits) in coverage {
            let mut length = 0u64;
            let mut frequency = 0u64;
            for term in &positive {
                if let Some(value) = tx.get(&self.postings, posting_key(term, &id))? {
                    length += term.len() as u64;
                    frequency += decode_count(value.as_ref()) as u64;
                }
            }
            ranked.push((id, hits, length, frequency));
        }

        ranked.sort_unstable_by(|a, b| {
            b.1.cmp(&a.1).then_with(|| b.2.cmp(&a.2)).then_with(|| b.3.cmp(&a.3)).then_with(|| a.0.cmp(&b.0))
        });
        Ok(ranked.into_iter().map(|(id, _, _, _)| Slice::from(id)).collect())
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

/// Posting value: the term's occurrence count in that document as a varint.
pub(super) fn encode_count(count: u32) -> Slice {
    let mut buf = Vec::with_capacity(5);
    write_varint(&mut buf, count as u64);
    Slice::from(buf)
}

/// Decodes a posting value back to a count. Legacy empty postings (written
/// before counts existed) decode as 1.
pub(super) fn decode_count(bytes: &[u8]) -> u32 {
    let mut pos = 0;
    match read_varint(bytes, &mut pos) {
        Some(value) => value.min(u32::MAX as u64) as u32,
        None => 1,
    }
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
