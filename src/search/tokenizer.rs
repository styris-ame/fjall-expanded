use std::collections::{BTreeMap, BTreeSet};

use charabia::Tokenize;

pub const DEFAULT_MIN_TOKEN_LENGTH: usize = 3;

/// Normalizes `text` into a sorted, de-duplicated set of word lemmas, dropping
/// any shorter than `min_length` bytes. charabia handles lowercasing, accent
/// folding, and script-aware word segmentation, so queries match regardless of
/// case or diacritics.
pub fn tokenize(text: &str, min_length: usize) -> BTreeSet<String> {
    text.tokenize()
        .filter(|token| token.is_word())
        .map(|token| token.lemma().to_string())
        .filter(|lemma| lemma.len() >= min_length)
        .collect()
}

/// Like [`tokenize`] but keeps each lemma's occurrence count, so the index can
/// rank documents by term frequency. Counts saturate at `u32::MAX`.
pub fn tokenize_counts(text: &str, min_length: usize) -> BTreeMap<String, u32> {
    let mut counts = BTreeMap::new();
    for token in text.tokenize().filter(|token| token.is_word()) {
        let lemma = token.lemma();
        if lemma.len() < min_length {
            continue;
        }
        let entry = counts.entry(lemma.to_string()).or_insert(0u32);
        *entry = entry.saturating_add(1);
    }
    counts
}

/// Like [`tokenize_counts`] but keeps each lemma's ordered positions (0-based
/// index among the length-passing word tokens), so the index can score in-order
/// term proximity. Positions per lemma are ascending; kept tokens are numbered
/// consecutively, so two words written next to each other are one apart.
pub fn tokenize_positions(text: &str, min_length: usize) -> BTreeMap<String, Vec<u32>> {
    let mut positions: BTreeMap<String, Vec<u32>> = BTreeMap::new();
    let mut index = 0u32;
    for token in text.tokenize().filter(|token| token.is_word()) {
        let lemma = token.lemma();
        if lemma.len() < min_length {
            continue;
        }
        positions.entry(lemma.to_string()).or_default().push(index);
        index = index.saturating_add(1);
    }
    positions
}

/// A word token's normalized lemma alongside its byte span in the source text,
/// for callers that need to highlight matches in the original string.
pub struct WordSpan {
    pub byte_start: usize,
    pub byte_end: usize,
    pub lemma: String,
}

/// Returns every word token with its source byte span and lemma, unfiltered by
/// length so callers can render surrounding context words. Match against the
/// length-filtered output of [`tokenize`] to decide which spans to highlight.
pub fn tokenize_spans(text: &str) -> Vec<WordSpan> {
    text.tokenize()
        .filter(|token| token.is_word())
        .map(|token| WordSpan {
            byte_start: token.byte_start,
            byte_end: token.byte_end,
            lemma: token.lemma().to_string(),
        })
        .collect()
}
