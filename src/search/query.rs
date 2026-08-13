use std::collections::BTreeSet;

use super::tokenizer::tokenize;

/// A parsed search query split into required, optional, and negative clauses.
///
/// Each clause is a set of lemmas that must *all* be present for the clause to
/// be satisfied (AND within a clause). A bare word yields a single-lemma clause;
/// a quoted phrase yields one clause holding every lemma in the phrase.
///
/// Required clauses are AND'd: a document must satisfy every one of them. Optional
/// clauses (`?word` / `?"phrase"`) never gate a match when a required clause
/// exists — they only add to the score so better matches rank higher. When there
/// are no required clauses, at least one optional clause must match. A document
/// satisfying any negative clause is excluded outright.
pub struct ParsedQuery {
    pub required: Vec<Vec<String>>,
    pub optional: Vec<Vec<String>>,
    pub negative: Vec<Vec<String>>,
}

impl ParsedQuery {
    /// Every lemma that should be highlighted in a snippet: the union of all
    /// required and optional clauses. Negatives are never highlighted.
    pub fn highlight_lemmas(&self) -> BTreeSet<String> {
        self.required.iter().chain(&self.optional).flatten().cloned().collect()
    }

    /// True when there is nothing to match on: no required and no optional clause.
    /// A query of only negatives can never select a document.
    pub fn is_empty(&self) -> bool {
        self.required.is_empty() && self.optional.is_empty()
    }
}

/// Which bucket a clause lands in, chosen by its single leading marker.
enum Kind {
    Required,
    Optional,
    Negative,
}

/// Parses `query` into clauses. Unquoted runs become one clause per word; a
/// `"quoted phrase"` becomes a single clause whose lemmas are AND'd. A single
/// leading marker selects the clause kind: `-` negates, `?` makes the clause
/// optional, anything else is required. Items whose text yields no lemmas (too
/// short, all punctuation) are dropped.
pub fn parse_query(query: &str, min_token_length: usize) -> ParsedQuery {
    let mut required = Vec::new();
    let mut optional = Vec::new();
    let mut negative = Vec::new();
    let mut chars = query.chars().peekable();

    while let Some(&c) = chars.peek() {
        if c.is_whitespace() {
            chars.next();
            continue;
        }

        let kind = match c {
            '-' => {
                chars.next();
                Kind::Negative
            }
            '?' => {
                chars.next();
                Kind::Optional
            }
            _ => Kind::Required,
        };

        let text = if chars.peek() == Some(&'"') {
            chars.next();
            let mut phrase = String::new();
            for c in chars.by_ref() {
                if c == '"' {
                    break;
                }
                phrase.push(c);
            }
            phrase
        } else {
            let mut word = String::new();
            while let Some(&c) = chars.peek() {
                if c.is_whitespace() {
                    break;
                }
                word.push(c);
                chars.next();
            }
            word
        };

        let lemmas: Vec<String> = tokenize(&text, min_token_length).into_iter().collect();
        if lemmas.is_empty() {
            continue;
        }
        match kind {
            Kind::Required => required.push(lemmas),
            Kind::Optional => optional.push(lemmas),
            Kind::Negative => negative.push(lemmas),
        }
    }

    ParsedQuery {
        required,
        optional,
        negative,
    }
}

#[cfg(test)]
mod tests {
    use super::parse_query;

    fn clause(words: &[&str]) -> Vec<String> {
        words.iter().map(|w| w.to_string()).collect()
    }

    #[test]
    fn bare_words_are_separate_required_clauses() {
        let parsed = parse_query("apple banana", 3);
        assert_eq!(parsed.required, vec![clause(&["apple"]), clause(&["banana"])]);
        assert!(parsed.optional.is_empty());
        assert!(parsed.negative.is_empty());
    }

    #[test]
    fn quoted_phrase_is_one_anded_required_clause() {
        // Lemmas within a clause are sorted (BTreeSet) so order is deterministic.
        let parsed = parse_query("\"red fox\"", 3);
        assert_eq!(parsed.required, vec![clause(&["fox", "red"])]);
    }

    #[test]
    fn leading_question_mark_makes_word_and_quote_optional() {
        let parsed = parse_query("?maybe ?\"red fox\"", 3);
        assert!(parsed.required.is_empty());
        assert_eq!(parsed.optional, vec![clause(&["maybe"]), clause(&["fox", "red"])]);
    }

    #[test]
    fn leading_dash_negates_word_and_quote() {
        let parsed = parse_query("-bad -\"new york\"", 3);
        assert!(parsed.required.is_empty());
        assert_eq!(parsed.negative, vec![clause(&["bad"]), clause(&["new", "york"])]);
    }

    #[test]
    fn mixed_query_buckets_each_item() {
        let parsed = parse_query("apple ?maybe -bad \"red fox\"", 3);
        assert_eq!(parsed.required, vec![clause(&["apple"]), clause(&["fox", "red"])]);
        assert_eq!(parsed.optional, vec![clause(&["maybe"])]);
        assert_eq!(parsed.negative, vec![clause(&["bad"])]);
    }

    #[test]
    fn short_tokens_are_dropped() {
        let parsed = parse_query("a apple ?b", 3);
        assert_eq!(parsed.required, vec![clause(&["apple"])]);
        assert!(parsed.optional.is_empty());
    }
}
