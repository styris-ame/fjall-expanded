use std::collections::BTreeSet;
use std::marker::PhantomData;

use anyhow::Result;
use fjall::{OptimisticWriteTx, Slice};

use crate::key_slice::ToKeySlice;
use crate::managed_database::{SpaceGroup, SpaceOptions};
use crate::store_key::SpaceKey;
use crate::{KeySource, Readable};

use super::index::{Index, Visible, named_parts};
use super::query::parse_query;
use super::tokenizer::DEFAULT_MIN_TOKEN_LENGTH;

/// An inverted index over documents keyed by any `K` that can be turned into a
/// byte key (`ToKeySlice<K>`), exactly like [`StoreSpace`](crate::StoreSpace).
///
/// Thin, unversioned wrapper around [`Index`]; the document id is the raw key
/// slice. For migratable keys use [`VersionedSearchSpace`](crate::VersionedSearchSpace).
pub struct SearchSpace<K: KeySource<K>> {
    index: Index,
    min_token_length: usize,
    _marker: PhantomData<fn() -> K>,
}

impl<K: KeySource<K>> Clone for SearchSpace<K> {
    fn clone(&self) -> Self {
        Self {
            index: self.index.clone(),
            min_token_length: self.min_token_length,
            _marker: PhantomData,
        }
    }
}

impl<K: KeySource<K>> SearchSpace<K> {
    /// Opens the two backing keyspaces (`postings` and `documents`) within the
    /// given space group.
    pub fn open(group: &mut SpaceGroup, options: SpaceOptions) -> Result<Self> {
        let postings = group.open_keyspace(group.full_name("postings"), options.clone())?;
        let documents = group.open_keyspace(group.full_name("documents"), options)?;
        let database = group.database().clone();
        Ok(Self {
            index: Index {
                postings,
                documents,
                database,
            },
            min_token_length: DEFAULT_MIN_TOKEN_LENGTH,
            _marker: PhantomData,
        })
    }

    /// Opens the backing keyspaces. An unversioned search index has nothing to
    /// migrate, so this is just [`open`](Self::open) under the name its versioned
    /// siblings use, letting a whole store be declared with one consistent call.
    pub fn stage_open(group: &mut SpaceGroup, options: SpaceOptions) -> Result<Self> {
        Self::open(group, options)
    }

    pub fn min_token_length(mut self, length: usize) -> Self {
        self.min_token_length = length;
        self
    }

    /// Indexes `content` as all of `doc_id`'s document, diffing against what it
    /// stored before so only the changed postings are written, and replacing any
    /// parts it held.
    pub fn index_document(&self, tx: &mut OptimisticWriteTx, doc_id: impl ToKeySlice<K>, content: &str) -> Result<()> {
        self.index.index(tx, doc_id.to_slice().as_ref(), &[(&[], content)], self.min_token_length)
    }

    /// Indexes `doc_id`'s document as its main `content`, which every search
    /// counts, and named `parts`, which [`search_visible`](Self::search_visible)
    /// only counts where it's told to. Whatever the document held before is
    /// replaced. A part's name can't encode to nothing, and each comes up once.
    pub fn index_parts<P, T, S>(
        &self,
        tx: &mut OptimisticWriteTx,
        doc_id: impl ToKeySlice<K>,
        content: &str,
        parts: impl IntoIterator<Item = (T, S)>,
    ) -> Result<()>
    where
        P: KeySource<P>,
        T: ToKeySlice<P>,
        S: AsRef<str>,
    {
        let parts: Vec<(Slice, S)> = parts.into_iter().map(|(part, text)| (part.to_slice(), text)).collect();
        let parts = named_parts(content, &parts)?;
        self.index.index(tx, doc_id.to_slice().as_ref(), &parts, self.min_token_length)
    }

    /// Removes every part of `doc_id`'s document and forgets their stored terms.
    pub fn remove_document(&self, tx: &mut OptimisticWriteTx, doc_id: impl ToKeySlice<K>) -> Result<()> {
        self.index.remove(tx, doc_id.to_slice().as_ref())
    }

    /// Returns the ids of every document matching `query`, ranked best-first,
    /// with all of each document's parts counted.
    ///
    /// `query` honors the full syntax: unquoted words are required (AND'd), a
    /// `"quoted phrase"` requires all its words, a `?` prefix makes a word or
    /// phrase optional (it only boosts the score), a leading `-` excludes, and
    /// documents matching more optional clauses rank ahead of the rest.
    pub fn search(&self, tx: &impl Readable, query: &str) -> Result<Vec<SpaceKey<K>>> {
        self.search_parts(tx, query, &mut |_, _| true)
    }

    /// Like [`search`](Self::search), but a document's named parts only count
    /// where `visible` says so for the document and the part, its main text
    /// always counting. A document is matched and ranked as if it held just the
    /// parts that count, so one that doesn't can neither find it nor keep it out.
    pub fn search_visible<P: KeySource<P>>(
        &self,
        tx: &impl Readable,
        query: &str,
        mut visible: impl FnMut(K::GetType<'_>, P::GetType<'_>) -> bool,
    ) -> Result<Vec<SpaceKey<K>>> {
        self.search_parts(tx, query, &mut |doc, part| visible(K::decode(doc), P::decode(part)))
    }

    fn search_parts(&self, tx: &impl Readable, query: &str, visible: &mut Visible) -> Result<Vec<SpaceKey<K>>> {
        let parsed = parse_query(query, self.min_token_length);
        let ids = self.index.search_scored(tx, &parsed.required, &parsed.optional, &parsed.negative, visible)?;
        Ok(ids.into_iter().map(SpaceKey::from).collect())
    }

    /// The lemmas worth highlighting in `query`'s results, parsed with this space's
    /// own tokenizer settings so the highlighted terms match what `search` queried.
    pub fn highlight_terms(&self, query: &str) -> BTreeSet<String> {
        parse_query(query, self.min_token_length).highlight_lemmas()
    }
}
