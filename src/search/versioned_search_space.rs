use std::collections::BTreeSet;
use std::marker::PhantomData;

use anyhow::{Result, anyhow};
use fjall::{OptimisticWriteTx, OwnedWriteBatch, Slice};
use log::info;
use migrateable::{MigrateDataKind, Migrateable, Type};

use crate::key_slice::ToKeySlice;
use crate::schema_map_space::{SpaceSchema, SpaceSchemaHandle};
use crate::store_key::SpaceKey;
use crate::managed_database::{SpaceGroup, SpaceOptions};
use crate::{IterExtensions, KeySource, Readable};

use super::index::{Index, decode_counts, encode_count, posting_key};
use super::query::parse_query;
use super::tokenizer::DEFAULT_MIN_TOKEN_LENGTH;

/// Placeholder schema value for the documents keyspace. The stored value is an
/// opaque, version-agnostic term set, so it never migrates; this exists only to
/// register the space with the schema map for type-hash and data-kind conflict
/// detection, mirroring the versioned store spaces.
#[derive(Migrateable)]
enum SearchValue {
    V1(Type<Slice>),
}

/// A versioned inverted index keyed by a migratable `K`, the search counterpart
/// to [`VersionedStoreSpace`](crate::VersionedStoreSpace).
///
/// Versioning applies only to the key: a 1-byte key-version prefix is folded
/// into the id body (`[version] ++ encoded_key`) that the version-agnostic
/// [`Index`] core treats as opaque. On open, keys stored under older versions
/// are migrated forward by scanning the documents keyspace (which holds each
/// doc's term set) and rewriting its postings under the current version.
pub struct VersionedSearchSpace<K: Migrateable>
where
    K::Current: KeySource<K::Current>,
{
    index: Index,
    min_token_length: usize,
    _marker: PhantomData<fn() -> K>,
}

impl<K: Migrateable> Clone for VersionedSearchSpace<K>
where
    K::Current: KeySource<K::Current>,
{
    fn clone(&self) -> Self {
        Self {
            index: self.index.clone(),
            min_token_length: self.min_token_length,
            _marker: PhantomData,
        }
    }
}

impl<K: Migrateable> VersionedSearchSpace<K>
where
    K::Current: KeySource<K::Current>,
{
    pub fn open_and_migrate(group: &mut SpaceGroup, options: SpaceOptions) -> Result<Self> {
        if K::CURRENT_DATA_KIND != MigrateDataKind::Key && K::CURRENT_DATA_KIND != MigrateDataKind::Bytes {
            return Err(anyhow!("Migrateable keys for `VersionedSearchSpace` must use the `Type` or `KeyType` data kind"));
        }

        let postings_name = group.full_name("postings");
        let documents_name = group.full_name("documents");
        let postings = group.open_keyspace(postings_name, options.clone())?;
        let documents = group.open_keyspace(documents_name.clone(), options)?;
        let database = group.database().clone();

        let result = Self {
            index: Index {
                postings,
                documents,
                database,
            },
            min_token_length: DEFAULT_MIN_TOKEN_LENGTH,
            _marker: PhantomData,
        };

        let schema_handle = group.schema_map().check_space_schema_and_insert_if_empty::<K, SearchValue>(&documents_name)?;
        result.migrate(&documents_name, &schema_handle)?;
        schema_handle.update_space_schema(
            &documents_name,
            &SpaceSchema::from_migrateables(K::iter().collect(), SearchValue::iter().collect()),
        )?;

        Ok(result)
    }

    pub fn min_token_length(mut self, length: usize) -> Self {
        self.min_token_length = length;
        self
    }

    /// Indexes `content` for `doc_id` under the current key version.
    pub fn index_document(&self, tx: &mut OptimisticWriteTx, doc_id: impl ToKeySlice<K::Current>, content: &str) -> Result<()> {
        let body = self.versioned_id(doc_id.to_slice().as_ref());
        self.index.index(tx, &body, content, self.min_token_length)
    }

    /// Removes every posting for `doc_id` and forgets its stored terms.
    pub fn remove_document(&self, tx: &mut OptimisticWriteTx, doc_id: impl ToKeySlice<K::Current>) -> Result<()> {
        let body = self.versioned_id(doc_id.to_slice().as_ref());
        self.index.remove(tx, &body)
    }

    /// Returns the ids of every document matching `query`, ranked best-first.
    ///
    /// `query` honors the full syntax: unquoted words are required (AND'd), a
    /// `"quoted phrase"` requires all its words, a `?` prefix makes a word or
    /// phrase optional (it only boosts the score), a leading `-` excludes, and
    /// documents matching more optional clauses rank ahead of the rest.
    pub fn search(&self, tx: &impl Readable, query: &str) -> Result<Vec<SpaceKey<K::Current>>> {
        let parsed = parse_query(query, self.min_token_length);
        let bodies = self.index.search_scored(tx, &parsed.required, &parsed.optional, &parsed.negative)?;
        Ok(bodies.into_iter().map(strip_version).collect())
    }

    /// The lemmas worth highlighting in `query`'s results, parsed with this space's
    /// own tokenizer settings so the highlighted terms match what `search` queried.
    pub fn highlight_terms(&self, query: &str) -> BTreeSet<String> {
        parse_query(query, self.min_token_length).highlight_lemmas()
    }

    fn versioned_id(&self, encoded: &[u8]) -> Vec<u8> {
        let mut id = Vec::with_capacity(1 + encoded.len());
        id.push(K::CURRENT_VERSION);
        id.extend_from_slice(encoded);
        id
    }

    fn migrate(&self, space_name: &str, schema: &SpaceSchemaHandle<'_, K, SearchValue>) -> Result<()> {
        const BATCH_SIZE: usize = 1024 * 1024 * 64;

        for (key_version, _) in schema.schema().key_schema.iter() {
            if key_version.to_u8() >= K::CURRENT_VERSION || key_version.to_u8() < schema.stored_key_version() {
                continue;
            }

            let mut logged = false;
            loop {
                let mut results_size = 0;
                let mut results = Vec::with_capacity(1024);
                for kv in self.index.database.read_tx().iter_prefixed(
                    &self.index.database,
                    &self.index.documents,
                    [key_version.to_u8()],
                ) {
                    if !logged {
                        info!(
                            "Migrating search keys in '{space_name}' from version {} to {}",
                            key_version.to_u8(),
                            K::CURRENT_VERSION
                        );
                        logged = true;
                    }

                    let (key, value) = kv?;
                    results_size += (1024 * 16) + (key.len() + value.len());
                    results.push((key, value));

                    if results_size >= BATCH_SIZE {
                        break;
                    }
                }

                // Migrated docs are rewritten under the current version prefix, so once a
                // scan of the old-version prefix comes back empty, this version is done.
                if results.is_empty() {
                    break;
                }

                let mut tx = OwnedWriteBatch::with_capacity(self.index.database.inner().clone(), results.len() * 4);

                for (key, value) in results.into_iter() {
                    let migrated: Vec<u8> = key_version.migrate(&key[1..])?;
                    let mut new_id = Vec::with_capacity(1 + migrated.len());
                    new_id.push(K::CURRENT_VERSION);
                    new_id.extend_from_slice(&migrated);

                    for (term, count) in decode_counts(value.as_ref()) {
                        tx.remove(self.index.postings.inner(), posting_key(&term, key.as_ref()));
                        tx.insert(self.index.postings.inner(), posting_key(&term, &new_id), encode_count(count));
                    }

                    tx.remove(self.index.documents.inner(), key);
                    tx.insert(self.index.documents.inner(), new_id, value);
                }

                tx.commit()?;
                crate::wait_for_write_buffer(self.index.database.inner());
            }
        }

        Ok(())
    }
}

fn strip_version<K: KeySource<K>>(body: Slice) -> SpaceKey<K> {
    SpaceKey::from(Slice::from(&body.as_ref()[1..]))
}
