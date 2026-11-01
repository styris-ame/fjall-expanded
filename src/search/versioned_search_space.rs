use std::collections::BTreeSet;
use std::marker::PhantomData;

use anyhow::{Context, Result, anyhow};
use fjall::{OptimisticWriteTx, OwnedWriteBatch, Slice};
use log::info;
use migrateable::{MigrateDataKind, MigrateResult, Migrateable, Type};

use crate::SchemaMapSpace;
use crate::key_slice::ToKeySlice;
use crate::managed_database::{SpaceGroup, SpaceOptions};
use crate::schema_map_space::{SpaceSchema, ValidatedSchema};
use crate::store_key::SpaceKey;
use crate::{KeySource, Readable};

use super::index::{HEAD_LEN, Index, Visible, decode_counts, head_of, named_parts, posting_key, split_body};
use super::query::parse_query;
use super::tokenizer::DEFAULT_MIN_TOKEN_LENGTH;

/// Placeholder schema value for the documents keyspace. The stored value is an
/// opaque, version-agnostic term set, so it never migrates; this exists only to
/// register the space with the schema map for type-hash and data-kind conflict
/// detection, mirroring the versioned store spaces. Its version also marks how
/// documents are laid out: from `V2` on, each is stored as its parts (see
/// [`Index`]), and a store last opened before that is moved over on open.
#[derive(Migrateable)]
enum SearchValue {
    V1(Type<Slice>),
    V2(Type<Slice>),
}

impl MigrateToCurrentSearchValue for Slice {
    fn migrate(self) -> MigrateResult<Slice> {
        Ok(self)
    }
}

/// The first [`SearchValue`] version whose documents are stored as parts
const PARTED_VERSION: u8 = 2;

/// A versioned inverted index keyed by a migratable `K`, the search counterpart
/// to [`VersionedStoreSpace`](crate::VersionedStoreSpace).
///
/// Versioning applies only to the key: a 1-byte key-version prefix is folded
/// into the document id (`[version] ++ encoded_key`) that the version-agnostic
/// [`Index`] core treats as opaque. On open, keys stored under older versions
/// are migrated forward by scanning the documents keyspace (which holds each
/// part's term set) and rewriting its postings under the current version.
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

impl<K: Migrateable + 'static> VersionedSearchSpace<K>
where
    K::Current: KeySource<K::Current>,
{
    /// Opens the postings and documents keyspaces and validates the stored
    /// schema against the current key type, returning the live space with its
    /// data migration queued to run after every space has validated. Validation
    /// reads only, so a schema mismatch here leaves the store untouched.
    pub fn stage_open(group: &mut SpaceGroup, options: SpaceOptions) -> Result<Self> {
        let (space, documents_name, validated, schema_map) = Self::open_and_validate(group, options)?;
        let migrator = space.clone();
        group.defer_migration(move || migrator.run_migration(&documents_name, &validated, &schema_map));
        Ok(space)
    }

    fn open_and_validate(
        group: &mut SpaceGroup,
        options: SpaceOptions,
    ) -> Result<(Self, String, ValidatedSchema<K, SearchValue>, SchemaMapSpace)> {
        if K::CURRENT_DATA_KIND != MigrateDataKind::Key && K::CURRENT_DATA_KIND != MigrateDataKind::Bytes {
            return Err(anyhow!("Migrateable keys for `VersionedSearchSpace` must use the `Type` or `KeyType` data kind"));
        }

        let postings_name = group.full_name("postings");
        let documents_name = group.full_name("documents");
        let postings = group.open_keyspace(postings_name, options.clone())?;
        let documents = group.open_keyspace(documents_name.clone(), options)?;
        let database = group.database().clone();

        let space = Self {
            index: Index {
                postings,
                documents,
                database,
            },
            min_token_length: DEFAULT_MIN_TOKEN_LENGTH,
            _marker: PhantomData,
        };

        let validated = group.schema_map().check_space_schema_and_insert_if_empty::<K, SearchValue>(&documents_name)?;
        let schema_map = group.schema_map().clone();
        Ok((space, documents_name, validated, schema_map))
    }

    fn run_migration(
        &self,
        documents_name: &str,
        validated: &ValidatedSchema<K, SearchValue>,
        schema_map: &SchemaMapSpace,
    ) -> Result<()> {
        self.migrate(documents_name, validated)?;
        schema_map.write_space_schema_if_changed(
            documents_name,
            &validated.schema,
            &SpaceSchema::from_migrateables(K::iter().collect(), SearchValue::iter().collect()),
        )
    }

    pub fn min_token_length(mut self, length: usize) -> Self {
        self.min_token_length = length;
        self
    }

    /// Indexes `content` as all of `doc_id`'s document under the current key
    /// version, replacing any parts it held.
    pub fn index_document(&self, tx: &mut OptimisticWriteTx, doc_id: impl ToKeySlice<K::Current>, content: &str) -> Result<()> {
        self.index.index(tx, &self.versioned_id(doc_id.to_slice().as_ref()), &[(&[], content)], self.min_token_length)
    }

    /// Indexes `doc_id`'s document as its main `content`, which every search
    /// counts, and named `parts`, which [`search_visible`](Self::search_visible)
    /// only counts where it's told to. Whatever the document held before is
    /// replaced. A part's name can't encode to nothing, and each comes up once.
    pub fn index_parts<P, T, S>(
        &self,
        tx: &mut OptimisticWriteTx,
        doc_id: impl ToKeySlice<K::Current>,
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
        self.index.index(tx, &self.versioned_id(doc_id.to_slice().as_ref()), &parts, self.min_token_length)
    }

    /// Removes every part of `doc_id`'s document and forgets their stored terms.
    pub fn remove_document(&self, tx: &mut OptimisticWriteTx, doc_id: impl ToKeySlice<K::Current>) -> Result<()> {
        self.index.remove(tx, &self.versioned_id(doc_id.to_slice().as_ref()))
    }

    /// Returns the ids of every document matching `query`, ranked best-first,
    /// with all of each document's parts counted.
    ///
    /// `query` honors the full syntax: unquoted words are required (AND'd), a
    /// `"quoted phrase"` requires all its words, a `?` prefix makes a word or
    /// phrase optional (it only boosts the score), a leading `-` excludes, and
    /// documents matching more optional clauses rank ahead of the rest.
    pub fn search(&self, tx: &impl Readable, query: &str) -> Result<Vec<SpaceKey<K::Current>>> {
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
        mut visible: impl FnMut(<K::Current as KeySource<K::Current>>::GetType<'_>, P::GetType<'_>) -> bool,
    ) -> Result<Vec<SpaceKey<K::Current>>> {
        self.search_parts(tx, query, &mut |doc, part| {
            visible(<K::Current as KeySource<K::Current>>::decode(&doc[1..]), P::decode(part))
        })
    }

    fn search_parts(&self, tx: &impl Readable, query: &str, visible: &mut Visible) -> Result<Vec<SpaceKey<K::Current>>> {
        let parsed = parse_query(query, self.min_token_length);
        let ids = self.index.search_scored(tx, &parsed.required, &parsed.optional, &parsed.negative, visible)?;
        Ok(ids.into_iter().map(strip_version).collect())
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

    /// Brings every stored part up to date in one pass over a snapshot: its
    /// document's id is migrated when stored under an older key version, and a
    /// document stored before documents had parts becomes its own main text.
    /// Postings move over as they are, positions and all.
    fn migrate(&self, space_name: &str, schema: &ValidatedSchema<K, SearchValue>) -> Result<()> {
        const BATCH_SIZE: usize = 1024 * 1024 * 64;

        let unparted = schema.stored_value_version < PARTED_VERSION;
        if !unparted && schema.stored_key_version >= K::CURRENT_VERSION {
            return Ok(());
        }

        let database = &self.index.database;
        let (postings, documents) = (self.index.postings.inner(), self.index.documents.inner());
        let snapshot = database.read_tx();
        let mut batch = OwnedWriteBatch::with_capacity(database.inner().clone(), 1024);
        let mut batch_size = 0;
        let mut logged = false;
        for entry in snapshot.iter(database, &self.index.documents) {
            let (body, counts) = entry.into_inner()?;
            // An unparted body starts with its key's version, never 0, where a parted one starts with its
            // id's length, whose first byte is 0 for any id under 256 bytes. So a run cut short leaves the
            // bodies it moved alone when it runs again.
            let parted = !unparted || body.first() == Some(&0);
            let (doc, part) = if parted {
                let (head, part) = split_body(&body);
                (&head[HEAD_LEN..], part)
            } else {
                (body.as_ref(), &[][..])
            };
            let Some(&version) = doc.first() else {
                continue;
            };
            let doc = if version != K::CURRENT_VERSION {
                let stored = K::from_u8(version)
                    .with_context(|| format!("'{space_name}' holds a document under unknown key version {version}"))?;
                let migrated: Vec<u8> = stored.migrate(&doc[1..])?;
                self.versioned_id(&migrated)
            } else if parted {
                continue;
            } else {
                doc.to_vec()
            };
            if !logged {
                info!("Migrating search documents in '{space_name}'");
                logged = true;
            }

            let new_body = [head_of(&doc)?.as_slice(), part].concat();
            for term in decode_counts(counts.as_ref()).keys() {
                let old_key = posting_key(term, &body);
                if let Some(posting) = snapshot.get(&self.index.postings, &old_key)? {
                    batch.remove(postings, old_key);
                    batch.insert(postings, posting_key(term, &new_body), posting);
                }
            }
            batch_size += (1024 * 16) + body.len() + counts.len();
            batch.remove(documents, body);
            batch.insert(documents, new_body, counts);

            if batch_size >= BATCH_SIZE {
                std::mem::replace(&mut batch, OwnedWriteBatch::with_capacity(database.inner().clone(), 1024)).commit()?;
                crate::wait_for_write_buffer(database.inner());
                batch_size = 0;
            }
        }
        batch.commit()?;
        crate::wait_for_write_buffer(database.inner());

        Ok(())
    }
}

fn strip_version<K: KeySource<K>>(id: Slice) -> SpaceKey<K> {
    SpaceKey::from(Slice::from(&id.as_ref()[1..]))
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use fjall::PersistMode;
    use migrateable::{MigrateResult, Migrateable, Type};

    use super::super::index::SEPARATOR;
    use super::*;
    use crate::{ManagedDatabase, ManagedDatabaseConfig, open_managed_database};

    #[derive(Migrateable)]
    enum FirstKey {
        V1(Type<[u8; 16]>),
    }

    #[derive(Migrateable)]
    enum SecondKey {
        V1(Type<[u8; 16]>),
        V2(Type<[u8; 16]>),
    }

    /// The second version keeps an id back to front, so a migrated one shows
    impl MigrateToCurrentSecondKey for [u8; 16] {
        fn migrate(mut self) -> MigrateResult<[u8; 16]> {
            self.reverse();
            Ok(self)
        }
    }

    type Opened<K> = (VersionedSearchSpace<K>, SchemaMapSpace, String);

    fn open<K: Migrateable + 'static>(dir: &Path) -> Result<ManagedDatabase<Opened<K>>>
    where
        K::Current: KeySource<K::Current>,
    {
        open_managed_database(ManagedDatabaseConfig::new(dir).temporary(false), |init| {
            let mut group = init.group("search");
            let space = VersionedSearchSpace::<K>::stage_open(&mut group, SpaceOptions::default())?;
            Ok((space, group.schema_map().clone(), group.full_name("documents")))
        })
    }

    fn id(n: u8) -> [u8; 16] {
        let mut id = [0; 16];
        id[0] = n;
        id
    }

    fn found(db: &ManagedDatabase<Opened<FirstKey>>, query: &str, shown: &[&str]) -> Result<Vec<[u8; 16]>> {
        let found = db.state().0.search_visible::<String>(&db.read_tx(), query, |_, part| shown.contains(&part.as_str()))?;
        Ok(found.into_iter().map(|key| key.get()).collect())
    }

    /// Puts a document back the way a store kept it before documents had parts, its main text alone
    fn unpart<K: Migrateable + 'static>(space: &VersionedSearchSpace<K>, tx: &mut OptimisticWriteTx, doc: [u8; 16]) -> Result<()>
    where
        K::Current: KeySource<K::Current>,
    {
        let old = space.versioned_id(&doc);
        let new = head_of(&old)?;
        let counts = tx.get(&space.index.documents, &new)?.context("the document is stored")?;
        for term in decode_counts(&counts).keys() {
            let posting = tx.get(&space.index.postings, posting_key(term, &new))?.context("its posting is stored")?;
            tx.remove(&space.index.postings, posting_key(term, &new));
            tx.insert(&space.index.postings, posting_key(term, &old), posting);
        }
        tx.remove(&space.index.documents, new);
        tx.insert(&space.index.documents, old, counts);
        Ok(())
    }

    /// Every body in both keyspaces, each split into its document and part
    fn bodies<K: Migrateable + 'static>(db: &ManagedDatabase<Opened<K>>) -> Result<Vec<(Vec<u8>, Vec<u8>)>>
    where
        K::Current: KeySource<K::Current>,
    {
        let space = &db.state().0;
        let read = db.read_tx();
        let mut bodies = Vec::new();
        for guard in read.iter(db, &space.index.documents) {
            let key = guard.key()?;
            let (head, part) = split_body(&key);
            bodies.push((head.to_vec(), part.to_vec()));
        }
        for guard in read.iter(db, &space.index.postings) {
            let key = guard.key()?;
            let at = key.iter().position(|byte| *byte == SEPARATOR).context("a posting has a separator")?;
            let (head, part) = split_body(&key[at + 1..]);
            bodies.push((head.to_vec(), part.to_vec()));
        }
        Ok(bodies)
    }

    #[test]
    fn a_store_from_before_parts_moves_over_even_when_cut_short() -> Result<()> {
        let dir = tempfile::tempdir()?;
        {
            let db = open::<FirstKey>(dir.path())?;
            let (space, schema_map, name) = db.state();
            let mut tx = db.write_tx()?;
            space.index_document(&mut tx, id(1), "apple banana")?;
            space.index_document(&mut tx, id(2), "apple cherry")?;
            space.index_document(&mut tx, id(3), "apple date")?;
            // The third stays moved over, as a run cut short would leave it
            for doc in [1, 2] {
                unpart(space, &mut tx, id(doc))?;
            }
            tx.commit()??;
            let current = SpaceSchema::<FirstKey, SearchValue>::from_migrateables(
                FirstKey::iter().collect(),
                SearchValue::iter().collect(),
            );
            let before = SpaceSchema::from_migrateables(FirstKey::iter().collect(), vec![SearchValue::V1(PhantomData)]);
            schema_map.write_space_schema_if_changed(name, &current, &before)?;
            db.persist(PersistMode::SyncAll)?;
        }

        let db = open::<FirstKey>(dir.path())?;
        // A body for each document and one for each of its two words' postings, all of them main text
        let mut wanted = Vec::new();
        for (doc, words) in [(1, 2), (2, 2), (3, 2)] {
            let head = head_of(&db.state().0.versioned_id(&id(doc)))?;
            wanted.extend(std::iter::repeat_n((head, Vec::new()), words + 1));
        }
        let mut stored = bodies(&db)?;
        stored.sort();
        wanted.sort();
        assert_eq!(stored, wanted);
        assert_eq!(found(&db, "apple", &[])?, vec![id(1), id(2), id(3)]);
        assert_eq!(found(&db, "banana", &[])?, vec![id(1)]);
        assert_eq!(found(&db, "apple -cherry", &[])?, vec![id(1), id(3)]);

        // The moved documents take parts and go like any other
        let mut tx = db.write_tx()?;
        db.state().0.index_parts::<String, _, _>(&mut tx, id(1), "apple", [("secret", "banana")])?;
        db.state().0.remove_document(&mut tx, id(2))?;
        tx.commit()??;
        assert!(found(&db, "banana", &[])?.is_empty());
        assert_eq!(found(&db, "apple banana", &["secret"])?, vec![id(1)]);
        assert!(found(&db, "cherry", &["secret"])?.is_empty());
        assert!(bodies(&db)?.iter().all(|(head, _)| head[HEAD_LEN..] != db.state().0.versioned_id(&id(2))[..]));
        Ok(())
    }

    #[test]
    fn a_key_migration_keeps_every_part() -> Result<()> {
        let dir = tempfile::tempdir()?;
        {
            let db = open::<FirstKey>(dir.path())?;
            let mut tx = db.write_tx()?;
            db.state()
                .0
                .index_parts::<String, _, _>(&mut tx, id(1), "apple", [("secret", "banana"), ("other", "cherry")])?;
            db.state().0.index_document(&mut tx, id(2), "apple")?;
            tx.commit()??;
            db.persist(PersistMode::SyncAll)?;
        }

        let db = open::<SecondKey>(dir.path())?;
        let moved = |n: u8| {
            let mut id = id(n);
            id.reverse();
            id
        };
        let space = &db.state().0;
        let read = db.read_tx();
        let found = |query: &str, shown: &[&str]| -> Result<Vec<[u8; 16]>> {
            let found = space.search_visible::<String>(&read, query, |_, part| shown.contains(&part.as_str()))?;
            Ok(found.into_iter().map(|key| key.get()).collect())
        };
        assert_eq!(found("apple", &[])?, vec![moved(1), moved(2)]);
        assert_eq!(found("apple banana cherry", &["secret", "other"])?, vec![moved(1)]);
        assert!(found("banana", &["other"])?.is_empty());
        assert!(bodies(&db)?.iter().all(|(head, _)| head[HEAD_LEN] == 2), "every body is under the new key version");
        Ok(())
    }
}
