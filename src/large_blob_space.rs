use std::collections::HashSet;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Result, anyhow};
use fjall::{OptimisticTxDatabase, OptimisticTxKeyspace, OptimisticWriteTx, OwnedWriteBatch, Slice};
use log::{error, info, warn};
use migrateable::{MigrateDataKind, Migrateable, Type};

use crate::key_slice::ToKeySlice;
use crate::schema_map_space::{SpaceSchema, ValidatedSchema};
use crate::SchemaMapSpace;
use crate::managed_database::{SpaceGroup, SpaceOptions};
use crate::{BlobOptions, IterExtensions, KeySource, Readable};

/// Placeholder schema value for the manifests keyspace. Manifests and chunks are
/// opaque internal formats that never migrate; this exists only to register the
/// space with the schema map for type-hash and data-kind conflict detection,
/// mirroring [`VersionedSearchSpace`](crate::VersionedSearchSpace).
#[derive(Migrateable)]
enum LargeBlobValue {
    V1(Type<Slice>),
}

const MANIFEST_FORMAT: u8 = 1;
const MANIFEST_LEN: usize = 29;

/// Locator for a blob's chunk run: chunks live under `blob_id ++ index` keys,
/// independent of the user key, so a blob can be staged before its key is known
/// (e.g. while a content hash is still being computed from the stream).
#[derive(Clone, Copy)]
struct BlobManifest {
    blob_id: [u8; 16],
    total_len: u64,
    chunk_size: u32,
}

impl BlobManifest {
    fn chunk_count(&self) -> u32 {
        if self.chunk_size == 0 {
            return 0;
        }
        #[expect(clippy::cast_possible_truncation, reason = "chunk count fits u32 for any u64 length with chunk_size >= 1KiB")]
        {
            self.total_len.div_ceil(u64::from(self.chunk_size)) as u32
        }
    }

    fn encode(&self) -> [u8; MANIFEST_LEN] {
        let mut buffer = [0u8; MANIFEST_LEN];
        buffer[0] = MANIFEST_FORMAT;
        buffer[1..17].copy_from_slice(&self.blob_id);
        buffer[17..25].copy_from_slice(&self.total_len.to_be_bytes());
        buffer[25..29].copy_from_slice(&self.chunk_size.to_be_bytes());
        buffer
    }

    fn decode(raw: &[u8]) -> Result<Self> {
        if raw.len() != MANIFEST_LEN || raw[0] != MANIFEST_FORMAT {
            return Err(anyhow!(
                "Invalid large blob manifest ({} bytes, format {})",
                raw.len(),
                raw.first().copied().unwrap_or(0)
            ));
        }
        Ok(Self {
            blob_id: raw[1..17].try_into().expect("slice length checked"),
            total_len: u64::from_be_bytes(raw[17..25].try_into().expect("slice length checked")),
            chunk_size: u32::from_be_bytes(raw[25..29].try_into().expect("slice length checked")),
        })
    }
}

fn chunk_key(blob_id: &[u8; 16], index: u32) -> [u8; 20] {
    let mut key = [0u8; 20];
    key[..16].copy_from_slice(blob_id);
    key[16..].copy_from_slice(&index.to_be_bytes());
    key
}

/// The keyspace handles a staged blob needs for cleanup, shared by the writer,
/// [`StagedBlob`] and the space itself.
#[derive(Clone)]
struct BlobStorage {
    chunks: OptimisticTxKeyspace,
    staging: OptimisticTxKeyspace,
    database: OptimisticTxDatabase,
    /// Blob ids with a live in-process writer or [`StagedBlob`]. A staging
    /// marker whose id is absent here belongs to no running upload, which is
    /// what makes [`LargeBlobSpace::sweep_stale`] safe to run at any time.
    live_uploads: Arc<Mutex<HashSet<[u8; 16]>>>,
}

impl BlobStorage {
    fn mark_live(&self, blob_id: [u8; 16]) {
        self.live_uploads.lock().expect("lock is poisoned").insert(blob_id);
    }

    fn mark_dead(&self, blob_id: &[u8; 16]) {
        self.live_uploads.lock().expect("lock is poisoned").remove(blob_id);
    }

    fn is_live(&self, blob_id: &[u8; 16]) -> bool {
        self.live_uploads.lock().expect("lock is poisoned").contains(blob_id)
    }

    /// Deletes every staged chunk of `blob_id` plus its staging marker. Scans by
    /// prefix rather than trusting a count so it also covers partial writes.
    fn discard_staged(&self, blob_id: &[u8; 16]) -> Result<usize> {
        let mut batch = OwnedWriteBatch::with_capacity(self.database.inner().clone(), 8);
        let mut removed = 0;

        {
            let snapshot = self.database.read_tx();
            for guard in fjall::Readable::prefix(&snapshot, &self.chunks, blob_id) {
                batch.remove(self.chunks.inner(), guard.key()?);
                removed += 1;
            }
        }

        batch.remove(self.staging.inner(), *blob_id);
        batch.commit()?;
        Ok(removed)
    }
}

/// Chunked storage for values too large to move through fjall in one piece,
/// keyed by a migratable `K` like [`VersionedStoreSpace`](crate::VersionedStoreSpace);
/// versioning applies only to the key, values are opaque bytes.
///
/// A blob is split into fixed-size chunks living in their own kv-separated
/// keyspace, so writes stream through a bounded buffer (one journal entry and
/// one memtable insert per chunk) and reads stream back chunk by chunk. Each
/// chunk read is an independent point read: no snapshot is held across chunks,
/// so a slow reader pins neither sealed memtables nor old tree versions, and the
/// block cache can evict already-served chunks. That is safe because a staged
/// blob's chunks are immutable: they are only ever deleted, never rewritten, and
/// a delete during a read surfaces as an error from the reader.
///
/// Writes are staged out-of-band under a random blob id, tracked by a staging
/// marker, and become visible atomically when [`commit`](Self::commit) runs in a
/// caller transaction. Dropping a [`LargeBlobWriter`] or an uncommitted
/// [`StagedBlob`] deletes its chunks immediately; markers that survive a crash
/// are swept on the next [`stage_open`](Self::stage_open).
pub struct LargeBlobSpace<K: Migrateable>
where
    K::Current: KeySource<K::Current>,
{
    manifests: OptimisticTxKeyspace,
    storage: BlobStorage,
    chunk_size: u32,
    _marker: PhantomData<fn() -> K>,
}

impl<K: Migrateable> Clone for LargeBlobSpace<K>
where
    K::Current: KeySource<K::Current>,
{
    fn clone(&self) -> Self {
        Self {
            manifests: self.manifests.clone(),
            storage: self.storage.clone(),
            chunk_size: self.chunk_size,
            _marker: PhantomData,
        }
    }
}

impl<K: Migrateable + 'static> LargeBlobSpace<K>
where
    K::Current: KeySource<K::Current>,
{
    /// Opens the three backing keyspaces (`{name}.manifests`, `{name}.chunks`,
    /// `{name}.staging`) and validates the manifests' stored schema against the
    /// current key type, returning the live space with its manifest migration and
    /// orphan sweep queued to run after every space has validated. Validation
    /// reads only, so a schema mismatch here leaves the store untouched.
    ///
    /// `options` applies to the chunks keyspace; kv separation is enabled there
    /// by default since chunks are the large payloads. Manifests and staging
    /// markers are small and use default options. `chunk_size` only affects new
    /// writes: each manifest records the size its blob was written with.
    pub fn stage_open(
        group: &mut SpaceGroup,
        name: impl AsRef<str>,
        options: SpaceOptions,
        chunk_size: u32,
    ) -> Result<Self> {
        let (space, manifests_name, validated, schema_map) = Self::open_and_validate(group, name, options, chunk_size)?;
        let migrator = space.clone();
        group.defer_migration(move || migrator.run_migration(&manifests_name, &validated, &schema_map));
        Ok(space)
    }

    fn open_and_validate(
        group: &mut SpaceGroup,
        name: impl AsRef<str>,
        mut options: SpaceOptions,
        chunk_size: u32,
    ) -> Result<(Self, String, ValidatedSchema<K, LargeBlobValue>, SchemaMapSpace)> {
        if K::CURRENT_DATA_KIND != MigrateDataKind::Key && K::CURRENT_DATA_KIND != MigrateDataKind::Bytes {
            return Err(anyhow!("Migrateable keys for `LargeBlobSpace` must use the `Type` or `KeyType` data kind"));
        }
        if chunk_size == 0 {
            return Err(anyhow!("`LargeBlobSpace` chunk size must be non-zero"));
        }

        if options.blob.is_none() {
            options = options.blob(BlobOptions::default());
        }

        let name = name.as_ref();
        let manifests_name = group.full_name(format!("{name}.manifests"));
        let manifests = group.open_keyspace(manifests_name.clone(), SpaceOptions::default())?;
        let chunks = group.open_keyspace(group.full_name(format!("{name}.chunks")), options)?;
        let staging = group.open_keyspace(group.full_name(format!("{name}.staging")), SpaceOptions::default())?;
        let database = group.database().clone();

        let space = Self {
            manifests,
            storage: BlobStorage {
                chunks,
                staging,
                database,
                live_uploads: Arc::new(Mutex::new(HashSet::new())),
            },
            chunk_size,
            _marker: PhantomData,
        };

        let validated = group.schema_map().check_space_schema_and_insert_if_empty::<K, LargeBlobValue>(&manifests_name)?;
        let schema_map = group.schema_map().clone();
        Ok((space, manifests_name, validated, schema_map))
    }

    fn run_migration(
        &self,
        manifests_name: &str,
        validated: &ValidatedSchema<K, LargeBlobValue>,
        schema_map: &SchemaMapSpace,
    ) -> Result<()> {
        self.migrate(manifests_name, validated)?;
        schema_map.write_space_schema_if_changed(
            manifests_name,
            &validated.schema,
            &SpaceSchema::from_migrateables(K::iter().collect(), LargeBlobValue::iter().collect()),
        )?;
        self.cleanup_orphans(manifests_name)
    }

    fn migrate(&self, space_name: &str, schema: &ValidatedSchema<K, LargeBlobValue>) -> Result<()> {
        let database = &self.storage.database;

        for (key_version, _) in schema.schema.key_schema.iter() {
            if key_version.to_u8() >= K::CURRENT_VERSION || key_version.to_u8() < schema.stored_key_version {
                continue;
            }

            let mut logged = false;
            loop {
                let mut results = Vec::with_capacity(1024);
                for kv in database.read_tx().iter_prefixed(database, &self.manifests, [key_version.to_u8()]) {
                    if !logged {
                        info!(
                            "Migrating large blob keys in '{space_name}' from version {} to {}",
                            key_version.to_u8(),
                            K::CURRENT_VERSION
                        );
                        logged = true;
                    }

                    results.push(kv?);

                    if results.len() >= 4096 {
                        break;
                    }
                }

                // Migrated manifests are rewritten under the current version prefix, so once
                // a scan of the old-version prefix comes back empty, this version is done.
                if results.is_empty() {
                    break;
                }

                let mut batch = OwnedWriteBatch::with_capacity(database.inner().clone(), results.len() * 2);

                for (key, value) in results {
                    let migrated: Vec<u8> = key_version.migrate(&key[1..])?;
                    let mut new_key = Vec::with_capacity(1 + migrated.len());
                    new_key.push(K::CURRENT_VERSION);
                    new_key.extend_from_slice(&migrated);

                    batch.remove(self.manifests.inner(), key);
                    batch.insert(self.manifests.inner(), new_key, value);
                }

                batch.commit()?;
                crate::wait_for_write_buffer(database.inner());
            }
        }

        Ok(())
    }

    /// Deletes chunk runs whose staging marker outlived its writer: a crash
    /// between staging and commit leaves the marker behind, and this runs before
    /// any new writer exists, so every marker found here is garbage.
    fn cleanup_orphans(&self, space_name: &str) -> Result<()> {
        let orphans: Vec<[u8; 16]> = {
            let snapshot = self.storage.database.read_tx();
            fjall::Readable::iter(&snapshot, &self.storage.staging)
                .map(|guard| {
                    let key = guard.key()?;
                    key.as_ref().try_into().map_err(|_| anyhow!("Invalid staging marker key of {} bytes", key.len()))
                })
                .collect::<Result<_>>()?
        };

        for blob_id in orphans {
            let removed = self.storage.discard_staged(&blob_id)?;
            warn!("Removed {removed} orphaned large blob chunks in '{space_name}' left by an interrupted upload");
        }

        Ok(())
    }

    fn manifest_key(&self, key: impl ToKeySlice<K::Current>) -> Vec<u8> {
        let encoded = key.to_slice();
        let mut buffer = Vec::with_capacity(1 + encoded.len());
        buffer.push(K::CURRENT_VERSION);
        buffer.extend_from_slice(&encoded);
        buffer
    }

    fn get_manifest(&self, tx: &impl Readable, key: impl ToKeySlice<K::Current>) -> Result<Option<BlobManifest>> {
        match tx.get(&self.manifests, self.manifest_key(key))? {
            Some(raw) => Ok(Some(BlobManifest::decode(&raw)?)),
            None => Ok(None),
        }
    }

    /// Starts staging a new blob. Chunks are written (and become durable with the
    /// journal) as they are fed in, but stay invisible until [`commit`](Self::commit).
    pub fn writer(&self) -> LargeBlobWriter {
        let blob_id = uuid::Uuid::now_v7().into_bytes();
        self.storage.mark_live(blob_id);
        LargeBlobWriter {
            storage: self.storage.clone(),
            blob_id,
            buffer: Vec::new(),
            chunk_size: self.chunk_size as usize,
            next_index: 0,
            total_len: 0,
            marker_written: false,
            armed: true,
        }
    }

    /// Convenience for payloads already in memory (small blobs): stages `bytes`
    /// through a writer in one call.
    pub fn stage(&self, bytes: &[u8]) -> Result<StagedBlob> {
        let mut writer = self.writer();
        writer.write(bytes)?;
        writer.finalize()
    }

    /// Publishes a staged blob under `key` inside the caller's transaction,
    /// together with whatever other writes make the blob reachable. Returns
    /// `false` without writing if `key` already holds a blob, so a
    /// content-addressed caller can just drop the duplicate `staged`.
    ///
    /// Safe to re-run on optimistic retry. If the transaction ultimately never
    /// commits, the staging marker (only ever removed here) survives, so the
    /// chunks are reclaimed by [`sweep_stale`](Self::sweep_stale) or the next open.
    pub fn commit(&self, tx: &mut OptimisticWriteTx, key: impl ToKeySlice<K::Current>, staged: &StagedBlob) -> Result<bool> {
        // A blob staged in one space (or one database instance, e.g. before a
        // backup restore swapped it out) must not produce a manifest in another:
        // the manifest would point at chunks this space does not hold.
        if !Arc::ptr_eq(&self.storage.live_uploads, &staged.storage.live_uploads) {
            return Err(anyhow!("Staged blob belongs to a different space or database instance"));
        }

        let manifest_key = self.manifest_key(key);

        // In an optimistic retry loop only the last run's writes reach disk, so the
        // committed flag always mirrors the latest outcome; an earlier attempt that
        // won the insert but conflicted must not leave the flag armed (or a retry
        // that loses to a concurrent duplicate would skip its cleanup on drop).
        if tx.get(&self.manifests, &manifest_key)?.is_some() {
            staged.committed.store(false, Ordering::Release);
            return Ok(false);
        }

        let manifest = BlobManifest {
            blob_id: staged.blob_id,
            total_len: staged.total_len,
            chunk_size: staged.chunk_size,
        };
        tx.insert(&self.manifests, manifest_key, manifest.encode());
        tx.remove(&self.storage.staging, staged.blob_id);
        staged.committed.store(true, Ordering::Release);
        Ok(true)
    }

    /// Deletes staged chunk runs that belong to no live in-process upload, for
    /// periodic use on long-running servers (a marker can outlive its upload
    /// when a commit's transaction fails after the flag was set; without this,
    /// such chunks would only be swept on the next open).
    ///
    /// Safe against in-flight uploads: an id is registered as live from
    /// [`writer`](Self::writer) until its [`StagedBlob`] drops, and `commit` can
    /// only run while the `StagedBlob` is alive, so a marker whose id is not
    /// live can never gain a manifest later. A marker whose commit already
    /// landed does not exist (it is removed in the same transaction as the
    /// manifest insert). Returns the number of stale uploads removed.
    pub fn sweep_stale(&self) -> Result<usize> {
        let markers: Vec<[u8; 16]> = {
            let snapshot = self.storage.database.read_tx();
            fjall::Readable::iter(&snapshot, &self.storage.staging)
                .map(|guard| {
                    let key = guard.key()?;
                    key.as_ref().try_into().map_err(|_| anyhow!("Invalid staging marker key of {} bytes", key.len()))
                })
                .collect::<Result<_>>()?
        };

        let mut swept = 0;
        for blob_id in markers {
            if self.storage.is_live(&blob_id) {
                continue;
            }
            self.storage.discard_staged(&blob_id)?;
            swept += 1;
        }
        Ok(swept)
    }

    /// Removes the blob under `key` inside the caller's transaction: the manifest
    /// plus a tombstone per chunk (a handful of bytes each, so even a multi-GB
    /// blob stays a small transaction).
    pub fn remove(&self, tx: &mut OptimisticWriteTx, key: impl ToKeySlice<K::Current>) -> Result<bool> {
        let manifest_key = self.manifest_key(key);
        let Some(raw) = tx.get(&self.manifests, &manifest_key)? else {
            return Ok(false);
        };
        let manifest = BlobManifest::decode(&raw)?;

        for index in 0..manifest.chunk_count() {
            tx.remove(&self.storage.chunks, chunk_key(&manifest.blob_id, index));
        }
        tx.remove(&self.manifests, manifest_key);
        Ok(true)
    }

    pub fn contains_key(&self, tx: &impl Readable, key: impl ToKeySlice<K::Current>) -> Result<bool> {
        Ok(tx.contains_key(&self.manifests, self.manifest_key(key))?)
    }

    /// Total byte length of the blob under `key`, from its manifest alone.
    pub fn size_of(&self, tx: &impl Readable, key: impl ToKeySlice<K::Current>) -> Result<Option<u64>> {
        Ok(self.get_manifest(tx, key)?.map(|m| m.total_len))
    }

    /// Opens a streaming reader over the blob under `key`. The manifest is read
    /// through `tx`, but the reader itself holds no transaction or snapshot; see
    /// the type-level docs for why that is sound.
    pub fn reader(&self, tx: &impl Readable, key: impl ToKeySlice<K::Current>) -> Result<Option<LargeBlobReader>> {
        Ok(self.get_manifest(tx, key)?.map(|manifest| LargeBlobReader {
            chunks: self.storage.chunks.clone(),
            manifest,
            next_index: 0,
        }))
    }

    /// Reads a whole blob into one buffer. Only for blobs known to be small;
    /// large blobs should stream through [`reader`](Self::reader).
    pub fn read_all(&self, tx: &impl Readable, key: impl ToKeySlice<K::Current>) -> Result<Option<Vec<u8>>> {
        let Some(reader) = self.reader(tx, key)? else {
            return Ok(None);
        };
        #[expect(clippy::cast_possible_truncation, reason = "caller asserts the blob fits in memory")]
        let mut bytes = Vec::with_capacity(reader.total_len() as usize);
        for chunk in reader {
            bytes.extend_from_slice(&chunk?);
        }
        Ok(Some(bytes))
    }

    /// Number of committed blobs (manifest count estimate).
    pub fn approximate_len(&self) -> usize {
        self.manifests.inner().approximate_len()
    }

    pub fn disk_space(&self) -> u64 {
        self.manifests.inner().disk_space() + self.storage.chunks.inner().disk_space() + self.storage.staging.inner().disk_space()
    }
}

/// Streams a blob into staged chunks. Feed bytes with [`write`](Self::write) in
/// any increments; every full chunk is committed in its own small write batch,
/// so memory stays bounded by one chunk regardless of blob size. Call
/// [`finalize`](Self::finalize) to flush the tail and obtain the [`StagedBlob`]
/// to commit; dropping the writer instead deletes everything staged so far
/// (a cancelled upload cleans up immediately).
pub struct LargeBlobWriter {
    storage: BlobStorage,
    blob_id: [u8; 16],
    buffer: Vec<u8>,
    chunk_size: usize,
    next_index: u32,
    total_len: u64,
    marker_written: bool,
    armed: bool,
}

impl LargeBlobWriter {
    pub fn write(&mut self, mut bytes: &[u8]) -> Result<()> {
        self.total_len += bytes.len() as u64;

        while !bytes.is_empty() {
            if self.buffer.is_empty() && bytes.len() >= self.chunk_size {
                let (chunk, rest) = bytes.split_at(self.chunk_size);
                self.commit_chunk(chunk)?;
                bytes = rest;
                continue;
            }

            let take = (self.chunk_size - self.buffer.len()).min(bytes.len());
            let (part, rest) = bytes.split_at(take);
            self.buffer.extend_from_slice(part);
            bytes = rest;

            if self.buffer.len() == self.chunk_size {
                let chunk = std::mem::take(&mut self.buffer);
                self.commit_chunk(&chunk)?;
                self.buffer = chunk;
                self.buffer.clear();
            }
        }

        Ok(())
    }

    fn commit_chunk(&mut self, chunk: &[u8]) -> Result<()> {
        let mut batch = OwnedWriteBatch::with_capacity(self.storage.database.inner().clone(), 2);
        // The marker rides in the same atomic batch as the first chunk, so no chunk
        // can ever hit disk without a marker pointing the crash sweep at it.
        if !self.marker_written {
            batch.insert(self.storage.staging.inner(), self.blob_id, []);
        }
        batch.insert(self.storage.chunks.inner(), chunk_key(&self.blob_id, self.next_index), chunk);
        batch.commit()?;

        self.marker_written = true;
        self.next_index += 1;
        Ok(())
    }

    pub fn total_len(&self) -> u64 {
        self.total_len
    }

    /// Flushes the final partial chunk and hands the staged blob over for
    /// [`LargeBlobSpace::commit`]. Cleanup responsibility moves to the returned
    /// [`StagedBlob`].
    pub fn finalize(mut self) -> Result<StagedBlob> {
        if !self.buffer.is_empty() {
            let chunk = std::mem::take(&mut self.buffer);
            self.commit_chunk(&chunk)?;
        }

        self.armed = false;
        #[expect(clippy::cast_possible_truncation, reason = "chunk size is created from a u32")]
        Ok(StagedBlob {
            blob_id: self.blob_id,
            total_len: self.total_len,
            chunk_size: self.chunk_size as u32,
            chunk_count: self.next_index,
            committed: AtomicBool::new(false),
            storage: self.storage.clone(),
        })
    }
}

impl Drop for LargeBlobWriter {
    fn drop(&mut self) {
        if !self.armed {
            // Finalized: the StagedBlob now owns cleanup and the live registration.
            return;
        }
        if self.marker_written
            && let Err(e) = self.storage.discard_staged(&self.blob_id)
        {
            // The staging marker is still on disk, so a later sweep retries these chunks.
            error!("Failed to discard cancelled large blob upload: {e:?}");
        }
        self.storage.mark_dead(&self.blob_id);
    }
}

/// A fully staged blob awaiting [`LargeBlobSpace::commit`]. Dropping it without
/// a recorded commit deletes the staged chunks, so a duplicate (commit returned
/// `false`) or an abandoned upload cleans up by simply going out of scope.
#[must_use]
pub struct StagedBlob {
    blob_id: [u8; 16],
    total_len: u64,
    chunk_size: u32,
    chunk_count: u32,
    // Tracks the latest `commit` attempt's outcome. If a committing transaction
    // fails after the flag is set, the chunks survive until the staging marker
    // is reclaimed by `sweep_stale` or the next open; erring on that side means
    // a commit that DID land can never have its chunks deleted from under it.
    committed: AtomicBool,
    storage: BlobStorage,
}

impl StagedBlob {
    pub fn total_len(&self) -> u64 {
        self.total_len
    }

    pub fn chunk_count(&self) -> u32 {
        self.chunk_count
    }
}

impl Drop for StagedBlob {
    fn drop(&mut self) {
        if !self.committed.load(Ordering::Acquire)
            && self.chunk_count > 0
            && let Err(e) = self.storage.discard_staged(&self.blob_id)
        {
            // The staging marker is still on disk, so a later sweep retries these chunks.
            error!("Failed to discard uncommitted large blob: {e:?}");
        }
        self.storage.mark_dead(&self.blob_id);
    }
}

/// Streams a committed blob back out, one chunk per call. Holds no snapshot:
/// every chunk is an independent point read, so long-running reads (a slow
/// client downloading gigabytes) pin no memtables, no tree versions, and let the
/// cache evict chunks as soon as they have been served.
///
/// If the blob is removed while a read is in flight, the next chunk read fails
/// with an error rather than returning torn data.
pub struct LargeBlobReader {
    chunks: OptimisticTxKeyspace,
    manifest: BlobManifest,
    next_index: u32,
}

impl LargeBlobReader {
    pub fn total_len(&self) -> u64 {
        self.manifest.total_len
    }

    pub fn chunk_count(&self) -> u32 {
        self.manifest.chunk_count()
    }

    pub fn next_chunk(&mut self) -> Result<Option<Slice>> {
        if self.next_index >= self.manifest.chunk_count() {
            return Ok(None);
        }

        let index = self.next_index;
        let Some(chunk) = self.chunks.get(chunk_key(&self.manifest.blob_id, index))? else {
            return Err(anyhow!("Large blob chunk {index} disappeared mid-read (blob removed concurrently?)"));
        };

        // Every chunk is exactly chunk_size except the final one; anything else
        // means the manifest and chunk run disagree, so fail before serving it.
        let read_so_far = u64::from(index) * u64::from(self.manifest.chunk_size);
        let expected = (self.manifest.total_len - read_so_far).min(u64::from(self.manifest.chunk_size));
        if chunk.len() as u64 != expected {
            return Err(anyhow!("Large blob chunk {index} is {} bytes, expected {expected}", chunk.len()));
        }

        self.next_index += 1;
        Ok(Some(chunk))
    }
}

impl Iterator for LargeBlobReader {
    type Item = Result<Slice>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_chunk().transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NoTx, ManagedDatabaseConfig, open_managed_database};

    #[derive(Migrateable)]
    enum TestBlobKey {
        V1(Type<[u8; 16]>),
    }

    const CHUNK: u32 = 8;

    fn open_space(path: &std::path::Path) -> Result<crate::ManagedDatabase<LargeBlobSpace<TestBlobKey>>> {
        open_managed_database(ManagedDatabaseConfig::new(path), |init| {
            let mut group = init.group("test");
            LargeBlobSpace::stage_open(&mut group, "blobs", SpaceOptions::default(), CHUNK)
        })
    }

    fn payload(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    fn commit_staged(db: &crate::ManagedDatabase<LargeBlobSpace<TestBlobKey>>, key: [u8; 16], staged: &StagedBlob) -> Result<bool> {
        let mut tx = db.write_tx()?;
        let inserted = db.state().commit(&mut tx, key, staged)?;
        tx.commit()??;
        Ok(inserted)
    }

    fn chunk_entry_count(db: &crate::ManagedDatabase<LargeBlobSpace<TestBlobKey>>) -> usize {
        fjall::Readable::iter(&db.read_tx(), &db.state().storage.chunks).count()
    }

    fn staging_entry_count(db: &crate::ManagedDatabase<LargeBlobSpace<TestBlobKey>>) -> usize {
        fjall::Readable::iter(&db.read_tx(), &db.state().storage.staging).count()
    }

    #[test]
    fn large_blob_round_trip() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let db = open_space(dir.path())?;
        let space = db.state();

        // 3 full chunks + a 4-byte tail
        let data = payload(28);
        let staged = space.stage(&data)?;
        assert_eq!(4, staged.chunk_count());
        assert!(commit_staged(&db, [1; 16], &staged)?);
        drop(staged);

        assert!(space.contains_key(&NoTx, [1; 16])?);
        assert_eq!(Some(28), space.size_of(&NoTx, [1; 16])?);

        let reader = space.reader(&NoTx, [1; 16])?.expect("blob should exist");
        assert_eq!(28, reader.total_len());
        let mut streamed = Vec::new();
        for chunk in reader {
            let chunk = chunk?;
            assert!(chunk.len() <= CHUNK as usize);
            streamed.extend_from_slice(&chunk);
        }
        assert_eq!(data, streamed);

        assert_eq!(Some(data), space.read_all(&NoTx, [1; 16])?);
        assert_eq!(0, staging_entry_count(&db));
        Ok(())
    }

    #[test]
    fn large_blob_empty() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let db = open_space(dir.path())?;
        let space = db.state();

        let staged = space.stage(&[])?;
        assert_eq!(0, staged.chunk_count());
        assert!(commit_staged(&db, [2; 16], &staged)?);
        drop(staged);

        assert_eq!(Some(0), space.size_of(&NoTx, [2; 16])?);
        assert_eq!(Some(Vec::new()), space.read_all(&NoTx, [2; 16])?);
        Ok(())
    }

    #[test]
    fn large_blob_duplicate_commit_discards_staged() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let db = open_space(dir.path())?;
        let space = db.state();

        let staged = space.stage(&payload(20))?;
        assert!(commit_staged(&db, [3; 16], &staged)?);
        drop(staged);
        assert_eq!(3, chunk_entry_count(&db));

        // Same key again: commit refuses, dropping the duplicate removes its chunks.
        let duplicate = space.stage(&payload(20))?;
        assert!(!commit_staged(&db, [3; 16], &duplicate)?);
        drop(duplicate);

        assert_eq!(3, chunk_entry_count(&db));
        assert_eq!(0, staging_entry_count(&db));
        assert_eq!(Some(payload(20)), space.read_all(&NoTx, [3; 16])?);
        Ok(())
    }

    #[test]
    fn large_blob_writer_drop_discards() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let db = open_space(dir.path())?;
        let space = db.state();

        let mut writer = space.writer();
        writer.write(&payload(20))?;
        drop(writer);

        assert_eq!(0, chunk_entry_count(&db));
        assert_eq!(0, staging_entry_count(&db));
        Ok(())
    }

    #[test]
    fn large_blob_remove_deletes_chunks() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let db = open_space(dir.path())?;
        let space = db.state();

        let staged = space.stage(&payload(24))?;
        assert!(commit_staged(&db, [4; 16], &staged)?);
        drop(staged);
        assert_eq!(3, chunk_entry_count(&db));

        let mut tx = db.write_tx()?;
        assert!(space.remove(&mut tx, [4; 16])?);
        tx.commit()??;

        assert!(!space.contains_key(&NoTx, [4; 16])?);
        assert_eq!(0, chunk_entry_count(&db));
        Ok(())
    }

    #[test]
    fn large_blob_sweep_skips_live_reclaims_dead() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let db = open_space(dir.path())?;
        let space = db.state();

        // A commit whose transaction never lands: flag armed, marker left behind.
        let staged = space.stage(&payload(20))?;
        let mut tx = db.write_tx()?;
        assert!(space.commit(&mut tx, [6; 16], &staged)?);
        drop(tx);
        drop(staged);
        assert_eq!(1, staging_entry_count(&db));
        assert_eq!(3, chunk_entry_count(&db));

        // An upload still in flight must not be touched.
        let mut writer = space.writer();
        writer.write(&payload(9))?;

        assert_eq!(1, space.sweep_stale()?);
        assert_eq!(1, staging_entry_count(&db));
        assert_eq!(1, chunk_entry_count(&db));

        drop(writer);
        assert_eq!(0, staging_entry_count(&db));
        assert_eq!(0, chunk_entry_count(&db));
        Ok(())
    }

    #[test]
    fn large_blob_commit_rejects_foreign_staged() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let db = open_managed_database(ManagedDatabaseConfig::new(dir.path()), |init| {
            let mut group = init.group("test");
            Ok((
                LargeBlobSpace::<TestBlobKey>::stage_open(&mut group, "a", SpaceOptions::default(), CHUNK)?,
                LargeBlobSpace::<TestBlobKey>::stage_open(&mut group, "b", SpaceOptions::default(), CHUNK)?,
            ))
        })?;
        let (space_a, space_b) = db.state();

        let staged = space_a.stage(&payload(20))?;
        let mut tx = db.write_tx()?;
        assert!(space_b.commit(&mut tx, [7; 16], &staged).is_err());
        drop(tx);
        Ok(())
    }

    #[test]
    fn large_blob_orphans_swept_on_reopen() -> Result<()> {
        let dir = tempfile::tempdir()?;

        {
            let db = open_space(dir.path())?;
            let space = db.state();

            let staged = space.stage(&payload(30))?;
            assert!(commit_staged(&db, [5; 16], &staged)?);
            drop(staged);

            // Simulate a crash mid-upload: chunks and marker on disk, no cleanup runs.
            // (Written raw rather than through a leaked writer, which would keep the
            // database handle alive and hold the file lock across the reopen.)
            let blob_id = [9u8; 16];
            let mut batch = OwnedWriteBatch::with_capacity(db.inner().clone(), 4);
            batch.insert(space.storage.staging.inner(), blob_id, [0u8; 0]);
            batch.insert(space.storage.chunks.inner(), chunk_key(&blob_id, 0), payload(8));
            batch.insert(space.storage.chunks.inner(), chunk_key(&blob_id, 1), payload(8));
            batch.commit()?;

            assert_eq!(1, staging_entry_count(&db));
            db.persist(fjall::PersistMode::SyncAll)?;
        }

        let db = open_space(dir.path())?;
        assert_eq!(0, staging_entry_count(&db));
        assert_eq!(4, chunk_entry_count(&db));
        assert_eq!(Some(payload(30)), db.state().read_all(&NoTx, [5; 16])?);
        Ok(())
    }
}
