use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Result, anyhow};
use arc_swap::ArcSwap;
use fjall::config::{BlockSizePolicy, BloomConstructionPolicy, CompressionPolicy, FilterPolicy, FilterPolicyEntry, PartitioningPolicy};
use fjall::{CompressionType, KvSeparationOptions};
use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, OptimisticTxKeyspace, OwnedWriteBatch, PersistMode};

use easy_rkyv::Rkyv;

use crate::schema_map_space::SCHEMA_MAP_KEYSPACE;
use crate::{Readable, SchemaMapSpace};

const REGISTRY_KEYSPACE: &str = "__fjall_expanded_registry";

const BACKUP_BATCH_BYTES: usize = 1024 * 1024 * 16;

pub const BACKUP_SUFFIX: &str = ".tar.zst";

const ZSTD_LEVEL: i32 = 6;
const ZSTD_WINDOW_LOG: u32 = 25;

const STEP_LIMIT: usize = 50;

pub fn is_backup_archive(name: &str) -> bool {
    name.ends_with(BACKUP_SUFFIX)
}

fn is_internal_keyspace(name: &str) -> bool {
    name == REGISTRY_KEYSPACE || name == SCHEMA_MAP_KEYSPACE
}

pub fn namespaced(namespace: impl AsRef<str>, name: impl AsRef<str>) -> String {
    format!("{}/{}", namespace.as_ref(), name.as_ref())
}

#[derive(Clone, Debug, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct BlobOptions {
    pub file_target_size: u64,
    pub separation_threshold: u32,
}

impl Default for BlobOptions {
    fn default() -> Self {
        Self {
            file_target_size: 64 * 1024 * 1024,
            separation_threshold: 1024,
        }
    }
}

impl BlobOptions {
    pub fn file_target_size(mut self, bytes: u64) -> Self {
        self.file_target_size = bytes;
        self
    }

    pub fn separation_threshold(mut self, threshold: u32) -> Self {
        self.separation_threshold = threshold;
        self
    }
}

/// The subset of [`SpaceOptions`] that affects on-disk segment layout. Only
/// these participate in the keyspace fingerprint; runtime-only knobs are excluded
/// so they can be retuned without tripping the reopen guard.
#[derive(PartialEq, Eq)]
struct FormatFingerprint {
    data_block_size: u32,
    expect_point_read_hits: bool,
    filter_block_partitioning: Option<bool>,
    filter_policy: Option<FilterConfig>,
    // Only whether kv separation is on: the knobs inside BlobOptions (target
    // size, threshold, compression) shape new writes only — every existing blob
    // file records its own compression and stays readable — so they may be
    // retuned freely. Toggling separation itself is a real format change.
    blob_enabled: bool,
    disable_compression: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum FilterConfig {
    Disabled,
    BloomBitsPerKey(u8),
}

#[derive(Clone, Debug, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct SpaceOptions {
    pub max_memtable_size: u64,
    pub data_block_size: u32,
    pub manual_journal_persist: bool,
    pub expect_point_read_hits: bool,
    pub filter_block_partitioning: Option<bool>,
    pub filter_policy: Option<FilterConfig>,
    pub blob: Option<BlobOptions>,
    pub fifo_compaction_bytes: Option<u64>,
    pub disable_compression: bool,
}

impl Default for SpaceOptions {
    fn default() -> Self {
        Self {
            max_memtable_size: 2 * 1024 * 1024,
            data_block_size: 4 * 1024,
            manual_journal_persist: false,
            expect_point_read_hits: false,
            filter_block_partitioning: None,
            filter_policy: None,
            blob: None,
            fifo_compaction_bytes: None,
            disable_compression: false,
        }
    }
}

impl SpaceOptions {
    pub fn max_memtable_size(mut self, bytes: u64) -> Self {
        self.max_memtable_size = bytes;
        self
    }

    pub fn data_block_size(mut self, bytes: u32) -> Self {
        self.data_block_size = bytes;
        self
    }

    pub fn manual_journal_persist(mut self, flag: bool) -> Self {
        self.manual_journal_persist = flag;
        self
    }

    pub fn expect_point_read_hits(mut self, flag: bool) -> Self {
        self.expect_point_read_hits = flag;
        self
    }

    pub fn filter_block_partitioning(mut self, flag: bool) -> Self {
        self.filter_block_partitioning = Some(flag);
        self
    }

    pub fn filter_policy(mut self, policy: FilterConfig) -> Self {
        self.filter_policy = Some(policy);
        self
    }

    pub fn disable_compression(mut self) -> Self {
        self.disable_compression = true;
        self
    }

    pub fn blob(mut self, blob: BlobOptions) -> Self {
        self.blob = Some(blob);
        self
    }

    /// FIFO compaction: once the space's segments exceed `bytes`, the oldest-written segments
    /// are dropped wholesale. Only suitable for append-style data (logs, caches).
    pub fn fifo_compaction(mut self, bytes: u64) -> Self {
        self.fifo_compaction_bytes = Some(bytes);
        self
    }

    /// Fingerprint of only the options that change bytes written to immutable
    /// segments, so reopening with a different runtime knob (memtable size,
    /// journal-persist policy) is allowed while a genuine on-disk format change
    /// is still rejected.
    fn fingerprint(&self) -> FormatFingerprint {
        FormatFingerprint {
            data_block_size: self.data_block_size,
            expect_point_read_hits: self.expect_point_read_hits,
            filter_block_partitioning: self.filter_block_partitioning,
            filter_policy: self.filter_policy.clone(),
            blob_enabled: self.blob.is_some(),
            disable_compression: self.disable_compression,
        }
    }

    fn into_create_options(self) -> KeyspaceCreateOptions {
        let mut options = KeyspaceCreateOptions::default()
            .max_memtable_size(self.max_memtable_size)
            .data_block_size_policy(BlockSizePolicy::all(self.data_block_size))
            .manual_journal_persist(self.manual_journal_persist)
            .expect_point_read_hits(self.expect_point_read_hits);

        if let Some(blob) = &self.blob {
            let mut kv_options = KvSeparationOptions::default()
                .file_target_size(blob.file_target_size)
                .separation_threshold(blob.separation_threshold);
            // `disable_compression` covers the whole space: data blocks below and,
            // here, the separated blob values where the bulk of the bytes live.
            if self.disable_compression {
                kv_options = kv_options.compression(CompressionType::None);
            }
            options = options.with_kv_separation(Some(kv_options));
        }

        if let Some(bytes) = self.fifo_compaction_bytes {
            options = options.compaction_strategy(Arc::new(fjall::compaction::Fifo::new(bytes, None)));
        }

        if let Some(filter_block_partitioning) = self.filter_block_partitioning {
            options = options.filter_block_partitioning_policy(PartitioningPolicy::all(filter_block_partitioning));
        }

        if let Some(filter_policy) = &self.filter_policy {
            let entry = match filter_policy {
                FilterConfig::Disabled => FilterPolicyEntry::None,
                FilterConfig::BloomBitsPerKey(bits) => {
                    FilterPolicyEntry::Bloom(BloomConstructionPolicy::BitsPerKey(f32::from(*bits)))
                }
            };
            options = options.filter_policy(FilterPolicy::all(entry));
        }

        if self.disable_compression {
            options = options.data_block_compression_policy(CompressionPolicy::new(&[CompressionType::None]))
        }

        options
    }
}

/// Runtime-tunable backup configuration. Every field is an [`ArcSwap`] behind a
/// shared [`Arc`], so cloning is cheap and all clones observe the same live
/// values. Hand a clone to the database and keep one yourself; scheduling is the
/// caller's concern — backups run only via [`ManagedDatabase::create_backup`].
#[derive(Clone)]
pub struct BackupSettings {
    inner: Arc<BackupSettingsInner>,
}

struct BackupSettingsInner {
    enabled: ArcSwap<bool>,
    path: ArcSwap<PathBuf>,
    max_backups: ArcSwap<u32>,
    max_size: ArcSwap<u64>,
}

impl BackupSettings {
    pub fn new(path: PathBuf, max_backups: u32, max_size: u64) -> Self {
        Self {
            inner: Arc::new(BackupSettingsInner {
                enabled: ArcSwap::from_pointee(true),
                path: ArcSwap::from_pointee(path),
                max_backups: ArcSwap::from_pointee(max_backups),
                max_size: ArcSwap::from_pointee(max_size),
            }),
        }
    }

    pub fn disabled() -> Self {
        let settings = Self::new(PathBuf::new(), 0, 0);
        settings.set_enabled(false);
        settings
    }

    pub fn enabled(&self) -> bool {
        **self.inner.enabled.load()
    }
    pub fn set_enabled(&self, enabled: bool) {
        self.inner.enabled.store(Arc::new(enabled));
    }

    pub fn path(&self) -> Arc<PathBuf> {
        self.inner.path.load_full()
    }
    pub fn set_path(&self, path: PathBuf) {
        self.inner.path.store(Arc::new(path));
    }

    pub fn max_backups(&self) -> u32 {
        **self.inner.max_backups.load()
    }
    pub fn set_max_backups(&self, max_backups: u32) {
        self.inner.max_backups.store(Arc::new(max_backups));
    }

    /// Max total size of all backups. If exceeded, old backups are deleted even when the number of
    /// backups is below `max_backups`. The latest backup is never deleted to satisfy this limit.
    /// Zero disables the size limit.
    pub fn max_size(&self) -> u64 {
        **self.inner.max_size.load()
    }
    pub fn set_max_size(&self, max_size: u64) {
        self.inner.max_size.store(Arc::new(max_size));
    }

    /// Whether a backup should actually be taken: enabled and pointed at a usable target.
    pub fn should_run(&self) -> bool {
        self.enabled() && self.max_backups() > 0 && !self.path().as_os_str().is_empty()
    }
}

/// Live progress of a running backup, shared with the caller through atomics so it can be
/// sampled from any thread without locking. Export counts logical key/value bytes copied into
/// the snapshot (against an estimate from the source directory's disk size); compression counts
/// exact bytes read from the snapshot files as they stream into the compressor.
pub struct BackupProgress {
    step: ArcSwap<String>,
    stage: AtomicU8,
    export_done: AtomicU64,
    export_total: AtomicU64,
    compress_done: AtomicU64,
    compress_total: AtomicU64,
    copy_done: AtomicU64,
    copy_total: AtomicU64,
    cancelled: AtomicBool,
}

const STAGE_RUNNING: u8 = 0;
const STAGE_COMPRESSED: u8 = 1;
const STAGE_RETAINED: u8 = 2;
const STAGE_FINISHED: u8 = 3;

impl Default for BackupProgress {
    fn default() -> Self {
        Self {
            step: ArcSwap::from_pointee(String::from("Copying database")),
            stage: AtomicU8::new(STAGE_RUNNING),
            export_done: AtomicU64::new(0),
            export_total: AtomicU64::new(0),
            compress_done: AtomicU64::new(0),
            compress_total: AtomicU64::new(0),
            copy_done: AtomicU64::new(0),
            copy_total: AtomicU64::new(0),
            cancelled: AtomicBool::new(false),
        }
    }
}

impl BackupProgress {
    /// The current backup step as a short human-readable line (at most 42 characters).
    pub fn step(&self) -> Arc<String> {
        self.step.load_full()
    }

    fn set_step(&self, step: String) {
        self.step.store(Arc::new(step));
    }

    /// Overall completion in `0.0..=1.0`. Export owns `0.0..0.5` and compression `0.5..0.93`:
    /// the export total is only a disk-size estimate, so per-phase fractions are clamped rather
    /// than blended into one byte count that could run backwards when the estimate is off. The
    /// last steps land at fixed milestones: 0.93 once compression finished, 0.95 after retention
    /// enforcement, then landing the archive owns `0.95..1.0` — driven by the copy byte count on a
    /// cross-volume copy, or a straight jump to 1.0 when a same-volume rename lands it instantly.
    pub fn fraction(&self) -> f64 {
        fn phase(done: u64, total: u64) -> f64 {
            if total == 0 {
                0.0
            } else {
                (done as f64 / total as f64).min(1.0)
            }
        }
        match self.stage.load(Ordering::Relaxed) {
            STAGE_FINISHED => return 1.0,
            STAGE_RETAINED => {
                let copy_total = self.copy_total.load(Ordering::Relaxed);
                return if copy_total > 0 {
                    0.95 + 0.05 * phase(self.copy_done.load(Ordering::Relaxed), copy_total)
                } else {
                    0.95
                };
            }
            STAGE_COMPRESSED => return 0.93,
            _ => {}
        }
        let compress_total = self.compress_total.load(Ordering::Relaxed);
        if compress_total > 0 {
            0.5 + 0.43 * phase(self.compress_done.load(Ordering::Relaxed), compress_total)
        } else {
            0.5 * phase(self.export_done.load(Ordering::Relaxed), self.export_total.load(Ordering::Relaxed))
        }
    }

    fn begin_export(&self, estimated_total: u64) {
        self.export_total.store(estimated_total.max(1), Ordering::Relaxed);
    }

    fn record_export(&self, bytes: u64) {
        self.export_done.fetch_add(bytes, Ordering::Relaxed);
    }

    fn begin_compression(&self, total: u64) {
        self.compress_total.store(total.max(1), Ordering::Relaxed);
    }

    fn begin_copy(&self, total: u64) {
        self.copy_done.store(0, Ordering::Relaxed);
        self.copy_total.store(total.max(1), Ordering::Relaxed);
    }

    fn record_copy(&self, done: u64) {
        self.copy_done.store(done, Ordering::Relaxed);
    }

    fn set_stage(&self, stage: u8) {
        self.stage.store(stage, Ordering::Relaxed);
    }

    pub fn request_cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    fn ensure_not_cancelled(&self) -> Result<()> {
        if self.is_cancelled() {
            return Err(anyhow!("backup cancelled"));
        }
        Ok(())
    }
}

struct CountingReader<'a, R> {
    inner: R,
    progress: &'a BackupProgress,
}

impl<R: Read> Read for CountingReader<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.progress.is_cancelled() {
            return Err(std::io::Error::other("backup cancelled"));
        }
        let n = self.inner.read(buf)?;
        self.progress.compress_done.fetch_add(n as u64, Ordering::Relaxed);
        Ok(n)
    }
}

pub struct ManagedDatabaseConfig {
    pub path: PathBuf,
    pub temporary: bool,
    pub cache_size: u64,
    pub backup: BackupSettings,
}

impl ManagedDatabaseConfig {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            temporary: false,
            cache_size: 8 * 1024 * 1024,
            backup: BackupSettings::disabled(),
        }
    }

    pub fn temporary(mut self, flag: bool) -> Self {
        self.temporary = flag;
        self
    }

    pub fn backup(mut self, settings: &BackupSettings) -> Self {
        self.backup = settings.clone();
        self
    }

    pub fn cache_size(mut self, bytes: u64) -> Self {
        self.cache_size = bytes;
        self
    }

    fn open_at(&self, path: &Path, temporary: bool) -> fjall::Result<OptimisticTxDatabase> {
        OptimisticTxDatabase::builder(path)
            .temporary(temporary)
            .cache_size(self.cache_size)
            .max_journaling_size(64 * 1024 * 1024)
            .open()
    }
}

/// The build phase of a database, handed to the [`open_managed_database`] closure.
/// Declare each space through a [`group`](Self::group); once the closure returns,
/// the framework garbage-collects any space that was not declared and starts the
/// background backup thread. There is no public way to construct or finalize this
/// type yourself, which guarantees that finalization always runs.
pub struct DatabaseInitializer {
    database: OptimisticTxDatabase,
    schema_map: SchemaMapSpace,
    registry: OptimisticTxKeyspace,
    previous: HashMap<String, Vec<u8>>,
    opened: HashSet<String>,
    config: ManagedDatabaseConfig,
}

impl DatabaseInitializer {
    fn open(config: ManagedDatabaseConfig) -> Result<Self> {
        let database = config.open_at(&config.path, config.temporary)?;

        let schema_map = SchemaMapSpace::open(&database)?;
        let registry = database.keyspace(REGISTRY_KEYSPACE, KeyspaceCreateOptions::default)?;

        let mut previous = HashMap::new();
        {
            let snapshot = database.read_tx();
            for guard in snapshot.iter(&database, &registry) {
                let (key, value) = guard.into_inner()?;
                previous.insert(String::from_utf8(key.to_vec())?, value.to_vec());
            }
        }

        Ok(Self {
            database,
            schema_map,
            registry,
            previous,
            opened: HashSet::new(),
            config,
        })
    }

    fn finish<T>(self, state: T) -> Result<ManagedDatabase<T>> {
        let DatabaseInitializer {
            database,
            schema_map,
            registry,
            opened,
            config,
            ..
        } = self;

        let backups_enabled = config.backup.should_run();
        let mut backed_up = false;
        for name in database.list_keyspace_names() {
            let name: &str = &name;
            if is_internal_keyspace(name) || opened.contains(name) {
                continue;
            }

            // Backup-before-destroy: an undeclared keyspace is about to be GC'd, so capture one
            // last archive of the old schema first.
            if backups_enabled && !backed_up {
                let tmp = config.path.parent().unwrap_or(&config.path).join("tmp");
                std::fs::create_dir_all(&tmp)?;
                let staging = tempfile::tempdir_in(&tmp)?;
                let archive = tempfile::tempdir_in(&tmp)?;
                run_backup(&database, &config, staging.path(), archive.path(), &BackupProgress::default())?;
                backed_up = true;
            }

            let stale = database.keyspace(name, KeyspaceCreateOptions::default)?;
            log::info!("Removing stale keyspace: {}", name);
            database.inner().delete_keyspace(stale.inner().clone())?;

            schema_map.remove_space(name)?;

            let mut tx = database.write_tx()?;
            tx.remove(&registry, name.as_bytes());
            tx.commit()??;
        }

        Ok(ManagedDatabase {
            database,
            state,
            config,
        })
    }

    pub fn database(&self) -> &OptimisticTxDatabase {
        &self.database
    }

    pub fn group(&mut self, namespace: impl Into<String>) -> SpaceGroup<'_> {
        SpaceGroup {
            initializer: self,
            namespace: namespace.into(),
        }
    }

    pub(crate) fn schema_map(&self) -> &SchemaMapSpace {
        &self.schema_map
    }

    pub(crate) fn open_keyspace(&mut self, full_name: String, options: SpaceOptions) -> Result<OptimisticTxKeyspace> {
        let encoded = Rkyv::serialize(&options)?;

        let up_to_date = match self.previous.get(&full_name) {
            Some(stored) => match Rkyv::deserialize_untrusted::<SpaceOptions>(stored) {
                Ok(stored_options) => {
                    if stored_options.fingerprint() != options.fingerprint() {
                        return Err(anyhow!(
                            "Keyspace '{full_name}' was previously opened with different options; \
                             refusing to reopen with a mismatching configuration"
                        ));
                    }

                    stored_options == options
                }
                Err(_) => false,
            },
            None => false,
        };

        let keyspace = self.database.keyspace(&full_name, || options.into_create_options())?;

        if !up_to_date {
            let mut tx = self.database.write_tx()?;
            tx.insert(&self.registry, full_name.clone().into_bytes(), encoded.clone());
            tx.commit()??;
            self.previous.insert(full_name.clone(), encoded);
        }

        self.opened.insert(full_name);
        Ok(keyspace)
    }
}

/// A namespaced view over a [`DatabaseInitializer`]. All spaces opened through a
/// group share its namespace, so callers name each space once without repeating
/// the namespace prefix.
pub struct SpaceGroup<'a> {
    initializer: &'a mut DatabaseInitializer,
    namespace: String,
}

impl SpaceGroup<'_> {
    pub fn database(&self) -> &OptimisticTxDatabase {
        self.initializer.database()
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub(crate) fn schema_map(&self) -> &SchemaMapSpace {
        self.initializer.schema_map()
    }

    pub(crate) fn full_name(&self, name: impl AsRef<str>) -> String {
        namespaced(&self.namespace, name)
    }

    pub(crate) fn open_keyspace(&mut self, full_name: String, options: SpaceOptions) -> Result<OptimisticTxKeyspace> {
        self.initializer.open_keyspace(full_name, options)
    }
}

/// A live database produced by [`open_managed_database`], carrying the value `T`
/// that your initializer closure built (typically your set of opened spaces). It
/// derefs to the underlying [`OptimisticTxDatabase`] for transactions, exposes
/// your value through [`state`](Self::state), and takes on-demand backups via
/// [`create_backup`](Self::create_backup).
#[must_use]
pub struct ManagedDatabase<T> {
    database: OptimisticTxDatabase,
    state: T,
    config: ManagedDatabaseConfig,
}

impl<T> ManagedDatabase<T> {
    pub fn state(&self) -> &T {
        &self.state
    }

    pub fn state_mut(&mut self) -> &mut T {
        &mut self.state
    }

    pub fn config(&self) -> &ManagedDatabaseConfig {
        &self.config
    }

    pub fn backup_settings(&self) -> &BackupSettings {
        &self.config.backup
    }

    /// Takes a consistent snapshot of the live database and compresses it into the configured
    /// backup directory, reporting through `progress`. Runs on the calling thread; the snapshot
    /// is a `read_tx`, so serving continues throughout. Returns the archive file name.
    pub fn create_backup(&self, staging_dir: &Path, archive_dir: &Path, progress: &BackupProgress) -> Result<String> {
        run_backup(&self.database, &self.config, staging_dir, archive_dir, progress)
    }
}

impl<T> Deref for ManagedDatabase<T> {
    type Target = OptimisticTxDatabase;

    fn deref(&self) -> &Self::Target {
        &self.database
    }
}

/// Opens a database, lets `init` declare its spaces, then finalizes it: any space
/// not declared during `init` is garbage-collected and the background backup
/// thread is started. Finalization is run by this function, not the caller, so it
/// can never be skipped. If `init` returns an error, the database is discarded
/// without finalizing. The built value is returned inside the [`ManagedDatabase`].
pub fn open_managed_database<T>(
    config: ManagedDatabaseConfig,
    init: impl FnOnce(&mut DatabaseInitializer) -> Result<T>,
) -> Result<ManagedDatabase<T>> {
    let mut initializer = DatabaseInitializer::open(config)?;
    let state = init(&mut initializer)?;
    initializer.finish(state)
}

fn copying_step(keyspace: &str) -> String {
    const PREFIX: &str = "Copying ";
    const SUFFIX: &str = " data";
    const ELLIPSIS: &str = "...";
    let budget = STEP_LIMIT - PREFIX.len() - SUFFIX.len();
    if keyspace.chars().count() <= budget {
        return format!("{PREFIX}{keyspace}{SUFFIX}");
    }
    let end = keyspace.char_indices().nth(budget - ELLIPSIS.len()).map_or(keyspace.len(), |(i, _)| i);
    format!("{PREFIX}{}{ELLIPSIS}{SUFFIX}", &keyspace[..end])
}

fn run_backup(
    database: &OptimisticTxDatabase,
    config: &ManagedDatabaseConfig,
    staging_dir: &Path,
    archive_dir: &Path,
    progress: &BackupProgress,
) -> Result<String> {
    progress.set_step("Copying database".to_owned());
    let source_dir = config.path.as_path();
    let settings = &config.backup;
    let path = settings.path();
    if path.as_os_str().is_empty() {
        return Err(anyhow!("no backup directory configured"));
    }
    std::fs::create_dir_all(path.as_ref())?;

    for entry in std::fs::read_dir(path.as_ref())?.flatten() {
        if entry.path().extension().is_some_and(|e| e == "incoming") {
            let _ = std::fs::remove_file(entry.path());
        }
    }

    let source_size = directory_size(source_dir);
    if check_free_space(source_size, source_dir, path.as_ref(), staging_dir).is_err() {
        reclaim_backups_for_space(source_size, source_dir, path.as_ref(), staging_dir)?;
    }
    check_free_space(source_size, source_dir, path.as_ref(), staging_dir)?;

    progress.begin_export(source_size);

    {
        let backup_db = config.open_at(staging_dir, false)?;
        let snapshot = database.read_tx();
        database.persist(PersistMode::SyncAll)?;

        let registry = database.keyspace(REGISTRY_KEYSPACE, KeyspaceCreateOptions::default)?;
        let mut space_options: HashMap<String, SpaceOptions> = HashMap::new();
        for guard in snapshot.iter(database, &registry) {
            let (key, value) = guard.into_inner()?;
            if let Ok(options) = Rkyv::deserialize_untrusted::<SpaceOptions>(&value) {
                space_options.insert(String::from_utf8(key.to_vec())?, options);
            }
        }

        let mut batch = OwnedWriteBatch::with_capacity(backup_db.inner().clone(), 1024);
        let mut batch_bytes = 0usize;

        for name in database.list_keyspace_names() {
            if !is_internal_keyspace(&name) {
                progress.set_step(copying_step(&name));
            }
            let source = database.keyspace(&name, KeyspaceCreateOptions::default)?;
            let target = match space_options.get(&*name) {
                Some(options) => backup_db.keyspace(&name, || options.clone().into_create_options())?,
                None => backup_db.keyspace(&name, KeyspaceCreateOptions::default)?,
            };

            for guard in snapshot.iter(database, &source) {
                progress.ensure_not_cancelled()?;
                let (key, value) = guard.into_inner()?;
                batch_bytes += key.len() + value.len() + (16 * 1024);
                progress.record_export((key.len() + value.len()) as u64);
                batch.insert(target.inner(), key, value);

                if batch_bytes >= BACKUP_BATCH_BYTES {
                    batch.commit()?;
                    crate::wait_for_write_buffer(backup_db.inner());
                    batch = OwnedWriteBatch::with_capacity(backup_db.inner().clone(), 1024);
                    batch_bytes = 0;
                }
            }
        }

        batch.commit()?;
        backup_db.persist(PersistMode::SyncAll)?;
        drop(snapshot);

        progress.set_step("Compacting staged database".to_owned());
        for name in backup_db.list_keyspace_names() {
            progress.ensure_not_cancelled()?;
            let keyspace = backup_db.keyspace(&name, KeyspaceCreateOptions::default)?;
            keyspace.inner().rotate_memtable_and_wait()?;
            keyspace.inner().major_compact()?;
        }
    }

    let timestamp = jiff::Zoned::now().strftime("%Y-%m-%d_%H-%M");
    let archive_name = format!("backup_{timestamp}{BACKUP_SUFFIX}");
    let archive_path = archive_dir.join(&archive_name);

    progress.set_step("Packing backup".to_owned());
    progress.begin_compression(directory_size(staging_dir));
    compress_directory(staging_dir, &archive_path, progress)?;
    progress.set_stage(STAGE_COMPRESSED);

    progress.ensure_not_cancelled()?;
    progress.set_step("Cleaning up old backups".to_owned());
    let archive_size = std::fs::metadata(&archive_path)?.len();
    enforce_retention(path.as_ref(), archive_size, settings.max_backups(), settings.max_size())?;
    progress.set_stage(STAGE_RETAINED);

    progress.ensure_not_cancelled()?;
    land_archive(&archive_path, path.as_ref(), &archive_name, progress)?;

    progress.set_stage(STAGE_FINISHED);
    // Artificial delay so the UI sees 100% for a bit
    std::thread::sleep(Duration::from_millis(750));

    Ok(archive_name)
}

fn land_archive(archive_path: &Path, target_dir: &Path, archive_name: &str, progress: &BackupProgress) -> Result<()> {
    let destination = target_dir.join(archive_name);
    if std::fs::rename(archive_path, &destination).is_err() {
        // cross-volume: copy under a non-archive name (invisible to listing/retention/restore) and
        // rename into place, so a failure can't leave a partial file that looks like a valid backup
        progress.set_step("Copying backup to destination".to_owned());
        progress.begin_copy(std::fs::metadata(archive_path).map(|m| m.len()).unwrap_or(0));
        let incoming = target_dir.join(format!("{archive_name}.incoming"));
        let landed = copy_file_cancellable(archive_path, &incoming, progress)
            .and_then(|()| std::fs::rename(&incoming, &destination).map_err(anyhow::Error::from));
        if let Err(e) = landed {
            let _ = std::fs::remove_file(&incoming);
            return Err(e);
        }
    }
    Ok(())
}

/// Cross-volume copy that checks cancellation between chunks, so a cancelled backup
/// abandons a large copy promptly instead of running it to completion. `writer` is
/// closed as it drops here, before the caller renames `dst` (matters on Windows,
/// where an open handle blocks the rename). The caller removes `dst` on any error.
fn copy_file_cancellable(src: &Path, dst: &Path, progress: &BackupProgress) -> Result<()> {
    let mut reader = std::fs::File::open(src)?;
    let mut writer = std::fs::File::create(dst)?;
    let mut buf = vec![0u8; 1024 * 1024];
    let mut copied = 0u64;
    loop {
        progress.ensure_not_cancelled()?;
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        writer.write_all(&buf[..n])?;
        copied += n as u64;
        progress.record_copy(copied);
    }
    Ok(())
}

pub fn pack_prerestore_snapshot(
    store_dir: &Path,
    target_dir: &Path,
    archive_name: &str,
    settings: &BackupSettings,
    progress: &BackupProgress,
) -> Result<()> {
    progress.set_step("Compacting pre-restore database".to_owned());
    {
        let db = OptimisticTxDatabase::builder(store_dir).open()?;
        for name in db.list_keyspace_names() {
            progress.ensure_not_cancelled()?;
            let keyspace = db.keyspace(&name, KeyspaceCreateOptions::default)?;
            keyspace.inner().rotate_memtable_and_wait()?;
            keyspace.inner().major_compact()?;
        }
        db.persist(PersistMode::SyncAll)?;
    }

    progress.ensure_not_cancelled()?;
    std::fs::create_dir_all(target_dir)?;
    let archive_path = store_dir.parent().ok_or_else(|| anyhow!("store dir has no parent"))?.join(archive_name);

    progress.set_step("Packing pre-restore snapshot".to_owned());
    progress.begin_compression(directory_size(store_dir));
    compress_directory(store_dir, &archive_path, progress)?;
    progress.set_stage(STAGE_COMPRESSED);

    progress.ensure_not_cancelled()?;
    progress.set_step("Cleaning up old backups".to_owned());
    let archive_size = std::fs::metadata(&archive_path)?.len();
    enforce_retention(target_dir, archive_size, settings.max_backups(), settings.max_size())?;
    progress.set_stage(STAGE_RETAINED);

    progress.ensure_not_cancelled()?;
    land_archive(&archive_path, target_dir, archive_name, progress)?;

    progress.set_stage(STAGE_FINISHED);
    std::thread::sleep(Duration::from_millis(750));
    Ok(())
}

/// Aborts before a backup starts unless every volume involved has headroom. The run writes a full
/// export into the staging dir, compresses that into a staged archive, then lands it in the
/// target dir — each can transiently hold roughly a source-sized copy, so we demand 2x the source
/// DB size free on all three rather than fail deep into a long run. Staging and the archive both
/// live under `temp_dir`, so one check covers them.
fn check_free_space(source_size: u64, source_dir: &Path, target_dir: &Path, temp_dir: &Path) -> Result<()> {
    let required = required_backup_space(source_size);
    for (label, dir) in [("source", source_dir), ("backup target", target_dir), ("staging", temp_dir)] {
        let available = fs4::available_space(dir)
            .map_err(|e| anyhow!("failed to query free space for {label} dir {}: {e}", dir.display()))?;
        if available < required {
            return Err(anyhow!(
                "insufficient free space for backup in {label} dir {}: {} free, need {} (2x source DB size {})",
                dir.display(),
                bytesize::ByteSize(available).display().iec(),
                bytesize::ByteSize(required).display().iec(),
                bytesize::ByteSize(source_size).display().iec(),
            ));
        }
    }
    Ok(())
}

// saturating so a huge DB can't wrap the requirement to a small number
fn required_backup_space(source_size: u64) -> u64 {
    source_size.saturating_mul(2)
}

/// When the space guard fails, deletes existing backups (oldest first) until it passes — but only
/// if freeing all of them provably satisfies the guard on every involved volume, so a hopeless run
/// never destroys backups. Freed bytes only count toward volumes shared with the backup target.
fn reclaim_backups_for_space(source_size: u64, source_dir: &Path, target_dir: &Path, temp_dir: &Path) -> Result<()> {
    let archives = list_backup_archives(target_dir)?;
    if archives.is_empty() {
        return Ok(());
    }
    let reclaimable: u64 = archives.iter().map(|(_, size)| *size).sum();

    let required = required_backup_space(source_size);
    for dir in [source_dir, temp_dir, target_dir] {
        let credit = if same_volume(dir, target_dir) {
            reclaimable
        } else {
            0
        };
        if fs4::available_space(dir)?.saturating_add(credit) < required {
            return Ok(());
        }
    }

    for (path, _) in &archives {
        if check_free_space(source_size, source_dir, target_dir, temp_dir).is_ok() {
            break;
        }
        std::fs::remove_file(path)?;
        log::warn!("deleted backup {} to free space for a new backup", path.display());
    }
    Ok(())
}

#[cfg(unix)]
fn same_volume(a: &Path, b: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    match (std::fs::metadata(a), std::fs::metadata(b)) {
        (Ok(a), Ok(b)) => a.dev() == b.dev(),
        _ => false,
    }
}

#[cfg(not(unix))]
fn same_volume(a: &Path, b: &Path) -> bool {
    // Drive/UNC prefix of the canonical path; unknown counts as a different volume.
    fn root(p: &Path) -> Option<std::ffi::OsString> {
        Some(p.canonicalize().ok()?.components().next()?.as_os_str().to_os_string())
    }
    match (root(a), root(b)) {
        (Some(a), Some(b)) => a == b,
        _ => false,
    }
}

fn directory_size(dir: &Path) -> u64 {
    let mut total = 0u64;
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    for entry in entries.flatten() {
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if file_type.is_dir() {
            total += directory_size(&entry.path());
        } else if file_type.is_file() {
            total += entry.metadata().map(|m| m.len()).unwrap_or(0);
        }
    }
    total
}

fn compress_directory(source: &Path, destination: &Path, progress: &BackupProgress) -> Result<()> {
    let file = std::fs::File::create(destination)?;
    let mut encoder = zstd::stream::write::Encoder::new(file, ZSTD_LEVEL)?;
    encoder.long_distance_matching(true)?;
    encoder.window_log(ZSTD_WINDOW_LOG)?;
    let mut archive = tar::Builder::new(encoder);
    push_directory(&mut archive, source, source, progress)?;
    archive.into_inner()?.finish()?.sync_all()?;
    Ok(())
}

fn push_directory<W: std::io::Write>(
    archive: &mut tar::Builder<W>,
    dir: &Path,
    root: &Path,
    progress: &BackupProgress,
) -> Result<()> {
    for entry in std::fs::read_dir(dir)? {
        progress.ensure_not_cancelled()?;
        let entry = entry?;
        let path = entry.path();
        let name = path.strip_prefix(root)?;
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            archive.append_dir(name, &path)?;
            push_directory(archive, &path, root, progress)?;
        } else if file_type.is_file() {
            let file = std::fs::File::open(&path)?;
            let mut header = tar::Header::new_gnu();
            header.set_metadata(&file.metadata()?);
            let reader = CountingReader {
                inner: file,
                progress,
            };
            archive.append_data(&mut header, name, reader)?;
            let _ = std::fs::remove_file(&path);
        }
    }
    Ok(())
}

/// A backup archive on disk, surfaced to the admin UI.
pub struct BackupEntry {
    pub name: String,
    pub bytes: u64,
    pub modified: SystemTime,
    pub prerestore: bool,
}

/// List every backup archive in `directory`, newest first. Missing dir → empty list.
pub fn list_backups(directory: &Path) -> Result<Vec<BackupEntry>> {
    let mut entries = Vec::new();
    let read = match std::fs::read_dir(directory) {
        Ok(r) => r,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(entries),
        Err(e) => return Err(e.into()),
    };
    for entry in read {
        let entry = entry?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !is_backup_archive(name) {
            continue;
        }
        let meta = entry.metadata()?;
        entries.push(BackupEntry {
            name: name.to_owned(),
            bytes: meta.len(),
            modified: meta.modified().unwrap_or(UNIX_EPOCH),
            prerestore: name.contains(".prerestore."),
        });
    }
    entries.sort_by_key(|e| std::cmp::Reverse(e.modified));
    Ok(entries)
}

/// Backup archives in `directory`, oldest first.
fn list_backup_archives(directory: &Path) -> Result<Vec<(PathBuf, u64)>> {
    let mut archives: Vec<(PathBuf, u64, SystemTime)> = Vec::new();

    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        if !path.file_name().and_then(|n| n.to_str()).is_some_and(is_backup_archive) {
            continue;
        }
        let meta = entry.metadata()?;
        let modified = meta.modified().unwrap_or(UNIX_EPOCH);
        archives.push((path, meta.len(), modified));
    }

    archives.sort_by_key(|(_, _, modified)| *modified);
    Ok(archives.into_iter().map(|(path, size, _)| (path, size)).collect())
}

fn enforce_retention(directory: &Path, new_size: u64, max_backups: u32, max_size: u64) -> Result<()> {
    let archives = list_backup_archives(directory)?;

    let mut total: u64 = new_size + archives.iter().map(|(_, size)| *size).sum::<u64>();
    let mut count = archives.len() + 1;

    for (path, size) in &archives {
        let over_count = count > max_backups as usize;
        let over_size = max_size > 0 && total > max_size;
        if !over_count && !over_size {
            break;
        }
        std::fs::remove_file(path)?;
        total = total.saturating_sub(*size);
        count -= 1;
    }

    Ok(())
}
