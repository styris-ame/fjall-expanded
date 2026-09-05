#![allow(dead_code)]

use std::path::PathBuf;

use anyhow::Result;
use easy_rkyv::TrustedRkyvImpl;
use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, PersistMode};
use migrateable::{HashedTypeDef, MigrateResult, Migrateable, RkyvType, Type};

use crate::{
    BackupSettings, Readable, SpaceOptions, ManagedDatabaseConfig, VersionedRkyvSpace, encode_key_prefix, namespaced,
    open_managed_database,
};

const REGISTRY_KEYSPACE: &str = "__fjall_expanded_registry";
const SCHEMA_MAP_KEYSPACE: &str = "__fjall_expanded_migrateable_schema";
const SYNC: PersistMode = PersistMode::SyncData;

fn ns(i: usize) -> String {
    format!("space{i:03}")
}

fn make_id(space: usize, entry: usize) -> [u8; 16] {
    let mut id = [0u8; 16];
    id[..8].copy_from_slice(&(space as u64).to_be_bytes());
    id[8..].copy_from_slice(&(entry as u64).to_be_bytes());
    id
}

fn make_payload(id: &[u8; 16], len: usize) -> Vec<u8> {
    let seed = id[15].wrapping_add(id[7]);
    (0..len).map(|j| seed.wrapping_add(j as u8)).collect()
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, HashedTypeDef)]
struct StressData {
    id: [u8; 16],
    payload: Vec<u8>,
}

#[derive(Migrateable)]
enum StressKey {
    V1(Type<[u8; 16]>),
}

#[derive(Migrateable)]
enum StressVal {
    V1(RkyvType<StressData>),
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, HashedTypeDef)]
struct FileDataV1 {
    id: [u8; 16],
    payload: Vec<u8>,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, HashedTypeDef)]
struct FileDataV2 {
    id: [u8; 16],
    payload: Vec<u8>,
    migrated: bool,
}

#[derive(Migrateable)]
enum MigKey {
    V1(Type<[u8; 16]>),
}

#[derive(Migrateable)]
enum FileValV1 {
    V1(RkyvType<FileDataV1>),
}

#[derive(Migrateable)]
enum FileValV2 {
    V1(RkyvType<FileDataV1>),
    V2(RkyvType<FileDataV2>),
}

impl MigrateToCurrentFileValV2 for FileDataV1 {
    fn migrate(self) -> MigrateResult<FileDataV2> {
        Ok(FileDataV2 {
            id: self.id,
            payload: self.payload,
            migrated: true,
        })
    }
}

#[test]
fn stress_staleness_deletions() -> Result<()> {
    const TOTAL: usize = 12;
    const KEEP: usize = 4;
    const ENTRIES: usize = 3000;
    const PAYLOAD: usize = 512;

    let dir = tempfile::tempdir()?;

    {
        let db = open_managed_database(ManagedDatabaseConfig::new(dir.path()).temporary(false), |init| {
            let mut spaces: Vec<VersionedRkyvSpace<StressKey, StressVal>> = Vec::new();
            for i in 0..TOTAL {
                let mut group = init.group(ns(i));
                spaces.push(VersionedRkyvSpace::stage_open(&mut group, "data", SpaceOptions::default())?);
            }
            Ok(spaces)
        })?;

        for (i, space) in db.state().iter().enumerate() {
            let mut tx = db.write_tx()?;
            for e in 0..ENTRIES {
                let id = make_id(i, e);
                space.insert(
                    &mut tx,
                    id,
                    StressData {
                        id,
                        payload: make_payload(&id, PAYLOAD),
                    },
                )?;
            }
            tx.commit()??;
        }
        db.persist(SYNC)?;
    }

    let db = open_managed_database(ManagedDatabaseConfig::new(dir.path()).temporary(false), |init| {
        let mut spaces: Vec<VersionedRkyvSpace<StressKey, StressVal>> = Vec::new();
        for i in 0..KEEP {
            let mut group = init.group(ns(i));
            spaces.push(VersionedRkyvSpace::stage_open(&mut group, "data", SpaceOptions::default())?);
        }
        Ok(spaces)
    })?;

    let names: Vec<String> = db.list_keyspace_names().iter().map(|n| n.to_string()).collect();
    for i in 0..KEEP {
        let full = namespaced(ns(i), "data");
        assert!(names.contains(&full), "kept keyspace {full} missing from list");
    }
    for i in KEEP..TOTAL {
        let full = namespaced(ns(i), "data");
        assert!(!names.contains(&full), "stale keyspace {full} was not garbage-collected");
    }

    let snap = db.read_tx();

    let registry = db.keyspace(REGISTRY_KEYSPACE, KeyspaceCreateOptions::default)?;
    let registry_keys: Vec<String> = snap
        .iter(&db, &registry)
        .map(|g| -> Result<String> {
            let key = g.key()?;
            Ok(String::from_utf8(key.to_vec())?)
        })
        .collect::<Result<Vec<_>>>()?;
    for i in 0..KEEP {
        let full = namespaced(ns(i), "data");
        assert!(registry_keys.contains(&full), "kept registry entry {full} missing");
    }
    for i in KEEP..TOTAL {
        let full = namespaced(ns(i), "data");
        assert!(!registry_keys.contains(&full), "stale registry entry {full} not removed");
    }

    let schema = db.keyspace(SCHEMA_MAP_KEYSPACE, KeyspaceCreateOptions::default)?;
    for i in 0..KEEP {
        let full = namespaced(ns(i), "data");
        let count = snap.prefix(&db, &schema, encode_key_prefix(&full)?).count();
        assert!(count > 0, "kept schema entries for {full} missing");
    }
    for i in KEEP..TOTAL {
        let full = namespaced(ns(i), "data");
        let count = snap.prefix(&db, &schema, encode_key_prefix(&full)?).count();
        assert_eq!(count, 0, "stale schema entries for {full} not removed");
    }

    for (i, space) in db.state().iter().enumerate() {
        let mut seen = 0usize;
        for kv in space.iter(&snap) {
            let (_key, value) = kv?;
            let data = value.deserialize()?;
            assert_eq!(data.payload, make_payload(&data.id, PAYLOAD));
            assert_eq!(data.id[..8], (i as u64).to_be_bytes());
            seen += 1;
        }
        assert_eq!(seen, ENTRIES, "kept space {i} lost data");
    }

    Ok(())
}

#[test]
fn stress_backup_parity() -> Result<()> {
    // ~16 KiB accounted per entry, so a single keyspace's >16.3k entries forces a
    // mid-iteration flush past the 256 MiB BACKUP_BATCH_BYTES threshold.
    const SPACES: usize = 2;
    const ENTRIES: usize = 20_000;
    const PAYLOAD: usize = 16;

    let dir = tempfile::tempdir()?;
    let backup_dir = tempfile::tempdir()?;

    let settings = BackupSettings::new(backup_dir.path().to_path_buf(), 4, 0);

    let db = open_managed_database(ManagedDatabaseConfig::new(dir.path()).temporary(false).backup(&settings), |init| {
        let mut spaces: Vec<VersionedRkyvSpace<StressKey, StressVal>> = Vec::new();
        for i in 0..SPACES {
            let mut group = init.group(ns(i));
            spaces.push(VersionedRkyvSpace::stage_open(&mut group, "data", SpaceOptions::default())?);
        }
        Ok(spaces)
    })?;

    for (i, space) in db.state().iter().enumerate() {
        let mut tx = db.write_tx()?;
        for e in 0..ENTRIES {
            let id = make_id(i, e);
            space.insert(
                &mut tx,
                id,
                StressData {
                    id,
                    payload: make_payload(&id, PAYLOAD),
                },
            )?;
        }
        tx.commit()??;
    }

    let staging_dir = tempfile::tempdir()?;
    let archive_dir = tempfile::tempdir()?;

    db.create_backup(staging_dir.path(), archive_dir.path(), &crate::BackupProgress::default())?;

    let archive = std::fs::read_dir(backup_dir.path())?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| p.file_name().and_then(|n| n.to_str()).is_some_and(|n| n.ends_with(crate::BACKUP_SUFFIX)))
        .expect("no backup archive produced");

    let restore = tempfile::tempdir()?;
    let restore_path: PathBuf = restore.path().to_path_buf();
    let decoder = zstd::stream::read::Decoder::new(std::fs::File::open(&archive)?)?;
    tar::Archive::new(decoder).unpack(&restore_path).expect("decompression failed");

    let restored = OptimisticTxDatabase::builder(&restore_path).open()?;

    let snap_orig = db.read_tx();
    let snap_rest = restored.read_tx();

    let mut original_names: Vec<String> = db.list_keyspace_names().iter().map(|n| n.to_string()).collect();
    let mut restored_names: Vec<String> = restored.list_keyspace_names().iter().map(|n| n.to_string()).collect();
    original_names.sort();
    restored_names.sort();
    assert_eq!(original_names, restored_names, "restored keyspace set differs");

    for name in &original_names {
        let ks_o = db.keyspace(name, KeyspaceCreateOptions::default)?;
        let ks_r = restored.keyspace(name, KeyspaceCreateOptions::default)?;

        let mut orig = Vec::new();
        for g in snap_orig.iter(&db, &ks_o) {
            let (k, v) = g.into_inner()?;
            orig.push((k.to_vec(), v.to_vec()));
        }

        let mut rest = Vec::new();
        for g in snap_rest.iter(&restored, &ks_r) {
            let (k, v) = g.into_inner()?;
            rest.push((k.to_vec(), v.to_vec()));
        }

        assert_eq!(orig, rest, "backup parity mismatch in keyspace {name}");
    }

    Ok(())
}

#[test]
fn stress_data_migration() -> Result<()> {
    // ~16 KiB accounted per entry, so a single keyspace's >16.3k entries forces the
    // value-migration loop past its 256 MiB batch limit into a second pass.
    const SPACES: usize = 2;
    const ENTRIES: usize = 20_000;
    const PAYLOAD: usize = 16;

    let dir = tempfile::tempdir()?;

    {
        let db = open_managed_database(ManagedDatabaseConfig::new(dir.path()).temporary(false), |init| {
            let mut spaces: Vec<VersionedRkyvSpace<MigKey, FileValV1>> = Vec::new();
            for i in 0..SPACES {
                let mut group = init.group(ns(i));
                spaces.push(VersionedRkyvSpace::stage_open(&mut group, "data", SpaceOptions::default())?);
            }
            Ok(spaces)
        })?;

        for (i, space) in db.state().iter().enumerate() {
            let mut tx = db.write_tx()?;
            for e in 0..ENTRIES {
                let id = make_id(i, e);
                space.insert(
                    &mut tx,
                    id,
                    FileDataV1 {
                        id,
                        payload: make_payload(&id, PAYLOAD),
                    },
                )?;
            }
            tx.commit()??;
        }
        db.persist(SYNC)?;
    }

    let db = open_managed_database(ManagedDatabaseConfig::new(dir.path()).temporary(false), |init| {
        let mut spaces: Vec<VersionedRkyvSpace<MigKey, FileValV2>> = Vec::new();
        for i in 0..SPACES {
            let mut group = init.group(ns(i));
            spaces.push(VersionedRkyvSpace::stage_open(&mut group, "data", SpaceOptions::default())?);
        }
        Ok(spaces)
    })?;

    let snap = db.read_tx();
    let mut total = 0usize;
    for (i, space) in db.state().iter().enumerate() {
        let mut seen = 0usize;
        for kv in space.iter(&snap) {
            let (_key, value) = kv?;
            let data = value.deserialize()?;
            assert!(data.migrated, "value in space {i} was not migrated");
            assert_eq!(data.payload, make_payload(&data.id, PAYLOAD));
            assert_eq!(data.id[..8], (i as u64).to_be_bytes());
            seen += 1;
        }
        assert_eq!(seen, ENTRIES, "space {i} lost data during migration");
        total += seen;
    }
    assert_eq!(total, SPACES * ENTRIES);

    Ok(())
}
