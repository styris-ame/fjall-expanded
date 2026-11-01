//! Documents held as parts, with `SearchSpace::search_visible` counting only some of them.
//!
//!   cargo test -p fjall_expanded --features search --test search_parts

#![cfg(feature = "search")]

use fjall::PersistMode;
use fjall_expanded::{ManagedDatabase, ManagedDatabaseConfig, SearchSpace, SpaceOptions, open_managed_database};

type Space = ManagedDatabase<SearchSpace<[u8; 16]>>;

fn id(n: u64) -> [u8; 16] {
    let mut bytes = [0u8; 16];
    bytes[8..].copy_from_slice(&n.to_be_bytes());
    bytes
}

fn open() -> (tempfile::TempDir, Space) {
    let dir = tempfile::tempdir().unwrap();
    let database = open_managed_database(ManagedDatabaseConfig::new(dir.path()), |init| {
        let mut group = init.group("search");
        SearchSpace::<[u8; 16]>::open(&mut group, SpaceOptions::default())
    })
    .unwrap();
    (dir, database)
}

fn index(database: &Space, doc: u64, main: &str, parts: &[(&str, &str)]) {
    let mut tx = database.write_tx().unwrap().durability(Some(PersistMode::Buffer));
    database.state().index_parts::<String, _, _>(&mut tx, id(doc), main, parts.iter().copied()).unwrap();
    tx.commit().unwrap().ok();
}

/// What a search finds counting just the parts `shown` names, of whichever document
fn search(database: &Space, query: &str, shown: &[&str]) -> Vec<[u8; 16]> {
    let read = database.read_tx();
    let found = database.state().search_visible::<String>(&read, query, |_, part| shown.contains(&part.as_str()));
    found.unwrap().into_iter().map(|key| key.get()).collect()
}

#[test]
fn words_from_different_parts_meet_one_query() {
    let (_dir, database) = open();
    index(&database, 1, "apple", &[("secret", "banana"), ("other", "cherry")]);
    assert_eq!(search(&database, "apple banana cherry", &["secret", "other"]), vec![id(1)]);
    assert_eq!(search(&database, "\"banana cherry\"", &["secret", "other"]), vec![id(1)]);
    assert!(search(&database, "apple banana cherry", &["secret"]).is_empty());
}

#[test]
fn a_part_that_doesnt_count_neither_finds_nor_excludes() {
    let (_dir, database) = open();
    index(&database, 1, "apple", &[("secret", "banana")]);
    assert!(search(&database, "banana", &[]).is_empty());
    assert!(search(&database, "?banana", &[]).is_empty());
    assert_eq!(search(&database, "apple -banana", &[]), vec![id(1)]);
    assert!(search(&database, "apple -banana", &["secret"]).is_empty());
    assert_eq!(search(&database, "banana", &["secret"]), vec![id(1)]);
}

#[test]
fn the_main_text_always_counts() {
    let (_dir, database) = open();
    index(&database, 1, "apple", &[]);
    index(&database, 2, "", &[("secret", "apple")]);
    assert_eq!(search(&database, "apple", &[]), vec![id(1)]);
    assert_eq!(search(&database, "apple", &["secret"]), vec![id(1), id(2)]);
}

#[test]
fn each_document_is_asked_about_its_own_parts() {
    let (_dir, database) = open();
    index(&database, 1, "", &[("secret", "apple")]);
    index(&database, 2, "", &[("secret", "apple")]);
    let read = database.read_tx();
    let found: Vec<[u8; 16]> = database
        .state()
        .search_visible::<String>(&read, "apple", |doc, part| doc == id(2) && part == "secret")
        .unwrap()
        .into_iter()
        .map(|key| key.get())
        .collect();
    assert_eq!(found, vec![id(2)]);
}

#[test]
fn only_parts_that_count_rank() {
    let (_dir, database) = open();
    index(&database, 1, "apple apple", &[]);
    index(&database, 2, "apple", &[("secret", "apple apple apple")]);
    // Hidden, doc 2 holds the word once to doc 1's twice. Shown, it holds it four times.
    assert_eq!(search(&database, "apple", &[]), vec![id(1), id(2)]);
    assert_eq!(search(&database, "apple", &["secret"]), vec![id(2), id(1)]);
}

#[test]
fn closeness_is_only_scored_within_a_part() {
    let (_dir, database) = open();
    index(&database, 1, "apple", &[("secret", "banana")]);
    index(&database, 2, "", &[("secret", "apple banana")]);
    assert_eq!(search(&database, "apple banana", &["secret"]), vec![id(2), id(1)]);
}

#[test]
fn reindexing_drops_the_parts_left_out() {
    let (_dir, database) = open();
    index(&database, 1, "apple", &[("secret", "banana"), ("other", "cherry")]);
    index(&database, 1, "apple", &[("other", "cherry")]);
    assert!(search(&database, "banana", &["secret", "other"]).is_empty());
    assert_eq!(search(&database, "cherry", &["other"]), vec![id(1)]);

    let mut tx = database.write_tx().unwrap();
    database.state().index_document(&mut tx, id(1), "apple").unwrap();
    tx.commit().unwrap().ok();
    assert!(search(&database, "cherry", &["other"]).is_empty());
    assert_eq!(search(&database, "apple", &[]), vec![id(1)]);
}

#[test]
fn removing_a_document_removes_every_part() {
    let (_dir, database) = open();
    index(&database, 1, "apple", &[("secret", "banana")]);
    index(&database, 2, "apple", &[("secret", "banana")]);
    let mut tx = database.write_tx().unwrap();
    database.state().remove_document(&mut tx, id(1)).unwrap();
    tx.commit().unwrap().ok();
    assert_eq!(search(&database, "apple", &["secret"]), vec![id(2)]);
    assert_eq!(search(&database, "banana", &["secret"]), vec![id(2)]);
    assert_eq!(database.state().search(&database.read_tx(), "banana").unwrap().len(), 1);
}

#[test]
fn a_part_named_twice_or_by_nothing_is_refused() {
    let (_dir, database) = open();
    let mut tx = database.write_tx().unwrap();
    let space = database.state();
    assert!(space.index_parts::<String, _, _>(&mut tx, id(1), "apple", [("secret", "a"), ("secret", "b")]).is_err());
    assert!(space.index_parts::<String, _, _>(&mut tx, id(1), "apple", [("", "banana")]).is_err());
}

#[test]
fn documents_whose_ids_share_a_start_stay_apart() {
    let dir = tempfile::tempdir().unwrap();
    let database = open_managed_database(ManagedDatabaseConfig::new(dir.path()), |init| {
        let mut group = init.group("search");
        SearchSpace::<String>::open(&mut group, SpaceOptions::default())
    })
    .unwrap();
    let mut tx = database.write_tx().unwrap();
    let space = database.state();
    space.index_parts::<String, _, _>(&mut tx, "ab", "apple", [("c", "banana")]).unwrap();
    space.index_parts::<String, _, _>(&mut tx, "abc", "cherry", [("x", "banana")]).unwrap();
    tx.commit().unwrap().ok();
    let read = database.read_tx();
    let found = |query: &str, shown: &str| -> Vec<String> {
        let found = space.search_visible::<String>(&read, query, |_, part| part == shown).unwrap();
        found.into_iter().map(|key| key.get()).collect()
    };
    assert_eq!(found("apple banana", "c"), vec!["ab".to_string()]);
    assert!(found("apple banana", "x").is_empty());
    assert_eq!(found("cherry banana", "x"), vec!["abc".to_string()]);
}
