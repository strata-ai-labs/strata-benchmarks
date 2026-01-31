//! Dataset-driven KV tests.
//!
//! Loads `data/kv.json` and verifies insert, read-back, prefix listing,
//! deletion, and overwrite semantics against a fresh StrataDB instance.

mod common;

use common::{load_kv_dataset, fresh_db};

#[test]
fn insert_and_readback_all_entries() {
    let ds = load_kv_dataset();
    let db = fresh_db();

    for entry in &ds.entries {
        db.kv_put(&entry.key, entry.value.to_value()).unwrap();
    }

    for entry in &ds.entries {
        let got = db.kv_get(&entry.key).unwrap();
        assert!(got.is_some(), "missing key: {}", entry.key);
        assert_eq!(got.unwrap(), entry.value.to_value(), "mismatch for key: {}", entry.key);
    }
}

#[test]
fn prefix_listing_counts() {
    let ds = load_kv_dataset();
    let db = fresh_db();

    for entry in &ds.entries {
        db.kv_put(&entry.key, entry.value.to_value()).unwrap();
    }

    for (prefix, expected_count) in &ds.prefixes {
        let results = db.kv_list(Some(prefix)).unwrap();
        assert_eq!(
            results.len(),
            *expected_count,
            "prefix '{}' expected {} entries, got {}",
            prefix,
            expected_count,
            results.len()
        );
    }
}

#[test]
fn delete_entries() {
    let ds = load_kv_dataset();
    let db = fresh_db();

    for entry in &ds.entries {
        db.kv_put(&entry.key, entry.value.to_value()).unwrap();
    }

    for key in &ds.deletions {
        db.kv_delete(key).unwrap();
        let got = db.kv_get(key).unwrap();
        assert!(got.is_none(), "key '{}' should be deleted", key);
    }

    // Non-deleted keys should still exist
    let remaining: Vec<_> = ds.entries.iter()
        .filter(|e| !ds.deletions.contains(&e.key))
        .collect();
    for entry in &remaining {
        let got = db.kv_get(&entry.key).unwrap();
        assert!(got.is_some(), "key '{}' should still exist", entry.key);
    }
}

#[test]
fn overwrite_sequence() {
    let ds = load_kv_dataset();
    let db = fresh_db();

    for entry in &ds.entries {
        db.kv_put(&entry.key, entry.value.to_value()).unwrap();
    }

    for ow in &ds.overwrites {
        db.kv_put(&ow.key, ow.value.to_value()).unwrap();
    }

    // counter:page_views -> 3, config:debug_mode -> true
    let page_views = db.kv_get("counter:page_views").unwrap().unwrap();
    assert_eq!(page_views, stratadb::Value::Int(3));

    let debug = db.kv_get("config:debug_mode").unwrap().unwrap();
    assert_eq!(debug, stratadb::Value::Bool(true));
}

#[test]
fn delete_then_reinsert() {
    let ds = load_kv_dataset();
    let db = fresh_db();

    for entry in &ds.entries {
        db.kv_put(&entry.key, entry.value.to_value()).unwrap();
    }

    let key = &ds.deletions[0];
    db.kv_delete(key).unwrap();
    assert!(db.kv_get(key).unwrap().is_none());

    db.kv_put(key, stratadb::Value::String("reinserted".into())).unwrap();
    let got = db.kv_get(key).unwrap().unwrap();
    assert_eq!(got, stratadb::Value::String("reinserted".into()));
}

#[test]
fn prefix_listing_after_deletes() {
    let ds = load_kv_dataset();
    let db = fresh_db();

    for entry in &ds.entries {
        db.kv_put(&entry.key, entry.value.to_value()).unwrap();
    }

    for key in &ds.deletions {
        db.kv_delete(key).unwrap();
    }

    // cache:session: had 2 entries, deleted 1 -> 1 remaining
    let cache_results = db.kv_list(Some("cache:session:")).unwrap();
    assert_eq!(cache_results.len(), 1);

    // queue:pending: had 3 entries, deleted 1 -> 2 remaining
    let queue_results = db.kv_list(Some("queue:pending:")).unwrap();
    assert_eq!(queue_results.len(), 2);

    // tag: had 2 entries, deleted 1 -> 1 remaining
    let tag_results = db.kv_list(Some("tag:")).unwrap();
    assert_eq!(tag_results.len(), 1);
}

#[test]
fn total_entry_count() {
    let ds = load_kv_dataset();
    let db = fresh_db();

    for entry in &ds.entries {
        db.kv_put(&entry.key, entry.value.to_value()).unwrap();
    }

    let all = db.kv_list(None).unwrap();
    assert_eq!(all.len(), ds.entries.len());
}
