//! Black-box tests for branch API capabilities.
//!
//! Tests `branch_get()`, `branch_exists()`, versioned `branch_list()`,
//! `branches()` power API, fork/diff, and bundle export/import.

use std::collections::HashMap;
use stratadb::{Strata, Value};

// =============================================================================
// Helpers
// =============================================================================

fn db() -> Strata {
    Strata::cache().expect("failed to open temp db")
}

fn disk_db(path: &str) -> Strata {
    Strata::open(path).expect("failed to open disk db")
}

// =============================================================================
// Section 1: branch_get()
// =============================================================================

#[test]
fn branch_get_returns_info() {
    let db = db();
    db.create_branch("test-branch").unwrap();

    let info = db.branch_get("test-branch").unwrap();
    assert!(info.is_some(), "branch_get should return Some for existing branch");

    let vbi = info.unwrap();
    assert_eq!(vbi.info.id.as_str(), "test-branch");
}

#[test]
fn branch_get_nonexistent_returns_none() {
    let db = db();
    let info = db.branch_get("no-such-branch").unwrap();
    assert!(info.is_none(), "branch_get should return None for nonexistent branch");
}

#[test]
fn branch_get_default_branch() {
    let db = db();
    let info = db.branch_get("default").unwrap();
    assert!(info.is_some(), "default branch should always be gettable");
    assert_eq!(info.unwrap().info.id.as_str(), "default");
}

#[test]
fn branch_get_has_timestamps() {
    let db = db();
    db.create_branch("ts-branch").unwrap();

    let vbi = db.branch_get("ts-branch").unwrap().unwrap();
    assert!(vbi.info.created_at > 0, "created_at should be > 0");
    assert!(vbi.info.updated_at > 0, "updated_at should be > 0");
}

#[test]
fn branch_get_has_version() {
    let db = db();
    db.create_branch("ver-branch").unwrap();

    let vbi = db.branch_get("ver-branch").unwrap().unwrap();
    assert!(vbi.version > 0, "version should be > 0");
    assert!(vbi.timestamp > 0, "timestamp should be > 0");
}

// =============================================================================
// Section 2: branch_exists()
// =============================================================================

#[test]
fn branch_exists_returns_true() {
    let db = db();
    db.create_branch("exists-branch").unwrap();
    assert!(db.branch_exists("exists-branch").unwrap());
}

#[test]
fn branch_exists_returns_false() {
    let db = db();
    assert!(!db.branch_exists("nonexistent").unwrap());
}

#[test]
fn branch_exists_default() {
    let db = db();
    assert!(db.branch_exists("default").unwrap());
}

#[test]
fn branch_exists_after_delete() {
    let db = db();
    db.create_branch("to-delete").unwrap();
    assert!(db.branch_exists("to-delete").unwrap());

    db.branch_delete("to-delete").unwrap();
    assert!(!db.branch_exists("to-delete").unwrap());
}

// =============================================================================
// Section 3: branch_list() versioned
// =============================================================================

#[test]
fn branch_list_returns_versioned_info() {
    let db = db();
    db.create_branch("list-a").unwrap();
    db.create_branch("list-b").unwrap();

    let list = db.branch_list(None, None, None).unwrap();
    assert!(list.len() >= 3, "should have default + 2 created branches");

    for vbi in &list {
        assert!(!vbi.info.id.as_str().is_empty(), "id should be non-empty");
        assert!(vbi.version > 0, "version should be > 0");
        assert!(vbi.timestamp > 0, "timestamp should be > 0");
    }
}

#[test]
fn branch_list_with_limit() {
    let db = db();
    for i in 0..5 {
        db.create_branch(&format!("limit-{}", i)).unwrap();
    }

    let limited = db.branch_list(None, Some(2), None).unwrap();
    assert_eq!(limited.len(), 2, "limit should cap results at 2");
}

#[test]
fn branch_list_includes_default() {
    let db = db();
    let list = db.branch_list(None, None, None).unwrap();
    let names: Vec<&str> = list.iter().map(|v| v.info.id.as_str()).collect();
    assert!(names.contains(&"default"), "default branch should always be in list");
}

// =============================================================================
// Section 4: branches() power API
// =============================================================================

#[test]
fn branches_api_list() {
    let db = db();
    db.branches().create("power-a").unwrap();
    db.branches().create("power-b").unwrap();

    let names = db.branches().list().unwrap();
    assert!(names.contains(&"power-a".to_string()));
    assert!(names.contains(&"power-b".to_string()));
}

#[test]
fn branches_api_exists() {
    let db = db();
    db.branches().create("check-me").unwrap();

    assert!(db.branches().exists("check-me").unwrap());
    assert!(!db.branches().exists("nope").unwrap());

    // Should match branch_exists()
    assert_eq!(
        db.branches().exists("check-me").unwrap(),
        db.branch_exists("check-me").unwrap()
    );
}

#[test]
fn branches_api_create_and_verify() {
    let db = db();
    db.branches().create("api-created").unwrap();

    // Verify via branch_get
    let info = db.branch_get("api-created").unwrap();
    assert!(info.is_some());
    assert_eq!(info.unwrap().info.id.as_str(), "api-created");
}

#[test]
fn branches_api_delete() {
    let db = db();
    db.branches().create("api-delete").unwrap();
    assert!(db.branch_exists("api-delete").unwrap());

    db.branches().delete("api-delete").unwrap();
    assert!(!db.branch_exists("api-delete").unwrap());
}

#[test]
fn branches_api_delete_default_fails() {
    let db = db();
    let result = db.branches().delete("default");
    assert!(result.is_err());
}

// =============================================================================
// Section 5: Fork and diff (now implemented)
// =============================================================================

#[test]
fn fork_branch_succeeds() {
    let db = db();
    // Fork creates a new branch from default
    let result = db.fork_branch("forked");
    assert!(result.is_ok(), "fork_branch should succeed: {:?}", result.err());
}

#[test]
fn diff_nonexistent_branches_fails() {
    let db = db();
    // Diff of non-existent branches should return an error (but not NotImplemented)
    let result = db.branches().diff("a", "b");
    assert!(result.is_err(), "diff of non-existent branches should fail");
}

// =============================================================================
// Section 6: Bundle export/import
// =============================================================================

#[test]
fn export_empty_branch() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("db");
    let db = disk_db(db_path.to_str().unwrap());

    db.create_branch("empty-export").unwrap();

    let bundle_path = dir.path().join("empty.runbundle.tar.zst");
    let result = db.branch_export("empty-export", bundle_path.to_str().unwrap()).unwrap();

    assert_eq!(result.branch_id, "empty-export");
    assert_eq!(result.entry_count, 0);
}

#[test]
fn export_branch_with_data() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("db");
    let mut db = disk_db(db_path.to_str().unwrap());

    db.create_branch("data-export").unwrap();
    db.set_branch("data-export").unwrap();
    db.kv_put("key1", "value1").unwrap();
    db.kv_put("key2", Value::Int(42)).unwrap();
    db.kv_put("key3", Value::Bool(true)).unwrap();

    let bundle_path = dir.path().join("data.runbundle.tar.zst");
    let result = db.branch_export("data-export", bundle_path.to_str().unwrap()).unwrap();

    assert_eq!(result.branch_id, "data-export");
    assert!(result.entry_count > 0, "should have entries");
    assert!(result.bundle_size > 0, "bundle should have nonzero size");
    assert!(bundle_path.exists(), "bundle file should exist on disk");
}

#[test]
fn validate_exported_bundle() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("db");
    let mut db = disk_db(db_path.to_str().unwrap());

    db.create_branch("validate-export").unwrap();
    db.set_branch("validate-export").unwrap();
    db.kv_put("vk", "vv").unwrap();

    let bundle_path = dir.path().join("validate.runbundle.tar.zst");
    let export = db.branch_export("validate-export", bundle_path.to_str().unwrap()).unwrap();

    let validate = db.branch_validate_bundle(bundle_path.to_str().unwrap()).unwrap();
    assert!(validate.checksums_valid, "checksums should be valid");
    assert_eq!(validate.entry_count, export.entry_count);
}

#[test]
fn validate_nonexistent_path_fails() {
    let db = db();
    let result = db.branch_validate_bundle("/tmp/nonexistent-bundle-12345.tar.zst");
    assert!(result.is_err());
}

#[test]
fn import_into_new_database() {
    let dir = tempfile::tempdir().unwrap();

    // DB A: create and export a branch
    let db_a_path = dir.path().join("db_a");
    let mut db_a = disk_db(db_a_path.to_str().unwrap());
    db_a.create_branch("portable").unwrap();
    db_a.set_branch("portable").unwrap();
    db_a.kv_put("hello", "world").unwrap();

    let bundle_path = dir.path().join("portable.runbundle.tar.zst");
    db_a.branch_export("portable", bundle_path.to_str().unwrap()).unwrap();

    // DB B: import the bundle
    let db_b_path = dir.path().join("db_b");
    let db_b = disk_db(db_b_path.to_str().unwrap());

    let import_result = db_b.branch_import(bundle_path.to_str().unwrap()).unwrap();
    assert_eq!(import_result.branch_id, "portable");
    assert!(import_result.transactions_applied > 0);

    // Verify the branch exists in DB B
    let branches = db_b.list_branches().unwrap();
    assert!(branches.contains(&"portable".to_string()));
}

#[test]
fn import_duplicate_branch_fails() {
    let dir = tempfile::tempdir().unwrap();

    let db_path = dir.path().join("db");
    let db = disk_db(db_path.to_str().unwrap());
    db.create_branch("dup-import").unwrap();

    let bundle_path = dir.path().join("dup.runbundle.tar.zst");
    db.branch_export("dup-import", bundle_path.to_str().unwrap()).unwrap();

    // Import into same DB — branch already exists
    let result = db.branch_import(bundle_path.to_str().unwrap());
    assert!(result.is_err(), "importing a branch that already exists should fail");
}

#[test]
fn import_kv_data_roundtrip() {
    let dir = tempfile::tempdir().unwrap();

    // DB A: populate branch with KV data
    let db_a_path = dir.path().join("db_a");
    let mut db_a = disk_db(db_a_path.to_str().unwrap());
    db_a.create_branch("kv-roundtrip").unwrap();
    db_a.set_branch("kv-roundtrip").unwrap();

    let entries: Vec<(&str, Value)> = vec![
        ("user:1", Value::String("Alice".into())),
        ("user:2", Value::String("Bob".into())),
        ("counter", Value::Int(999)),
        ("flag", Value::Bool(false)),
        ("rate", Value::Float(3.14)),
    ];

    for (k, v) in &entries {
        db_a.kv_put(*k, v.clone()).unwrap();
    }

    // Export
    let bundle_path = dir.path().join("kv.runbundle.tar.zst");
    db_a.branch_export("kv-roundtrip", bundle_path.to_str().unwrap()).unwrap();

    // DB B: import and verify
    let db_b_path = dir.path().join("db_b");
    let mut db_b = disk_db(db_b_path.to_str().unwrap());

    db_b.branch_import(bundle_path.to_str().unwrap()).unwrap();
    db_b.set_branch("kv-roundtrip").unwrap();

    for (k, v) in &entries {
        let got = db_b.kv_get(k).unwrap();
        assert_eq!(got.as_ref(), Some(v), "KV roundtrip mismatch for key '{}'", k);
    }
}

#[test]
fn import_multi_primitive_roundtrip() {
    let dir = tempfile::tempdir().unwrap();

    // DB A: populate branch with multiple primitives
    let db_a_path = dir.path().join("db_a");
    let mut db_a = disk_db(db_a_path.to_str().unwrap());
    db_a.create_branch("multi-roundtrip").unwrap();
    db_a.set_branch("multi-roundtrip").unwrap();

    // KV
    db_a.kv_put("name", "strata").unwrap();

    // State
    db_a.state_set("counter", Value::Int(42)).unwrap();

    // Event
    let payload = Value::Object(
        [("action".to_string(), Value::String("test".into()))]
            .into_iter()
            .collect::<HashMap<String, Value>>(),
    );
    db_a.event_append("audit", payload.clone()).unwrap();

    // JSON
    let doc = Value::Object(
        [
            ("title".to_string(), Value::String("doc1".into())),
            ("count".to_string(), Value::Int(10)),
        ]
        .into_iter()
        .collect::<HashMap<String, Value>>(),
    );
    db_a.json_set("doc:1", "$", doc).unwrap();

    // Export
    let bundle_path = dir.path().join("multi.runbundle.tar.zst");
    let export_result =
        db_a.branch_export("multi-roundtrip", bundle_path.to_str().unwrap()).unwrap();
    assert!(export_result.entry_count > 0);

    // DB B: import and verify each primitive
    let db_b_path = dir.path().join("db_b");
    let mut db_b = disk_db(db_b_path.to_str().unwrap());

    let import_result = db_b.branch_import(bundle_path.to_str().unwrap()).unwrap();
    assert_eq!(import_result.branch_id, "multi-roundtrip");
    assert!(import_result.keys_written > 0);

    db_b.set_branch("multi-roundtrip").unwrap();

    // Verify KV
    assert_eq!(
        db_b.kv_get("name").unwrap(),
        Some(Value::String("strata".into()))
    );

    // Verify State
    assert_eq!(
        db_b.state_get("counter").unwrap(),
        Some(Value::Int(42))
    );

    // Verify Event
    let events = db_b.event_get_by_type("audit").unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].value, payload);

    // Verify JSON
    let json_val = db_b.json_get("doc:1", "$").unwrap();
    assert!(json_val.is_some());
    let json_obj = json_val.unwrap();
    // Verify the fields are present
    if let Value::Object(map) = &json_obj {
        assert_eq!(map.get("title"), Some(&Value::String("doc1".into())));
        assert_eq!(map.get("count"), Some(&Value::Int(10)));
    } else {
        panic!("Expected Object, got {:?}", json_obj);
    }
}
