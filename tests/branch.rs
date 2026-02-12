//! Black-box tests for branch management and data isolation.

use stratadb::{Strata, Value};

fn db() -> Strata {
    Strata::cache().expect("failed to open temp db")
}

// =============================================================================
// Branch lifecycle
// =============================================================================

#[test]
fn starts_on_default_branch() {
    let db = db();
    assert_eq!(db.current_branch(), "default");
}

#[test]
fn create_and_switch_branch() {
    let mut db = db();
    db.create_branch("test").unwrap();
    db.set_branch("test").unwrap();
    assert_eq!(db.current_branch(), "test");
}

#[test]
fn switch_back_to_default() {
    let mut db = db();
    db.create_branch("test").unwrap();
    db.set_branch("test").unwrap();
    db.set_branch("default").unwrap();
    assert_eq!(db.current_branch(), "default");
}

#[test]
fn switch_to_nonexistent_branch_fails() {
    let mut db = db();
    assert!(db.set_branch("ghost").is_err());
}

#[test]
fn create_duplicate_branch_fails() {
    let db = db();
    db.create_branch("dup").unwrap();
    assert!(db.create_branch("dup").is_err());
}

#[test]
fn list_branches() {
    let db = db();
    db.create_branch("a").unwrap();
    db.create_branch("b").unwrap();
    db.create_branch("c").unwrap();

    let branches = db.list_branches().unwrap();
    assert!(branches.contains(&"a".to_string()));
    assert!(branches.contains(&"b".to_string()));
    assert!(branches.contains(&"c".to_string()));
    assert!(branches.contains(&"default".to_string()));
}

#[test]
fn delete_branch() {
    let db = db();
    db.create_branch("temp").unwrap();
    db.delete_branch("temp").unwrap();

    let branches = db.list_branches().unwrap();
    assert!(!branches.contains(&"temp".to_string()));
}

#[test]
fn delete_current_branch_fails() {
    let mut db = db();
    db.create_branch("current").unwrap();
    db.set_branch("current").unwrap();
    assert!(db.delete_branch("current").is_err());
}

#[test]
fn delete_default_branch_fails() {
    let db = db();
    assert!(db.delete_branch("default").is_err());
}

// =============================================================================
// KV isolation
// =============================================================================

#[test]
fn kv_data_isolated_between_branches() {
    let mut db = db();

    db.kv_put("key", "default-value").unwrap();

    db.create_branch("other").unwrap();
    db.set_branch("other").unwrap();

    // Key should not exist in new branch
    assert_eq!(db.kv_get("key").unwrap(), None);

    // Write different value
    db.kv_put("key", "other-value").unwrap();

    // Switch back — original value intact
    db.set_branch("default").unwrap();
    assert_eq!(db.kv_get("key").unwrap(), Some(Value::String("default-value".into())));
}

#[test]
fn kv_delete_isolated() {
    let mut db = db();

    db.kv_put("key", "value").unwrap();

    db.create_branch("other").unwrap();
    db.set_branch("other").unwrap();
    db.kv_put("key", "other-value").unwrap();
    db.kv_delete("key").unwrap();

    // Default branch key unaffected
    db.set_branch("default").unwrap();
    assert_eq!(db.kv_get("key").unwrap(), Some(Value::String("value".into())));
}

#[test]
fn kv_list_isolated() {
    let mut db = db();

    db.kv_put("default-key", 1i64).unwrap();

    db.create_branch("other").unwrap();
    db.set_branch("other").unwrap();
    db.kv_put("other-key", 2i64).unwrap();

    let other_keys = db.kv_list(None).unwrap();
    assert_eq!(other_keys, vec!["other-key"]);

    db.set_branch("default").unwrap();
    let default_keys = db.kv_list(None).unwrap();
    assert_eq!(default_keys, vec!["default-key"]);
}

// =============================================================================
// State isolation
// =============================================================================

#[test]
fn state_isolated_between_branches() {
    let mut db = db();

    db.state_set("cell", "default").unwrap();

    db.create_branch("other").unwrap();
    db.set_branch("other").unwrap();

    assert_eq!(db.state_get("cell").unwrap(), None);

    db.state_set("cell", "other").unwrap();

    db.set_branch("default").unwrap();
    assert_eq!(db.state_get("cell").unwrap(), Some(Value::String("default".into())));
}

// =============================================================================
// Event isolation
// =============================================================================

#[test]
fn events_isolated_between_branches() {
    let mut db = db();

    db.event_append("stream", Value::Object(
        [("source".to_string(), Value::String("default".into()))].into_iter().collect()
    )).unwrap();
    assert_eq!(db.event_len().unwrap(), 1);

    db.create_branch("other").unwrap();
    db.set_branch("other").unwrap();

    // New branch has no events
    assert_eq!(db.event_len().unwrap(), 0);

    db.event_append("stream", Value::Object(
        [("source".to_string(), Value::String("other".into()))].into_iter().collect()
    )).unwrap();

    // Default branch still has 1
    db.set_branch("default").unwrap();
    assert_eq!(db.event_len().unwrap(), 1);
}

// =============================================================================
// Vector isolation
// =============================================================================

#[test]
fn vector_collections_isolated_between_branches() {
    let mut db = db();

    db.vector_create_collection("vecs", 3, stratadb::DistanceMetric::Cosine).unwrap();
    db.vector_upsert("vecs", "v1", vec![1.0, 0.0, 0.0], None).unwrap();

    db.create_branch("other").unwrap();
    db.set_branch("other").unwrap();

    // Collection doesn't exist in new branch
    let collections = db.vector_list_collections().unwrap();
    assert!(collections.is_empty());
}

// =============================================================================
// JSON isolation
// =============================================================================

#[test]
fn json_isolated_between_branches() {
    let mut db = db();

    db.json_set("doc", "$", Value::Object(
        [("branch".to_string(), Value::String("default".into()))].into_iter().collect()
    )).unwrap();

    db.create_branch("other").unwrap();
    db.set_branch("other").unwrap();

    assert_eq!(db.json_get("doc", "$").unwrap(), None);
}

// =============================================================================
// Delete branch cleans up data
// =============================================================================

#[test]
fn deleted_branch_data_is_gone() {
    let mut db = db();

    db.create_branch("temp").unwrap();
    db.set_branch("temp").unwrap();
    db.kv_put("key", "value").unwrap();

    db.set_branch("default").unwrap();
    db.delete_branch("temp").unwrap();

    // Recreate same branch — should be empty
    db.create_branch("temp").unwrap();
    db.set_branch("temp").unwrap();
    assert_eq!(db.kv_get("key").unwrap(), None);
}

// =============================================================================
// Many branches
// =============================================================================

#[test]
fn many_branches_with_isolated_data() {
    let mut db = db();

    for i in 0..20 {
        let name = format!("branch-{}", i);
        db.create_branch(&name).unwrap();
        db.set_branch(&name).unwrap();
        db.kv_put("id", i as i64).unwrap();
    }

    // Verify each branch has its own value
    for i in 0..20 {
        let name = format!("branch-{}", i);
        db.set_branch(&name).unwrap();
        assert_eq!(db.kv_get("id").unwrap(), Some(Value::Int(i as i64)));
    }
}
