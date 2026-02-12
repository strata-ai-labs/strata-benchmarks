//! Black-box tests for the JSON Store primitive.

use stratadb::{Strata, Value};
use std::collections::HashMap;

fn db() -> Strata {
    Strata::cache().expect("failed to open temp db")
}

fn obj(pairs: &[(&str, Value)]) -> Value {
    let map: HashMap<String, Value> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    Value::Object(map)
}

// =============================================================================
// Set and Get at root
// =============================================================================

#[test]
fn set_and_get_root_document() {
    let db = db();
    let doc = obj(&[
        ("name", Value::String("Alice".into())),
        ("age", Value::Int(30)),
    ]);
    db.json_set("user:1", "$", doc.clone()).unwrap();

    let result = db.json_get("user:1", "$").unwrap();
    assert!(result.is_some());
}

#[test]
fn get_nonexistent_document() {
    let db = db();
    assert_eq!(db.json_get("ghost", "$").unwrap(), None);
}

// =============================================================================
// Path-level access
// =============================================================================

#[test]
fn set_and_get_nested_path() {
    let db = db();
    let doc = obj(&[
        ("name", Value::String("Alice".into())),
        ("age", Value::Int(30)),
    ]);
    db.json_set("user:1", "$", doc).unwrap();

    let name = db.json_get("user:1", "name").unwrap();
    assert_eq!(name, Some(Value::String("Alice".into())));

    let age = db.json_get("user:1", "age").unwrap();
    assert_eq!(age, Some(Value::Int(30)));
}

#[test]
fn update_nested_path() {
    let db = db();
    let doc = obj(&[("debug", Value::Bool(true))]);
    db.json_set("config", "$", doc).unwrap();

    // Update just the nested field
    db.json_set("config", "debug", Value::Bool(false)).unwrap();

    let val = db.json_get("config", "debug").unwrap();
    assert_eq!(val, Some(Value::Bool(false)));
}

#[test]
fn get_nonexistent_path() {
    let db = db();
    let doc = obj(&[("name", Value::String("Alice".into()))]);
    db.json_set("user:1", "$", doc).unwrap();

    let result = db.json_get("user:1", "nonexistent").unwrap();
    assert_eq!(result, None);
}

// =============================================================================
// Delete
// =============================================================================

#[test]
fn delete_entire_document() {
    let db = db();
    let doc = obj(&[("name", Value::String("Alice".into()))]);
    db.json_set("user:1", "$", doc).unwrap();

    db.json_delete("user:1", "$").unwrap();
    assert_eq!(db.json_get("user:1", "$").unwrap(), None);
}

#[test]
fn delete_nested_field() {
    let db = db();
    let doc = obj(&[
        ("name", Value::String("Alice".into())),
        ("temp", Value::String("remove-me".into())),
    ]);
    db.json_set("user:1", "$", doc).unwrap();

    db.json_delete("user:1", "temp").unwrap();

    // The field should be gone
    assert_eq!(db.json_get("user:1", "temp").unwrap(), None);
    // But the document still exists
    assert!(db.json_get("user:1", "name").unwrap().is_some());
}

// =============================================================================
// List
// =============================================================================

#[test]
fn list_documents() {
    let db = db();
    db.json_set("user:1", "$", obj(&[("n", Value::Int(1))])).unwrap();
    db.json_set("user:2", "$", obj(&[("n", Value::Int(2))])).unwrap();
    db.json_set("task:1", "$", obj(&[("n", Value::Int(3))])).unwrap();

    let (all, _cursor) = db.json_list(None, None, 100).unwrap();
    assert_eq!(all.len(), 3);

    let (users, _cursor) = db.json_list(Some("user:".into()), None, 100).unwrap();
    assert_eq!(users.len(), 2);
}

#[test]
fn list_empty() {
    let db = db();
    let (keys, _cursor) = db.json_list(None, None, 100).unwrap();
    assert!(keys.is_empty());
}

#[test]
fn list_with_limit() {
    let db = db();
    for i in 0..10 {
        db.json_set(&format!("doc:{}", i), "$", obj(&[("n", Value::Int(i))])).unwrap();
    }

    let (keys, cursor) = db.json_list(None, None, 3).unwrap();
    assert_eq!(keys.len(), 3);
    // There should be more results available
    assert!(cursor.is_some());
}

// =============================================================================
// Complex documents
// =============================================================================

#[test]
fn nested_object() {
    let db = db();
    let address = obj(&[
        ("city", Value::String("SF".into())),
        ("zip", Value::String("94102".into())),
    ]);
    let user = obj(&[
        ("name", Value::String("Alice".into())),
        ("address", address),
    ]);
    db.json_set("user:1", "$", user).unwrap();

    let city = db.json_get("user:1", "address.city").unwrap();
    assert_eq!(city, Some(Value::String("SF".into())));
}

#[test]
fn overwrite_entire_document() {
    let db = db();
    db.json_set("doc", "$", obj(&[("v", Value::Int(1))])).unwrap();
    db.json_set("doc", "$", obj(&[("v", Value::Int(2))])).unwrap();

    let val = db.json_get("doc", "v").unwrap();
    assert_eq!(val, Some(Value::Int(2)));
}

// =============================================================================
// Version History (json_getv)
// =============================================================================

#[test]
fn getv_returns_none_for_nonexistent_document() {
    let db = db();
    assert_eq!(db.json_getv("ghost").unwrap(), None);
}

#[test]
fn getv_single_version() {
    let db = db();
    let doc = obj(&[("name", Value::String("Alice".into()))]);
    db.json_set("user:1", "$", doc.clone()).unwrap();

    let history = db.json_getv("user:1").unwrap().unwrap();
    assert_eq!(history.len(), 1);
    assert!(history[0].version > 0);
}

#[test]
fn getv_multiple_versions_newest_first() {
    let db = db();
    db.json_set("doc", "$", obj(&[("v", Value::Int(1))])).unwrap();
    db.json_set("doc", "$", obj(&[("v", Value::Int(2))])).unwrap();
    db.json_set("doc", "$", obj(&[("v", Value::Int(3))])).unwrap();

    let history = db.json_getv("doc").unwrap().unwrap();
    assert_eq!(history.len(), 3);
    // Newest first — each version is the whole document
    assert!(history[0].version > history[1].version);
    assert!(history[1].version > history[2].version);
}

#[test]
fn getv_tracks_path_level_updates() {
    let db = db();
    let doc = obj(&[
        ("name", Value::String("Alice".into())),
        ("age", Value::Int(30)),
    ]);
    db.json_set("user:1", "$", doc).unwrap();
    // Update just a nested field
    db.json_set("user:1", "age", Value::Int(31)).unwrap();

    let history = db.json_getv("user:1").unwrap().unwrap();
    assert_eq!(history.len(), 2);
    assert!(history[0].version > history[1].version);
}

#[test]
fn getv_independent_documents() {
    let db = db();
    db.json_set("a", "$", obj(&[("n", Value::Int(1))])).unwrap();
    db.json_set("a", "$", obj(&[("n", Value::Int(2))])).unwrap();
    db.json_set("b", "$", obj(&[("n", Value::Int(10))])).unwrap();

    let ha = db.json_getv("a").unwrap().unwrap();
    let hb = db.json_getv("b").unwrap().unwrap();
    assert_eq!(ha.len(), 2);
    assert_eq!(hb.len(), 1);
}
