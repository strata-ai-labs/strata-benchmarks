//! Black-box tests for the KV primitive.
//!
//! Tests exercise only the public `stratadb::Strata` API.

use stratadb::{Strata, Value};

fn db() -> Strata {
    Strata::open_temp().expect("failed to open temp db")
}

// =============================================================================
// Basic CRUD
// =============================================================================

#[test]
fn put_and_get_string() {
    let db = db();
    let version = db.kv_put("key", "hello").unwrap();
    assert!(version > 0);

    let val = db.kv_get("key").unwrap();
    assert_eq!(val, Some(Value::String("hello".into())));
}

#[test]
fn put_and_get_int() {
    let db = db();
    db.kv_put("num", 42i64).unwrap();
    assert_eq!(db.kv_get("num").unwrap(), Some(Value::Int(42)));
}

#[test]
fn put_and_get_float() {
    let db = db();
    db.kv_put("pi", 3.14f64).unwrap();
    match db.kv_get("pi").unwrap() {
        Some(Value::Float(f)) => assert!((f - 3.14).abs() < f64::EPSILON),
        other => panic!("expected Float, got {:?}", other),
    }
}

#[test]
fn put_and_get_bool() {
    let db = db();
    db.kv_put("flag", true).unwrap();
    assert_eq!(db.kv_get("flag").unwrap(), Some(Value::Bool(true)));
}

#[test]
fn put_and_get_bytes() {
    let db = db();
    let data: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF];
    db.kv_put("bin", Value::Bytes(data.clone())).unwrap();
    assert_eq!(db.kv_get("bin").unwrap(), Some(Value::Bytes(data)));
}

#[test]
fn get_nonexistent_returns_none() {
    let db = db();
    assert_eq!(db.kv_get("ghost").unwrap(), None);
}

#[test]
fn put_overwrites_previous_value() {
    let db = db();
    db.kv_put("key", "first").unwrap();
    db.kv_put("key", "second").unwrap();
    assert_eq!(db.kv_get("key").unwrap(), Some(Value::String("second".into())));
}

#[test]
fn put_overwrites_with_different_type() {
    let db = db();
    db.kv_put("key", "string").unwrap();
    db.kv_put("key", 99i64).unwrap();
    assert_eq!(db.kv_get("key").unwrap(), Some(Value::Int(99)));
}

#[test]
fn version_increments_on_each_put() {
    let db = db();
    let v1 = db.kv_put("key", "a").unwrap();
    let v2 = db.kv_put("key", "b").unwrap();
    let v3 = db.kv_put("key", "c").unwrap();
    assert!(v2 > v1);
    assert!(v3 > v2);
}

// =============================================================================
// Delete
// =============================================================================

#[test]
fn delete_existing_key_returns_true() {
    let db = db();
    db.kv_put("key", "val").unwrap();
    assert!(db.kv_delete("key").unwrap());
}

#[test]
fn delete_nonexistent_key_returns_false() {
    let db = db();
    assert!(!db.kv_delete("ghost").unwrap());
}

#[test]
fn get_after_delete_returns_none() {
    let db = db();
    db.kv_put("key", "val").unwrap();
    db.kv_delete("key").unwrap();
    assert_eq!(db.kv_get("key").unwrap(), None);
}

#[test]
fn put_after_delete_creates_new_entry() {
    let db = db();
    db.kv_put("key", "first").unwrap();
    db.kv_delete("key").unwrap();
    db.kv_put("key", "second").unwrap();
    assert_eq!(db.kv_get("key").unwrap(), Some(Value::String("second".into())));
}

// =============================================================================
// List
// =============================================================================

#[test]
fn list_all_keys() {
    let db = db();
    db.kv_put("a", 1i64).unwrap();
    db.kv_put("b", 2i64).unwrap();
    db.kv_put("c", 3i64).unwrap();

    let mut keys = db.kv_list(None).unwrap();
    keys.sort();
    assert_eq!(keys, vec!["a", "b", "c"]);
}

#[test]
fn list_with_prefix() {
    let db = db();
    db.kv_put("user:1", 1i64).unwrap();
    db.kv_put("user:2", 2i64).unwrap();
    db.kv_put("task:1", 3i64).unwrap();

    let mut keys = db.kv_list(Some("user:")).unwrap();
    keys.sort();
    assert_eq!(keys, vec!["user:1", "user:2"]);
}

#[test]
fn list_with_prefix_no_matches() {
    let db = db();
    db.kv_put("key", "val").unwrap();
    let keys = db.kv_list(Some("zzz:")).unwrap();
    assert!(keys.is_empty());
}

#[test]
fn list_empty_db() {
    let db = db();
    let keys = db.kv_list(None).unwrap();
    assert!(keys.is_empty());
}

#[test]
fn list_excludes_deleted_keys() {
    let db = db();
    db.kv_put("a", 1i64).unwrap();
    db.kv_put("b", 2i64).unwrap();
    db.kv_delete("a").unwrap();

    let keys = db.kv_list(None).unwrap();
    assert_eq!(keys, vec!["b"]);
}

// =============================================================================
// Edge cases
// =============================================================================

#[test]
fn empty_string_key_is_rejected() {
    let db = db();
    assert!(db.kv_put("", "empty key").is_err());
}

#[test]
fn empty_string_value() {
    let db = db();
    db.kv_put("key", "").unwrap();
    assert_eq!(db.kv_get("key").unwrap(), Some(Value::String("".into())));
}

#[test]
fn unicode_key_and_value() {
    let db = db();
    db.kv_put("日本語", "こんにちは").unwrap();
    assert_eq!(
        db.kv_get("日本語").unwrap(),
        Some(Value::String("こんにちは".into()))
    );
}

#[test]
fn large_value() {
    let db = db();
    let big = "x".repeat(1_000_000);
    db.kv_put("big", big.as_str()).unwrap();
    assert_eq!(db.kv_get("big").unwrap(), Some(Value::String(big)));
}

#[test]
fn many_keys() {
    let db = db();
    for i in 0..1000 {
        db.kv_put(&format!("key:{:04}", i), i as i64).unwrap();
    }
    let keys = db.kv_list(None).unwrap();
    assert_eq!(keys.len(), 1000);
}
