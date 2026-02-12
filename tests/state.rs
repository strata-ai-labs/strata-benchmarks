//! Black-box tests for the State Cell primitive.

use stratadb::{Strata, Value};

fn db() -> Strata {
    Strata::cache().expect("failed to open temp db")
}

// =============================================================================
// Set and Read
// =============================================================================

#[test]
fn set_and_read() {
    let db = db();
    db.state_set("cell", "value").unwrap();
    assert_eq!(db.state_get("cell").unwrap(), Some(Value::String("value".into())));
}

#[test]
fn read_nonexistent_returns_none() {
    let db = db();
    assert_eq!(db.state_get("ghost").unwrap(), None);
}

#[test]
fn set_overwrites() {
    let db = db();
    db.state_set("cell", "first").unwrap();
    db.state_set("cell", "second").unwrap();
    assert_eq!(db.state_get("cell").unwrap(), Some(Value::String("second".into())));
}

#[test]
fn set_different_types() {
    let db = db();
    db.state_set("cell", "string").unwrap();
    db.state_set("cell", 42i64).unwrap();
    assert_eq!(db.state_get("cell").unwrap(), Some(Value::Int(42)));
}

#[test]
fn set_returns_incrementing_versions() {
    let db = db();
    let v1 = db.state_set("cell", "a").unwrap();
    let v2 = db.state_set("cell", "b").unwrap();
    assert!(v2 > v1);
}

// =============================================================================
// Compare-and-Swap
// =============================================================================

#[test]
fn cas_on_uninitialized_cell() {
    let db = db();
    // CAS with expected_counter=None means "expect cell to not exist"
    let result = db.state_cas("cell", None, "first").unwrap();
    assert!(result.is_some());
    assert_eq!(db.state_get("cell").unwrap(), Some(Value::String("first".into())));
}

#[test]
fn cas_with_correct_counter_succeeds() {
    let db = db();
    let v1 = db.state_set("cell", "initial").unwrap();
    let result = db.state_cas("cell", Some(v1), "updated").unwrap();
    assert!(result.is_some());
    assert_eq!(db.state_get("cell").unwrap(), Some(Value::String("updated".into())));
}

#[test]
fn cas_with_wrong_counter_fails() {
    let db = db();
    db.state_set("cell", "initial").unwrap();
    // Use a bogus counter
    let result = db.state_cas("cell", Some(99999), "should-fail").unwrap();
    assert!(result.is_none());
    // Value unchanged
    assert_eq!(db.state_get("cell").unwrap(), Some(Value::String("initial".into())));
}

#[test]
fn cas_sequential_updates() {
    let db = db();
    let v1 = db.state_cas("counter", None, 0i64).unwrap().unwrap();
    let v2 = db.state_cas("counter", Some(v1), 1i64).unwrap().unwrap();
    let v3 = db.state_cas("counter", Some(v2), 2i64).unwrap().unwrap();
    assert!(v3 > v2);
    assert_eq!(db.state_get("counter").unwrap(), Some(Value::Int(2)));
}

// =============================================================================
// Init
// =============================================================================

#[test]
fn init_creates_new_cell() {
    let db = db();
    db.state_init("cell", "initial").unwrap();
    assert_eq!(db.state_get("cell").unwrap(), Some(Value::String("initial".into())));
}

#[test]
fn init_is_idempotent() {
    let db = db();
    let v1 = db.state_init("cell", "first").unwrap();
    // Second init should succeed but not overwrite
    let v2 = db.state_init("cell", "should-not-overwrite").unwrap();
    assert_eq!(v1, v2, "Idempotent init should return same version");
    // Original value preserved
    assert_eq!(db.state_get("cell").unwrap(), Some(Value::String("first".into())));
}

// =============================================================================
// Multiple cells
// =============================================================================

#[test]
fn independent_cells() {
    let db = db();
    db.state_set("a", 1i64).unwrap();
    db.state_set("b", 2i64).unwrap();
    db.state_set("c", 3i64).unwrap();

    assert_eq!(db.state_get("a").unwrap(), Some(Value::Int(1)));
    assert_eq!(db.state_get("b").unwrap(), Some(Value::Int(2)));
    assert_eq!(db.state_get("c").unwrap(), Some(Value::Int(3)));
}

// =============================================================================
// Version History (state_readv)
// =============================================================================

#[test]
fn readv_returns_none_for_nonexistent_cell() {
    let db = db();
    assert_eq!(db.state_getv("ghost").unwrap(), None);
}

#[test]
fn readv_single_version() {
    let db = db();
    db.state_set("cell", 1i64).unwrap();

    let history = db.state_getv("cell").unwrap().unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].value, Value::Int(1));
    assert!(history[0].version > 0);
}

#[test]
fn readv_multiple_versions_newest_first() {
    let db = db();
    db.state_set("cell", 1i64).unwrap();
    db.state_set("cell", 2i64).unwrap();
    db.state_set("cell", 3i64).unwrap();

    let history = db.state_getv("cell").unwrap().unwrap();
    assert_eq!(history.len(), 3);
    assert_eq!(history[0].value, Value::Int(3));
    assert_eq!(history[1].value, Value::Int(2));
    assert_eq!(history[2].value, Value::Int(1));
}

#[test]
fn readv_versions_from_cas_updates() {
    let db = db();
    let v1 = db.state_set("cell", "initial").unwrap();
    let v2 = db.state_cas("cell", Some(v1), "updated").unwrap().unwrap();
    db.state_cas("cell", Some(v2), "final").unwrap();

    let history = db.state_getv("cell").unwrap().unwrap();
    assert_eq!(history.len(), 3);
    assert_eq!(history[0].value, Value::String("final".into()));
    assert_eq!(history[1].value, Value::String("updated".into()));
    assert_eq!(history[2].value, Value::String("initial".into()));
}

#[test]
fn readv_versions_have_decreasing_version_numbers() {
    let db = db();
    db.state_set("cell", "a").unwrap();
    db.state_set("cell", "b").unwrap();
    db.state_set("cell", "c").unwrap();

    let history = db.state_getv("cell").unwrap().unwrap();
    assert!(history[0].version > history[1].version);
    assert!(history[1].version > history[2].version);
}

#[test]
fn readv_independent_cells() {
    let db = db();
    db.state_set("x", 1i64).unwrap();
    db.state_set("x", 2i64).unwrap();
    db.state_set("y", 10i64).unwrap();

    let hx = db.state_getv("x").unwrap().unwrap();
    let hy = db.state_getv("y").unwrap().unwrap();
    assert_eq!(hx.len(), 2);
    assert_eq!(hy.len(), 1);
}
