//! Black-box tests for the Event Log primitive.

use stratadb::{Strata, Value};
use std::collections::HashMap;

fn db() -> Strata {
    Strata::open_temp().expect("failed to open temp db")
}

fn obj(pairs: &[(&str, Value)]) -> Value {
    let map: HashMap<String, Value> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    Value::Object(map)
}

// =============================================================================
// Append and Read
// =============================================================================

#[test]
fn append_and_read_by_sequence() {
    let db = db();
    let seq = db.event_append("tool_call", obj(&[("tool", Value::String("search".into()))])).unwrap();

    let event = db.event_read(seq).unwrap();
    assert!(event.is_some());
}

#[test]
fn append_returns_incrementing_sequences() {
    let db = db();
    let s1 = db.event_append("a", obj(&[("x", Value::Int(1))])).unwrap();
    let s2 = db.event_append("b", obj(&[("x", Value::Int(2))])).unwrap();
    let s3 = db.event_append("c", obj(&[("x", Value::Int(3))])).unwrap();
    assert!(s2 > s1);
    assert!(s3 > s2);
}

#[test]
fn read_nonexistent_sequence_returns_none() {
    let db = db();
    assert!(db.event_read(99999).unwrap().is_none());
}

// =============================================================================
// Read by type
// =============================================================================

#[test]
fn read_by_type_filters_correctly() {
    let db = db();
    db.event_append("tool_call", obj(&[("tool", Value::String("search".into()))])).unwrap();
    db.event_append("observation", obj(&[("result", Value::String("found".into()))])).unwrap();
    db.event_append("tool_call", obj(&[("tool", Value::String("write".into()))])).unwrap();

    let tool_calls = db.event_read_by_type("tool_call").unwrap();
    assert_eq!(tool_calls.len(), 2);

    let observations = db.event_read_by_type("observation").unwrap();
    assert_eq!(observations.len(), 1);
}

#[test]
fn read_by_type_nonexistent_returns_empty() {
    let db = db();
    db.event_append("a", obj(&[("x", Value::Int(1))])).unwrap();
    let events = db.event_read_by_type("nonexistent").unwrap();
    assert!(events.is_empty());
}

// =============================================================================
// Length
// =============================================================================

#[test]
fn len_empty() {
    let db = db();
    assert_eq!(db.event_len().unwrap(), 0);
}

#[test]
fn len_tracks_appends() {
    let db = db();
    db.event_append("a", obj(&[("x", Value::Int(1))])).unwrap();
    db.event_append("b", obj(&[("x", Value::Int(2))])).unwrap();
    db.event_append("c", obj(&[("x", Value::Int(3))])).unwrap();
    assert_eq!(db.event_len().unwrap(), 3);
}

// =============================================================================
// Immutability
// =============================================================================

#[test]
fn events_are_immutable() {
    let db = db();
    let seq = db.event_append("type", obj(&[("data", Value::String("original".into()))])).unwrap();

    // Append another event with same type — the original should be unchanged
    db.event_append("type", obj(&[("data", Value::String("second".into()))])).unwrap();

    let original = db.event_read(seq).unwrap().unwrap();
    // The original event should still have its original payload
    assert!(format!("{:?}", original.value).contains("original"));
}

// =============================================================================
// Many events
// =============================================================================

#[test]
fn many_events() {
    let db = db();
    for i in 0..500 {
        db.event_append("stream", obj(&[("i", Value::Int(i))])).unwrap();
    }
    assert_eq!(db.event_len().unwrap(), 500);

    let all = db.event_read_by_type("stream").unwrap();
    assert_eq!(all.len(), 500);
}
