//! Dataset-driven Event tests.
//!
//! Loads `data/events.json` and verifies append, read-back, type filtering,
//! and count semantics.

mod common;

use common::{load_event_dataset, json_to_value, fresh_db};

#[test]
fn append_all_events() {
    let ds = load_event_dataset();
    let db = fresh_db();

    for event in &ds.events {
        let payload = json_to_value(&event.payload);
        db.event_append(&event.event_type, payload).unwrap();
    }

    let len = db.event_len().unwrap();
    assert_eq!(len, ds.total as u64, "total event count mismatch");
}

#[test]
fn read_back_each_event() {
    let ds = load_event_dataset();
    let db = fresh_db();

    let mut seqs = Vec::new();
    for event in &ds.events {
        let seq = db.event_append(&event.event_type, json_to_value(&event.payload)).unwrap();
        seqs.push(seq);
    }

    // Read back each event by its sequence number — VersionedValue has
    // value/version/timestamp but not event_type, so we verify the payload.
    for (i, seq) in seqs.iter().enumerate() {
        let entry = db.event_get(*seq).unwrap();
        assert!(entry.is_some(), "event at seq {} should exist", seq);
        let entry = entry.unwrap();
        let expected = json_to_value(&ds.events[i].payload);
        assert_eq!(
            entry.value, expected,
            "payload mismatch at event index {}",
            i
        );
    }
}

#[test]
fn filter_by_type_counts() {
    let ds = load_event_dataset();
    let db = fresh_db();

    for event in &ds.events {
        db.event_append(&event.event_type, json_to_value(&event.payload)).unwrap();
    }

    for (event_type, expected_count) in &ds.expected_counts {
        let filtered = db.event_get_by_type(event_type).unwrap();
        assert_eq!(
            filtered.len(),
            *expected_count,
            "type '{}' expected {} events, got {}",
            event_type,
            expected_count,
            filtered.len()
        );
    }
}

#[test]
fn nonexistent_type_returns_empty() {
    let ds = load_event_dataset();
    let db = fresh_db();

    for event in &ds.events {
        db.event_append(&event.event_type, json_to_value(&event.payload)).unwrap();
    }

    let empty = db.event_get_by_type("nonexistent_type").unwrap();
    assert_eq!(empty.len(), 0);
}

#[test]
fn event_payloads_match() {
    let ds = load_event_dataset();
    let db = fresh_db();

    let mut seqs = Vec::new();
    for event in &ds.events {
        let seq = db.event_append(&event.event_type, json_to_value(&event.payload)).unwrap();
        seqs.push(seq);
    }

    for (i, seq) in seqs.iter().enumerate() {
        let event = db.event_get(*seq).unwrap().unwrap();
        let expected = json_to_value(&ds.events[i].payload);
        assert_eq!(
            event.value, expected,
            "payload mismatch at event index {}",
            i
        );
    }
}

#[test]
fn event_sequences_are_incrementing() {
    let ds = load_event_dataset();
    let db = fresh_db();

    let mut seqs = Vec::new();
    for event in &ds.events {
        let seq = db.event_append(&event.event_type, json_to_value(&event.payload)).unwrap();
        seqs.push(seq);
    }

    for i in 1..seqs.len() {
        assert!(
            seqs[i] > seqs[i - 1],
            "sequences should be strictly increasing: {} <= {}",
            seqs[i],
            seqs[i - 1]
        );
    }
}
