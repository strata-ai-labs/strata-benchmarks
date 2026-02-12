//! Adversarial / dirty-data tests.
//!
//! Loads `data/dirty.jsonl` and throws hostile inputs at every primitive.
//! The contract is simple:
//!   - If the operation succeeds, the data MUST round-trip exactly.
//!   - If the operation fails, it MUST fail with an error — never a panic.
//!
//! Any panic or data corruption is a bug.

mod common;

use common::{fresh_db, json_to_value, load_dirty_dataset, value_to_json};
use std::panic;
use stratadb::DistanceMetric;

// =============================================================================
// KV: dirty roundtrips
// =============================================================================

#[test]
fn dirty_kv_roundtrips() {
    let ds = load_dirty_dataset();
    let db = fresh_db();

    for entry in &ds.kv_roundtrips {
        let val = entry.value.to_value();
        let desc = &entry.desc;

        // Catch panics — a panic on dirty data is always a bug
        let put_result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            db.kv_put(&entry.key, val.clone())
        }));

        match put_result {
            Err(panic_info) => {
                panic!("[PANIC] kv_put panicked on dirty input '{}': {:?}", desc, panic_info);
            }
            Ok(Err(_)) => {
                // Clean rejection — acceptable
                continue;
            }
            Ok(Ok(_)) => {
                // Write succeeded — read back must match
                let get_result = db.kv_get(&entry.key).unwrap_or_else(|e| {
                    panic!("[BUG] kv_get failed after successful put for '{}': {}", desc, e);
                });
                match get_result {
                    None if val == stratadb::Value::Null => {
                        // Acceptable: storing Null is treated as deletion
                    }
                    None => {
                        panic!("[BUG] kv_get returned None after successful put for '{}'", desc);
                    }
                    Some(got) => {
                        assert_eq!(
                            got, val,
                            "[BUG] round-trip mismatch for '{}': expected {:?}, got {:?}",
                            desc, val, got
                        );
                    }
                }
            }
        }
    }
}

// =============================================================================
// KV: expected rejections
// =============================================================================

#[test]
fn dirty_kv_rejects() {
    let ds = load_dirty_dataset();
    let db = fresh_db();

    for entry in &ds.kv_rejects {
        let val = entry.value.to_value();
        let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            db.kv_put(&entry.key, val)
        }));

        match result {
            Err(panic_info) => {
                panic!("[PANIC] kv_put panicked on reject input '{}': {:?}", entry.desc, panic_info);
            }
            Ok(Err(_)) => { /* expected rejection */ }
            Ok(Ok(_)) => {
                panic!(
                    "[BUG] kv_put should have rejected '{}' but succeeded",
                    entry.desc
                );
            }
        }
    }
}

// =============================================================================
// State: dirty roundtrips
// =============================================================================

#[test]
fn dirty_state_roundtrips() {
    let ds = load_dirty_dataset();
    let db = fresh_db();

    for entry in &ds.state_roundtrips {
        let val = entry.value.to_value();
        let desc = &entry.desc;

        let set_result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            db.state_set(&entry.cell, val.clone())
        }));

        match set_result {
            Err(panic_info) => {
                panic!("[PANIC] state_set panicked on '{}': {:?}", desc, panic_info);
            }
            Ok(Err(_)) => continue,
            Ok(Ok(_)) => {
                let got = db.state_get(&entry.cell).unwrap_or_else(|e| {
                    panic!("[BUG] state_read failed after set for '{}': {}", desc, e);
                });
                assert_eq!(
                    got,
                    Some(val.clone()),
                    "[BUG] state round-trip mismatch for '{}'",
                    desc
                );
            }
        }
    }
}

// =============================================================================
// Event: dirty roundtrips
// =============================================================================

#[test]
fn dirty_event_roundtrips() {
    let ds = load_dirty_dataset();
    let db = fresh_db();

    for entry in &ds.event_roundtrips {
        let payload = json_to_value(&entry.payload);
        let desc = &entry.desc;

        let append_result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            db.event_append(&entry.event_type, payload.clone())
        }));

        match append_result {
            Err(panic_info) => {
                panic!("[PANIC] event_append panicked on '{}': {:?}", desc, panic_info);
            }
            Ok(Err(_)) => continue,
            Ok(Ok(seq)) => {
                let got = db.event_get(seq).unwrap_or_else(|e| {
                    panic!("[BUG] event_read failed after append for '{}': {}", desc, e);
                });
                let got = got.unwrap_or_else(|| {
                    panic!("[BUG] event_read returned None after append for '{}'", desc);
                });
                assert_eq!(
                    got.value, payload,
                    "[BUG] event payload round-trip mismatch for '{}'",
                    desc
                );
            }
        }
    }
}

// =============================================================================
// JSON: dirty roundtrips
// =============================================================================

#[test]
fn dirty_json_roundtrips() {
    let ds = load_dirty_dataset();
    let db = fresh_db();

    for entry in &ds.json_roundtrips {
        let val = json_to_value(&entry.doc);
        let desc = &entry.desc;

        let set_result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            db.json_set(&entry.key, "$", val.clone())
        }));

        match set_result {
            Err(panic_info) => {
                panic!("[PANIC] json_set panicked on '{}': {:?}", desc, panic_info);
            }
            Ok(Err(_)) => continue,
            Ok(Ok(_)) => {
                let got = db.json_get(&entry.key, "$").unwrap_or_else(|e| {
                    panic!("[BUG] json_get failed after set for '{}': {}", desc, e);
                });
                let got = got.unwrap_or_else(|| {
                    panic!("[BUG] json_get returned None after set for '{}'", desc);
                });
                let got_json = value_to_json(&got);
                assert_eq!(
                    got_json, entry.doc,
                    "[BUG] json round-trip mismatch for '{}'",
                    desc
                );
            }
        }
    }
}

// =============================================================================
// Vector: dirty roundtrips
// =============================================================================

#[test]
fn dirty_vector_roundtrips() {
    let ds = load_dirty_dataset();
    let db = fresh_db();

    // Create the dirty_vecs collection once
    db.vector_create_collection("dirty_vecs", 4, DistanceMetric::Cosine)
        .expect("failed to create dirty_vecs collection");

    for entry in &ds.vector_roundtrips {
        let desc = &entry.desc;

        let upsert_result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            db.vector_upsert(&entry.collection, &entry.key, entry.embedding.clone(), None)
        }));

        match upsert_result {
            Err(panic_info) => {
                panic!("[PANIC] vector_upsert panicked on '{}': {:?}", desc, panic_info);
            }
            Ok(Err(_)) => continue,
            Ok(Ok(_)) => {
                let got = db.vector_get(&entry.collection, &entry.key).unwrap_or_else(|e| {
                    panic!("[BUG] vector_get failed after upsert for '{}': {}", desc, e);
                });
                let got = got.unwrap_or_else(|| {
                    panic!("[BUG] vector_get returned None after upsert for '{}'", desc);
                });
                assert_eq!(
                    got.data.embedding, entry.embedding,
                    "[BUG] vector embedding round-trip mismatch for '{}'",
                    desc
                );
            }
        }
    }
}

// =============================================================================
// Cross-primitive: same dirty key in KV and JSON
// =============================================================================

#[test]
fn dirty_cross_kv_json() {
    let ds = load_dirty_dataset();
    let db = fresh_db();

    for entry in &ds.cross_kv_json {
        let kv_val = entry.kv_value.to_value();
        let json_val = json_to_value(&entry.json_doc);
        let desc = &entry.desc;

        // Write to KV
        let kv_ok = match db.kv_put(&entry.key, kv_val.clone()) {
            Ok(_) => true,
            Err(_) => false,
        };

        // Write to JSON store with same key
        let json_ok = match db.json_set(&entry.key, "$", json_val.clone()) {
            Ok(_) => true,
            Err(_) => false,
        };

        // If both succeeded, verify they don't interfere
        if kv_ok {
            let got = db.kv_get(&entry.key).unwrap();
            assert_eq!(
                got,
                Some(kv_val),
                "[BUG] cross-primitive KV corrupted for '{}'",
                desc
            );
        }
        if json_ok {
            let got = db.json_get(&entry.key, "$").unwrap();
            assert!(got.is_some(), "[BUG] cross-primitive JSON missing for '{}'", desc);
            let got_json = value_to_json(&got.unwrap());
            assert_eq!(
                got_json, entry.json_doc,
                "[BUG] cross-primitive JSON corrupted for '{}'",
                desc
            );
        }
    }
}

// =============================================================================
// Cross-branch: dirty data in branch isolation
// =============================================================================

#[test]
fn dirty_cross_branch_isolation() {
    let ds = load_dirty_dataset();
    let mut db = fresh_db();

    for entry in &ds.cross_branch_dirty {
        let desc = &entry.desc;

        // Create branch
        if db.create_branch(&entry.branch).is_err() {
            continue; // branch name rejected — acceptable
        }
        db.set_branch(&entry.branch).unwrap();

        // Write dirty KV
        let kv_val = entry.kv_value.to_value();
        let kv_ok = db.kv_put(&entry.key, kv_val.clone()).is_ok();

        // Write dirty state
        let state_val = entry.state_value.to_value();
        let state_ok = db.state_set(&entry.cell, state_val.clone()).is_ok();

        // Write dirty event
        let event_payload = json_to_value(&entry.event_payload);
        let event_ok = db.event_append(&entry.event_type, event_payload.clone()).is_ok();

        // Switch to default — dirty data should NOT leak
        db.set_branch("default").unwrap();

        if kv_ok {
            let got = db.kv_get(&entry.key).unwrap();
            assert!(
                got.is_none(),
                "[BUG] dirty KV leaked from branch '{}' to default for '{}'",
                entry.branch, desc
            );
        }

        if state_ok {
            let got = db.state_get(&entry.cell).unwrap();
            assert_eq!(
                got, None,
                "[BUG] dirty state leaked from branch '{}' to default for '{}'",
                entry.branch, desc
            );
        }

        if event_ok {
            let len = db.event_len().unwrap();
            assert_eq!(
                len, 0,
                "[BUG] dirty events leaked from branch '{}' to default for '{}'",
                entry.branch, desc
            );
        }

        // Switch back — dirty data should still be there
        db.set_branch(&entry.branch).unwrap();

        if kv_ok {
            let got = db.kv_get(&entry.key).unwrap().unwrap();
            assert_eq!(
                got, kv_val,
                "[BUG] dirty KV lost on branch for '{}'",
                desc
            );
        }

        if state_ok {
            let got = db.state_get(&entry.cell).unwrap().unwrap();
            assert_eq!(
                got, state_val,
                "[BUG] dirty state lost on branch for '{}'",
                desc
            );
        }

        // Switch back to default for next iteration
        db.set_branch("default").unwrap();
    }
}

// =============================================================================
// Programmatic stress tests (not from dataset)
// =============================================================================

#[test]
fn kv_very_long_key() {
    let db = fresh_db();
    let key = "k".repeat(100_000);
    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        db.kv_put(&key, "long-key-value")
    }));
    match result {
        Err(p) => panic!("[PANIC] kv_put panicked on 100K key: {:?}", p),
        Ok(Err(_)) => { /* rejection is fine */ }
        Ok(Ok(_)) => {
            let got = db.kv_get(&key).unwrap();
            assert!(got.is_some(), "100K key should be retrievable after successful put");
        }
    }
}

#[test]
fn kv_very_long_value() {
    let db = fresh_db();
    let val = "v".repeat(10_000_000); // 10MB string
    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        db.kv_put("big-value", val.as_str())
    }));
    match result {
        Err(p) => panic!("[PANIC] kv_put panicked on 10MB value: {:?}", p),
        Ok(Err(_)) => { /* rejection is fine */ }
        Ok(Ok(_)) => {
            let got = db.kv_get("big-value").unwrap().unwrap();
            assert_eq!(got, stratadb::Value::String(val));
        }
    }
}

#[test]
fn json_deeply_nested_100_levels() {
    let db = fresh_db();
    // Build 100-level nested JSON programmatically
    let mut val = serde_json::json!("bottom");
    for i in (0..100).rev() {
        val = serde_json::json!({ format!("l{}", i): val });
    }

    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        db.json_set("deep-100", "$", json_to_value(&val))
    }));
    match result {
        Err(p) => panic!("[PANIC] json_set panicked on 100-level nesting: {:?}", p),
        Ok(Err(_)) => { /* rejection is fine */ }
        Ok(Ok(_)) => {
            let got = db.json_get("deep-100", "$").unwrap();
            assert!(got.is_some(), "100-level nested doc should be retrievable");
        }
    }
}

#[test]
fn json_wide_object_1000_keys() {
    let db = fresh_db();
    let mut map = serde_json::Map::new();
    for i in 0..1000 {
        map.insert(format!("key_{:04}", i), serde_json::json!(i));
    }
    let doc = serde_json::Value::Object(map);

    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        db.json_set("wide-1000", "$", json_to_value(&doc))
    }));
    match result {
        Err(p) => panic!("[PANIC] json_set panicked on 1000-key object: {:?}", p),
        Ok(Err(_)) => { /* rejection is fine */ }
        Ok(Ok(_)) => {
            let got = db.json_get("wide-1000", "$").unwrap();
            assert!(got.is_some());
            let got_json = value_to_json(&got.unwrap());
            assert_eq!(got_json, doc, "1000-key object should round-trip");
        }
    }
}

#[test]
fn json_large_array_5000_elements() {
    let db = fresh_db();
    let arr: Vec<serde_json::Value> = (0..5000).map(|i| serde_json::json!(i)).collect();
    let doc = serde_json::json!({"data": arr});

    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        db.json_set("big-array", "$", json_to_value(&doc))
    }));
    match result {
        Err(p) => panic!("[PANIC] json_set panicked on 5000-element array: {:?}", p),
        Ok(Err(_)) => { /* rejection is fine */ }
        Ok(Ok(_)) => {
            let got = db.json_get("big-array", "$").unwrap();
            assert!(got.is_some());
        }
    }
}

#[test]
fn event_many_types_rapid_fire() {
    let db = fresh_db();
    // 500 events with different types — stress the type index
    for i in 0..500 {
        let event_type = format!("type_{:04}", i % 50);
        let payload = serde_json::json!({"i": i, "data": "x".repeat(100)});
        db.event_append(&event_type, json_to_value(&payload)).unwrap();
    }
    assert_eq!(db.event_len().unwrap(), 500);

    // Each of the 50 types should have 10 events
    for i in 0..50 {
        let event_type = format!("type_{:04}", i);
        let events = db.event_get_by_type(&event_type).unwrap();
        assert_eq!(events.len(), 10, "type {} should have 10 events", event_type);
    }
}

#[test]
fn vector_search_all_zeros_collection() {
    let db = fresh_db();
    db.vector_create_collection("zeros", 4, DistanceMetric::Cosine).unwrap();

    // Insert vectors that are all zeros — cosine similarity is undefined
    db.vector_upsert("zeros", "z1", vec![0.0, 0.0, 0.0, 0.0], None).unwrap();
    db.vector_upsert("zeros", "z2", vec![0.0, 0.0, 0.0, 0.0], None).unwrap();

    // Search should not panic
    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        db.vector_search("zeros", vec![0.0, 0.0, 0.0, 0.0], 2)
    }));
    match result {
        Err(p) => panic!("[PANIC] vector_search panicked on all-zero vectors: {:?}", p),
        Ok(Err(_)) => { /* error is acceptable for degenerate input */ }
        Ok(Ok(_)) => { /* any result is fine, just shouldn't panic */ }
    }
}

#[test]
fn vector_search_with_negative_embeddings() {
    let db = fresh_db();
    db.vector_create_collection("neg", 4, DistanceMetric::Euclidean).unwrap();

    db.vector_upsert("neg", "n1", vec![-1.0, -2.0, -3.0, -4.0], None).unwrap();
    db.vector_upsert("neg", "n2", vec![-0.5, -1.5, -2.5, -3.5], None).unwrap();
    db.vector_upsert("neg", "n3", vec![1.0, 2.0, 3.0, 4.0], None).unwrap();

    let results = db.vector_search("neg", vec![-1.0, -2.0, -3.0, -4.0], 3).unwrap();
    assert_eq!(results[0].key, "n1", "exact match should be nearest");
}

#[test]
fn rapid_overwrite_same_key() {
    let db = fresh_db();
    // Overwrite the same key 10,000 times
    for i in 0..10_000 {
        db.kv_put("hotkey", i as i64).unwrap();
    }
    let got = db.kv_get("hotkey").unwrap().unwrap();
    assert_eq!(got, stratadb::Value::Int(9999));
}

#[test]
fn rapid_cas_contention_simulation() {
    let db = fresh_db();
    let v = db.state_set("contested", stratadb::Value::Int(0)).unwrap();

    // Simulate 100 CAS attempts, only the first should succeed
    let mut last_good_version = v;
    let mut successes = 0;
    for i in 1..=100 {
        let result = db.state_cas("contested", Some(v), stratadb::Value::Int(i)).unwrap();
        if let Some(new_v) = result {
            successes += 1;
            last_good_version = new_v;
        }
    }

    // Only the first attempt (with the original version) should succeed
    assert_eq!(successes, 1, "only one CAS should succeed with same stale version");

    // The cell should hold the value from the single successful CAS
    let got = db.state_get("contested").unwrap().unwrap();
    assert_eq!(got, stratadb::Value::Int(1));

    // Now do a valid CAS with the correct version
    let result = db.state_cas("contested", Some(last_good_version), stratadb::Value::Int(999)).unwrap();
    assert!(result.is_some(), "CAS with correct version should succeed");
}
