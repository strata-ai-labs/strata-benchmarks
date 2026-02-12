//! Dataset-driven State tests.
//!
//! Loads `data/state.json` and verifies cell operations, CAS sequences,
//! CAS conflicts, and init semantics.
//!
//! Note: the dataset stores "expected_value"/"new_value" for CAS steps,
//! but StrataDB's CAS API uses version counters.  We bridge the two by
//! tracking the version returned from each write.

mod common;

use common::{load_state_dataset, fresh_db};

#[test]
fn set_and_read_all_cells() {
    let ds = load_state_dataset();
    let db = fresh_db();

    for cell in &ds.cells {
        db.state_set(&cell.cell, cell.value.to_value()).unwrap();
    }

    for cell in &ds.cells {
        let got = db.state_get(&cell.cell).unwrap();
        assert_eq!(
            got,
            Some(cell.value.to_value()),
            "mismatch for cell: {}",
            cell.cell
        );
    }
}

#[test]
fn cas_sequence_counter_increment() {
    let ds = load_state_dataset();
    let db = fresh_db();

    // Set initial values and track versions
    let mut versions = std::collections::HashMap::new();
    for cell in &ds.cells {
        let v = db.state_set(&cell.cell, cell.value.to_value()).unwrap();
        versions.insert(cell.cell.clone(), v);
    }

    // Run the counter:operations CAS sequence
    let seq = &ds.cas_sequences[0];
    assert_eq!(seq.cell, "counter:operations");

    let mut ver = *versions.get(&seq.cell).unwrap();
    for step in &seq.steps {
        let result = db.state_cas(&seq.cell, Some(ver), step.new_value.to_value()).unwrap();
        assert!(result.is_some(), "CAS step failed for {}", seq.cell);
        ver = result.unwrap();
    }

    let final_val = db.state_get(&seq.cell).unwrap().unwrap();
    let last_step = seq.steps.last().unwrap();
    assert_eq!(final_val, last_step.new_value.to_value());
}

#[test]
fn cas_sequence_phase_transitions() {
    let ds = load_state_dataset();
    let db = fresh_db();

    let mut versions = std::collections::HashMap::new();
    for cell in &ds.cells {
        let v = db.state_set(&cell.cell, cell.value.to_value()).unwrap();
        versions.insert(cell.cell.clone(), v);
    }

    let seq = &ds.cas_sequences[1];
    assert_eq!(seq.cell, "phase:pipeline");

    let mut ver = *versions.get(&seq.cell).unwrap();
    for step in &seq.steps {
        let result = db.state_cas(&seq.cell, Some(ver), step.new_value.to_value()).unwrap();
        assert!(result.is_some(), "CAS phase transition failed");
        ver = result.unwrap();
    }

    let final_val = db.state_get(&seq.cell).unwrap().unwrap();
    assert_eq!(final_val, stratadb::Value::String("complete".into()));
}

#[test]
fn cas_sequence_lock_acquire_release() {
    let ds = load_state_dataset();
    let db = fresh_db();

    let mut versions = std::collections::HashMap::new();
    for cell in &ds.cells {
        let v = db.state_set(&cell.cell, cell.value.to_value()).unwrap();
        versions.insert(cell.cell.clone(), v);
    }

    let seq = &ds.cas_sequences[2];
    assert_eq!(seq.cell, "lock:global");

    let ver = *versions.get(&seq.cell).unwrap();

    // Acquire lock
    let result = db.state_cas(&seq.cell, Some(ver), seq.steps[0].new_value.to_value()).unwrap();
    assert!(result.is_some(), "lock acquire failed");
    let ver2 = result.unwrap();

    let locked = db.state_get(&seq.cell).unwrap().unwrap();
    assert_eq!(locked, stratadb::Value::String("agent-1".into()));

    // Release lock
    let result = db.state_cas(&seq.cell, Some(ver2), seq.steps[1].new_value.to_value()).unwrap();
    assert!(result.is_some(), "lock release failed");

    let free = db.state_get(&seq.cell).unwrap().unwrap();
    assert_eq!(free, stratadb::Value::String("free".into()));
}

#[test]
fn cas_conflict_wrong_version() {
    let ds = load_state_dataset();
    let db = fresh_db();

    let conflict = &ds.cas_conflicts[0];
    let ver = db.state_set(&conflict.cell, stratadb::Value::String("free".into())).unwrap();

    // Agent-1 acquires lock
    let result = db.state_cas(
        &conflict.cell,
        Some(ver),
        stratadb::Value::String("agent-1".into()),
    ).unwrap();
    assert!(result.is_some());

    // Agent-2 tries with stale version — should fail
    let result = db.state_cas(
        &conflict.cell,
        Some(ver), // stale version
        stratadb::Value::String("agent-2".into()),
    ).unwrap();
    assert!(result.is_none(), "CAS should fail with stale version");

    // Lock should still be held by agent-1
    let val = db.state_get(&conflict.cell).unwrap().unwrap();
    assert_eq!(val, stratadb::Value::String("agent-1".into()));
}

#[test]
fn init_creates_new_cells() {
    let ds = load_state_dataset();
    let db = fresh_db();

    for init in &ds.init_cells {
        db.state_init(&init.cell, init.value.to_value()).unwrap();
    }

    for init in &ds.init_cells {
        let got = db.state_get(&init.cell).unwrap().unwrap();
        assert_eq!(got, init.value.to_value(), "init cell mismatch: {}", init.cell);
    }
}

#[test]
fn init_is_idempotent() {
    let ds = load_state_dataset();
    let db = fresh_db();

    let init = &ds.init_cells[0];
    let v1 = db.state_init(&init.cell, init.value.to_value()).unwrap();

    // Second init should succeed idempotently
    let v2 = db.state_init(&init.cell, stratadb::Value::Int(9999)).unwrap();
    assert_eq!(v1, v2, "idempotent init should return same version");

    // Original value should be preserved (not overwritten)
    let got = db.state_get(&init.cell).unwrap().unwrap();
    assert_eq!(got, init.value.to_value());
}

#[test]
fn cells_are_independent() {
    let ds = load_state_dataset();
    let db = fresh_db();

    for cell in &ds.cells {
        db.state_set(&cell.cell, cell.value.to_value()).unwrap();
    }

    // Modify one cell
    db.state_set("counter:operations", stratadb::Value::Int(999)).unwrap();

    // Other cells should be unaffected
    let health = db.state_get("status:health").unwrap().unwrap();
    assert_eq!(health, stratadb::Value::String("green".into()));

    let batch = db.state_get("config:batch_size").unwrap().unwrap();
    assert_eq!(batch, stratadb::Value::Int(100));
}
