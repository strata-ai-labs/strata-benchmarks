//! Dataset-driven Branch tests.
//!
//! Loads `data/branches.json` and verifies branch isolation, per-branch data,
//! isolation checks, and cross-branch comparison.

mod common;

use common::{load_branch_dataset, json_to_value, fresh_db};

#[test]
fn create_branches_and_populate() {
    let ds = load_branch_dataset();
    let db = fresh_db();

    for branch_name in &ds.branches {
        db.create_branch(branch_name).unwrap();
    }

    let listed = db.list_branches().unwrap();
    for branch_name in &ds.branches {
        assert!(
            listed.iter().any(|b| b == branch_name),
            "branch '{}' not listed",
            branch_name
        );
    }
}

#[test]
fn per_branch_kv_isolation() {
    let ds = load_branch_dataset();
    let mut db = fresh_db();

    for branch_name in &ds.branches {
        db.create_branch(branch_name).unwrap();
    }

    for (branch_name, data) in &ds.per_branch_data {
        db.set_branch(branch_name).unwrap();
        for entry in &data.kv {
            db.kv_put(&entry.key, entry.value.to_value()).unwrap();
        }
    }

    for (branch_name, data) in &ds.per_branch_data {
        db.set_branch(branch_name).unwrap();
        for entry in &data.kv {
            let got = db.kv_get(&entry.key).unwrap();
            assert!(got.is_some(), "branch '{}' missing key '{}'", branch_name, entry.key);
            assert_eq!(got.unwrap(), entry.value.to_value());
        }
    }
}

#[test]
fn per_branch_state_isolation() {
    let ds = load_branch_dataset();
    let mut db = fresh_db();

    for branch_name in &ds.branches {
        db.create_branch(branch_name).unwrap();
    }

    for (branch_name, data) in &ds.per_branch_data {
        db.set_branch(branch_name).unwrap();
        for cell in &data.state {
            db.state_set(&cell.cell, cell.value.to_value()).unwrap();
        }
    }

    for (branch_name, data) in &ds.per_branch_data {
        db.set_branch(branch_name).unwrap();
        for cell in &data.state {
            let got = db.state_get(&cell.cell).unwrap();
            assert_eq!(
                got,
                Some(cell.value.to_value()),
                "branch '{}' state cell '{}' mismatch",
                branch_name,
                cell.cell
            );
        }
    }
}

#[test]
fn per_branch_event_isolation() {
    let ds = load_branch_dataset();
    let mut db = fresh_db();

    for branch_name in &ds.branches {
        db.create_branch(branch_name).unwrap();
    }

    for (branch_name, data) in &ds.per_branch_data {
        db.set_branch(branch_name).unwrap();
        for event in &data.events {
            db.event_append(&event.event_type, json_to_value(&event.payload)).unwrap();
        }
    }

    for (branch_name, data) in &ds.per_branch_data {
        db.set_branch(branch_name).unwrap();
        let len = db.event_len().unwrap();
        assert_eq!(
            len,
            data.events.len() as u64,
            "branch '{}' event count mismatch",
            branch_name
        );
    }
}

#[test]
fn isolation_check_kv_values() {
    let ds = load_branch_dataset();
    let mut db = fresh_db();

    for branch_name in &ds.branches {
        db.create_branch(branch_name).unwrap();
    }

    for (branch_name, data) in &ds.per_branch_data {
        db.set_branch(branch_name).unwrap();
        for entry in &data.kv {
            db.kv_put(&entry.key, entry.value.to_value()).unwrap();
        }
    }

    // Run isolation checks that have a key + expected_value
    for check in &ds.isolation_checks {
        if let (Some(key), Some(expected)) = (&check.key, &check.expected_value) {
            // Try to switch to the target branch
            if db.set_branch(&check.on_branch).is_ok() {
                let got = db.kv_get(key).unwrap();
                if expected.is_null() {
                    assert!(
                        got.is_none(),
                        "isolation check '{}': key '{}' should not exist on branch '{}'",
                        check.description,
                        key,
                        check.on_branch
                    );
                } else {
                    assert_eq!(
                        got.unwrap(),
                        expected.to_value(),
                        "isolation check '{}' failed",
                        check.description
                    );
                }
            }
        }
    }
}

#[test]
fn isolation_check_event_count() {
    let ds = load_branch_dataset();
    let mut db = fresh_db();

    for branch_name in &ds.branches {
        db.create_branch(branch_name).unwrap();
    }

    for (branch_name, data) in &ds.per_branch_data {
        db.set_branch(branch_name).unwrap();
        for event in &data.events {
            db.event_append(&event.event_type, json_to_value(&event.payload)).unwrap();
        }
    }

    for check in &ds.isolation_checks {
        if let Some(expected_count) = check.expected_event_count {
            db.set_branch(&check.on_branch).unwrap();
            let len = db.event_len().unwrap();
            assert_eq!(
                len, expected_count as u64,
                "isolation check '{}' failed: expected {} events, got {}",
                check.description, expected_count, len
            );
        }
    }
}

#[test]
fn cross_branch_state_comparison() {
    let ds = load_branch_dataset();
    let mut db = fresh_db();

    for branch_name in &ds.branches {
        db.create_branch(branch_name).unwrap();
    }

    for (branch_name, data) in &ds.per_branch_data {
        db.set_branch(branch_name).unwrap();
        for cell in &data.state {
            db.state_set(&cell.cell, cell.value.to_value()).unwrap();
        }
    }

    let cmp = &ds.cross_branch_comparison;

    let mut values: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
    for (branch_name, expected_val) in &cmp.expected {
        db.set_branch(branch_name).unwrap();
        let got = db.state_get(&cmp.cell).unwrap().unwrap();
        if let stratadb::Value::Float(f) = got {
            values.insert(branch_name.clone(), f);
            assert!(
                (f - expected_val).abs() < 1e-9,
                "branch '{}' cell '{}': expected {}, got {}",
                branch_name,
                cmp.cell,
                expected_val,
                f
            );
        } else {
            panic!("expected Float for cell '{}' on branch '{}'", cmp.cell, branch_name);
        }
    }

    // Verify winner has the highest value
    let winner_val = values.get(&cmp.winner).unwrap();
    for (branch, val) in &values {
        assert!(
            winner_val >= val,
            "winner '{}' ({}) should have highest value, but '{}' has {}",
            cmp.winner,
            winner_val,
            branch,
            val
        );
    }
}

#[test]
fn default_branch_sees_no_branch_data() {
    let ds = load_branch_dataset();
    let mut db = fresh_db();

    for branch_name in &ds.branches {
        db.create_branch(branch_name).unwrap();
    }

    for (branch_name, data) in &ds.per_branch_data {
        db.set_branch(branch_name).unwrap();
        for entry in &data.kv {
            db.kv_put(&entry.key, entry.value.to_value()).unwrap();
        }
    }

    db.set_branch("default").unwrap();

    for (_branch_name, data) in &ds.per_branch_data {
        for entry in &data.kv {
            let got = db.kv_get(&entry.key).unwrap();
            assert!(
                got.is_none(),
                "default branch should not see key '{}' from another branch",
                entry.key
            );
        }
    }
}
