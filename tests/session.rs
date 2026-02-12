//! Black-box tests for Session (transactional command execution).
//!
//! Tests exercise the Session API through the public `stratadb` types:
//! Session, Command, Output, and Database.

use stratadb::{Command, Database, Output, Session, Strata, Value};
use std::sync::Arc;

fn db() -> Arc<Database> {
    Database::cache().unwrap()
}

fn session() -> Session {
    Session::new(db())
}

// =============================================================================
// Transaction Lifecycle
// =============================================================================

#[test]
fn session_starts_without_transaction() {
    let s = session();
    assert!(!s.in_transaction());
}

#[test]
fn begin_starts_transaction() {
    let mut s = session();
    let output = s.execute(Command::TxnBegin { branch: None, options: None }).unwrap();
    assert!(matches!(output, Output::TxnBegun));
    assert!(s.in_transaction());
}

#[test]
fn commit_ends_transaction() {
    let mut s = session();
    s.execute(Command::TxnBegin { branch: None, options: None }).unwrap();
    assert!(s.in_transaction());

    let output = s.execute(Command::TxnCommit).unwrap();
    assert!(matches!(output, Output::TxnCommitted { .. }));
    assert!(!s.in_transaction());
}

#[test]
fn rollback_ends_transaction() {
    let mut s = session();
    s.execute(Command::TxnBegin { branch: None, options: None }).unwrap();
    assert!(s.in_transaction());

    let output = s.execute(Command::TxnRollback).unwrap();
    assert!(matches!(output, Output::TxnAborted));
    assert!(!s.in_transaction());
}

#[test]
fn txn_is_active_reflects_state() {
    let mut s = session();
    let output = s.execute(Command::TxnIsActive).unwrap();
    assert!(matches!(output, Output::Bool(false)));

    s.execute(Command::TxnBegin { branch: None, options: None }).unwrap();
    let output = s.execute(Command::TxnIsActive).unwrap();
    assert!(matches!(output, Output::Bool(true)));

    s.execute(Command::TxnCommit).unwrap();
    let output = s.execute(Command::TxnIsActive).unwrap();
    assert!(matches!(output, Output::Bool(false)));
}

#[test]
fn txn_info_none_when_no_transaction() {
    let mut s = session();
    let output = s.execute(Command::TxnInfo).unwrap();
    assert!(matches!(output, Output::TxnInfo(None)));
}

#[test]
fn txn_info_some_when_active() {
    let mut s = session();
    s.execute(Command::TxnBegin { branch: None, options: None }).unwrap();

    let output = s.execute(Command::TxnInfo).unwrap();
    match output {
        Output::TxnInfo(Some(info)) => {
            assert!(!info.id.is_empty());
        }
        _ => panic!("Expected TxnInfo(Some(_))"),
    }
}

// =============================================================================
// Error States
// =============================================================================

#[test]
fn begin_while_active_fails() {
    let mut s = session();
    s.execute(Command::TxnBegin { branch: None, options: None }).unwrap();

    let result = s.execute(Command::TxnBegin { branch: None, options: None });
    assert!(result.is_err(), "Double begin should fail");
    // Transaction should still be active
    assert!(s.in_transaction());
}

#[test]
fn commit_without_transaction_fails() {
    let mut s = session();
    let result = s.execute(Command::TxnCommit);
    assert!(result.is_err(), "Commit without active transaction should fail");
}

#[test]
fn rollback_without_transaction_fails() {
    let mut s = session();
    let result = s.execute(Command::TxnRollback);
    assert!(result.is_err(), "Rollback without active transaction should fail");
}

// =============================================================================
// Read-Your-Writes (within transaction)
// =============================================================================

#[test]
fn read_your_writes_kv() {
    let mut s = session();
    s.execute(Command::TxnBegin { branch: None, options: None }).unwrap();

    // Write within transaction
    s.execute(Command::KvPut {
        branch: None,
        space: None,
        key: "key".into(),
        value: Value::Int(42),
    }).unwrap();

    // Read within same transaction sees the write
    let output = s.execute(Command::KvGet {
        branch: None,
        space: None,
        key: "key".into(),
        as_of: None,
    }).unwrap();

    match output {
        Output::Maybe(Some(val)) => assert_eq!(val, Value::Int(42)),
        _ => panic!("Expected to read our own write within transaction"),
    }
}

#[test]
fn read_your_writes_state() {
    let mut s = session();
    s.execute(Command::TxnBegin { branch: None, options: None }).unwrap();

    // StateInit is transactional (StateSet bypasses transactions)
    s.execute(Command::StateInit {
        branch: None,
        space: None,
        cell: "cell".into(),
        value: Value::String("hello".into()),
    }).unwrap();

    let output = s.execute(Command::StateGet {
        branch: None,
        space: None,
        cell: "cell".into(),
        as_of: None,
    }).unwrap();

    match output {
        Output::Maybe(Some(val)) => assert_eq!(val, Value::String("hello".into())),
        _ => panic!("Expected to read state write within transaction"),
    }
}

#[test]
fn read_your_writes_event() {
    let mut s = session();
    s.execute(Command::TxnBegin { branch: None, options: None }).unwrap();

    s.execute(Command::EventAppend {
        branch: None,
        space: None,
        event_type: "test".into(),
        payload: Value::Object(
            [("data".to_string(), Value::Int(1))].into_iter().collect(),
        ),
    }).unwrap();

    let output = s.execute(Command::EventLen { branch: None, space: None }).unwrap();
    match output {
        Output::Uint(len) => assert_eq!(len, 1),
        _ => panic!("Expected event count within transaction"),
    }
}

// =============================================================================
// Commit makes writes visible
// =============================================================================

#[test]
fn commit_makes_kv_writes_visible() {
    let db = db();
    let mut s = Session::new(db.clone());

    s.execute(Command::TxnBegin { branch: None, options: None }).unwrap();
    s.execute(Command::KvPut {
        branch: None,
        space: None,
        key: "committed".into(),
        value: Value::Int(99),
    }).unwrap();
    s.execute(Command::TxnCommit).unwrap();

    // New session on same database should see committed data
    let strata = Strata::from_database(db).unwrap();
    assert_eq!(strata.kv_get("committed").unwrap(), Some(Value::Int(99)));
}

#[test]
fn commit_makes_state_writes_visible() {
    let db = db();
    let mut s = Session::new(db.clone());

    s.execute(Command::TxnBegin { branch: None, options: None }).unwrap();
    // Use StateInit (transactional) instead of StateSet (bypasses transaction)
    s.execute(Command::StateInit {
        branch: None,
        space: None,
        cell: "cell".into(),
        value: Value::Int(7),
    }).unwrap();
    s.execute(Command::TxnCommit).unwrap();

    let strata = Strata::from_database(db).unwrap();
    assert_eq!(strata.state_get("cell").unwrap(), Some(Value::Int(7)));
}

// =============================================================================
// Rollback discards writes
// =============================================================================

#[test]
fn rollback_discards_kv_writes() {
    let db = db();
    let mut s = Session::new(db.clone());

    s.execute(Command::TxnBegin { branch: None, options: None }).unwrap();
    s.execute(Command::KvPut {
        branch: None,
        space: None,
        key: "rolled_back".into(),
        value: Value::Int(1),
    }).unwrap();
    s.execute(Command::TxnRollback).unwrap();

    // Data should not be visible after rollback
    let strata = Strata::from_database(db).unwrap();
    assert_eq!(strata.kv_get("rolled_back").unwrap(), None);
}

#[test]
fn rollback_discards_state_writes() {
    let db = db();
    let mut s = Session::new(db.clone());

    s.execute(Command::TxnBegin { branch: None, options: None }).unwrap();
    // Use StateInit (transactional) instead of StateSet (bypasses transaction)
    s.execute(Command::StateInit {
        branch: None,
        space: None,
        cell: "temp".into(),
        value: Value::Int(999),
    }).unwrap();
    s.execute(Command::TxnRollback).unwrap();

    let strata = Strata::from_database(db).unwrap();
    assert_eq!(strata.state_get("temp").unwrap(), None);
}

// =============================================================================
// Session drop auto-rollback
// =============================================================================

#[test]
fn session_drop_rolls_back_uncommitted() {
    let db = db();

    {
        let mut s = Session::new(db.clone());
        s.execute(Command::TxnBegin { branch: None, options: None }).unwrap();
        s.execute(Command::KvPut {
            branch: None,
            space: None,
            key: "orphaned".into(),
            value: Value::Int(42),
        }).unwrap();
        // Dropped without commit or rollback
    }

    // Data should have been auto-rolled back
    let strata = Strata::from_database(db).unwrap();
    assert_eq!(strata.kv_get("orphaned").unwrap(), None);
}

// =============================================================================
// Non-transactional commands bypass transaction
// =============================================================================

#[test]
fn branch_commands_bypass_transaction() {
    let mut s = session();
    s.execute(Command::TxnBegin { branch: None, options: None }).unwrap();

    // Branch list should work even within a transaction
    let output = s.execute(Command::BranchList {
        state: None,
        limit: None,
        offset: None,
    }).unwrap();
    assert!(matches!(output, Output::BranchInfoList(_)));

    s.execute(Command::TxnRollback).unwrap();
}

#[test]
fn db_commands_work_in_transaction() {
    let mut s = session();
    s.execute(Command::TxnBegin { branch: None, options: None }).unwrap();

    let output = s.execute(Command::Ping).unwrap();
    assert!(matches!(output, Output::Pong { .. }));

    let output = s.execute(Command::Info).unwrap();
    assert!(matches!(output, Output::DatabaseInfo(_)));

    s.execute(Command::TxnRollback).unwrap();
}

// =============================================================================
// Multiple operations in a transaction
// =============================================================================

#[test]
fn multiple_kv_operations_in_transaction() {
    let db = db();
    let mut s = Session::new(db.clone());

    s.execute(Command::TxnBegin { branch: None, options: None }).unwrap();

    // Multiple puts
    s.execute(Command::KvPut {
        branch: None,
        space: None,
        key: "a".into(),
        value: Value::Int(1),
    }).unwrap();
    s.execute(Command::KvPut {
        branch: None,
        space: None,
        key: "b".into(),
        value: Value::Int(2),
    }).unwrap();
    s.execute(Command::KvPut {
        branch: None,
        space: None,
        key: "c".into(),
        value: Value::Int(3),
    }).unwrap();

    // Delete one
    s.execute(Command::KvDelete {
        branch: None,
        space: None,
        key: "b".into(),
    }).unwrap();

    // Overwrite one
    s.execute(Command::KvPut {
        branch: None,
        space: None,
        key: "a".into(),
        value: Value::Int(10),
    }).unwrap();

    s.execute(Command::TxnCommit).unwrap();

    // Verify final state
    let strata = Strata::from_database(db).unwrap();
    assert_eq!(strata.kv_get("a").unwrap(), Some(Value::Int(10)));
    assert_eq!(strata.kv_get("b").unwrap(), None);
    assert_eq!(strata.kv_get("c").unwrap(), Some(Value::Int(3)));
}

#[test]
fn cross_primitive_transaction() {
    let db = db();
    let mut s = Session::new(db.clone());

    s.execute(Command::TxnBegin { branch: None, options: None }).unwrap();

    // KV
    s.execute(Command::KvPut {
        branch: None,
        space: None,
        key: "key".into(),
        value: Value::String("val".into()),
    }).unwrap();

    // State (StateInit is transactional; StateSet bypasses transactions)
    s.execute(Command::StateInit {
        branch: None,
        space: None,
        cell: "cell".into(),
        value: Value::Int(42),
    }).unwrap();

    // Event
    s.execute(Command::EventAppend {
        branch: None,
        space: None,
        event_type: "audit".into(),
        payload: Value::Object(
            [("action".to_string(), Value::String("test".into()))].into_iter().collect(),
        ),
    }).unwrap();

    s.execute(Command::TxnCommit).unwrap();

    // All should be visible
    let strata = Strata::from_database(db).unwrap();
    assert_eq!(strata.kv_get("key").unwrap(), Some(Value::String("val".into())));
    assert_eq!(strata.state_get("cell").unwrap(), Some(Value::Int(42)));
    assert_eq!(strata.event_len().unwrap(), 1);
}

// =============================================================================
// Sequential transactions
// =============================================================================

#[test]
fn multiple_sequential_transactions() {
    let db = db();
    let mut s = Session::new(db.clone());

    // First transaction
    s.execute(Command::TxnBegin { branch: None, options: None }).unwrap();
    s.execute(Command::KvPut {
        branch: None,
        space: None,
        key: "counter".into(),
        value: Value::Int(1),
    }).unwrap();
    s.execute(Command::TxnCommit).unwrap();

    // Second transaction
    s.execute(Command::TxnBegin { branch: None, options: None }).unwrap();
    s.execute(Command::KvPut {
        branch: None,
        space: None,
        key: "counter".into(),
        value: Value::Int(2),
    }).unwrap();
    s.execute(Command::TxnCommit).unwrap();

    // Third transaction (rolled back)
    s.execute(Command::TxnBegin { branch: None, options: None }).unwrap();
    s.execute(Command::KvPut {
        branch: None,
        space: None,
        key: "counter".into(),
        value: Value::Int(999),
    }).unwrap();
    s.execute(Command::TxnRollback).unwrap();

    // Final value should be from second transaction
    let strata = Strata::from_database(db).unwrap();
    assert_eq!(strata.kv_get("counter").unwrap(), Some(Value::Int(2)));
}

// =============================================================================
// Commands outside transaction execute immediately
// =============================================================================

#[test]
fn commands_without_transaction_auto_commit() {
    let db = db();
    let mut s = Session::new(db.clone());

    // Execute without begin — should auto-commit
    s.execute(Command::KvPut {
        branch: None,
        space: None,
        key: "auto".into(),
        value: Value::Int(1),
    }).unwrap();

    // Should be visible immediately
    let strata = Strata::from_database(db).unwrap();
    assert_eq!(strata.kv_get("auto").unwrap(), Some(Value::Int(1)));
}

#[test]
fn commit_returns_version() {
    let mut s = session();
    s.execute(Command::TxnBegin { branch: None, options: None }).unwrap();
    s.execute(Command::KvPut {
        branch: None,
        space: None,
        key: "versioned".into(),
        value: Value::Int(1),
    }).unwrap();

    let output = s.execute(Command::TxnCommit).unwrap();
    match output {
        Output::TxnCommitted { version } => {
            assert!(version > 0, "Commit version should be positive");
        }
        _ => panic!("Expected TxnCommitted with version"),
    }
}
