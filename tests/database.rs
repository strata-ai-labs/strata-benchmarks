//! Black-box tests for database-level operations.

use stratadb::Strata;

fn db() -> Strata {
    Strata::open_temp().expect("failed to open temp db")
}

// =============================================================================
// Ping
// =============================================================================

#[test]
fn ping_returns_version() {
    let db = db();
    let version = db.ping().unwrap();
    assert!(!version.is_empty());
}

// =============================================================================
// Info
// =============================================================================

#[test]
fn info_returns_valid_data() {
    let db = db();
    let info = db.info().unwrap();
    assert!(!info.version.is_empty());
}

// =============================================================================
// Flush
// =============================================================================

#[test]
fn flush_succeeds() {
    let db = db();
    db.kv_put("key", "value").unwrap();
    db.flush().unwrap();
}

// =============================================================================
// Compact
// =============================================================================

#[test]
fn compact_succeeds() {
    let db = db();
    db.kv_put("key", "value").unwrap();
    db.compact().unwrap();
}

// =============================================================================
// Persistence
// =============================================================================

#[test]
fn data_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap();

    {
        let db = Strata::open(path).unwrap();
        db.kv_put("persistent", "data").unwrap();
        db.flush().unwrap();
    }

    {
        let db = Strata::open(path).unwrap();
        assert_eq!(
            db.kv_get("persistent").unwrap(),
            Some(stratadb::Value::String("data".into()))
        );
    }
}

#[test]
fn branches_persist_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap();

    {
        let db = Strata::open(path).unwrap();
        db.create_branch("test-branch").unwrap();
        db.flush().unwrap();
    }

    {
        let db = Strata::open(path).unwrap();
        let branches = db.list_branches().unwrap();
        assert!(branches.contains(&"test-branch".to_string()));
    }
}

#[test]
fn all_primitives_persist() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap();

    {
        let db = Strata::open(path).unwrap();
        db.kv_put("kv-key", "kv-value").unwrap();
        db.state_set("state-cell", 42i64).unwrap();
        db.event_append("stream", stratadb::Value::Object(
            [("x".to_string(), stratadb::Value::Int(1))].into_iter().collect()
        )).unwrap();
        db.json_set("doc", "$", stratadb::Value::Object(
            [("field".to_string(), stratadb::Value::String("value".into()))].into_iter().collect()
        )).unwrap();
        db.flush().unwrap();
    }

    {
        let db = Strata::open(path).unwrap();
        assert_eq!(db.kv_get("kv-key").unwrap(), Some(stratadb::Value::String("kv-value".into())));
        assert_eq!(db.state_read("state-cell").unwrap(), Some(stratadb::Value::Int(42)));
        assert_eq!(db.event_len().unwrap(), 1);
        assert!(db.json_get("doc", "field").unwrap().is_some());
    }
}

/// Vector embeddings do not survive database reopen.
/// This documents the current behavior — vector data is in-memory only
/// and is not recovered from WAL/snapshots.
#[test]
fn vector_data_does_not_persist_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap();

    {
        let db = Strata::open(path).unwrap();
        db.vector_create_collection("vecs", 3, stratadb::DistanceMetric::Cosine).unwrap();
        db.vector_upsert("vecs", "v1", vec![1.0, 0.0, 0.0], None).unwrap();
        db.flush().unwrap();
    }

    {
        let db = Strata::open(path).unwrap();
        // Vector data is currently lost on reopen — this test documents that limitation
        let result = db.vector_get("vecs", "v1");
        assert!(result.is_err() || result.unwrap().is_none(),
            "vector data unexpectedly survived reopen — if this passes, vector persistence is fixed!");
    }
}
