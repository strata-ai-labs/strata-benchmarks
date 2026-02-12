//! Black-box tests that exercise multiple primitives together.

use stratadb::{Strata, Value, DistanceMetric};
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
// Agent workflow simulation
// =============================================================================

#[test]
fn agent_workflow_kv_events_state() {
    let db = db();

    // Agent starts a task
    db.state_set("agent:status", "working").unwrap();
    db.kv_put("task:current", "search for docs").unwrap();

    // Agent logs tool calls as events
    db.event_append("tool_call", obj(&[
        ("tool", Value::String("web_search".into())),
        ("query", Value::String("rust embedded database".into())),
    ])).unwrap();

    db.event_append("tool_call", obj(&[
        ("tool", Value::String("read_file".into())),
        ("path", Value::String("/docs/readme.md".into())),
    ])).unwrap();

    // Agent stores results
    db.kv_put("result:search", "found 5 relevant docs").unwrap();

    // Agent completes
    db.state_set("agent:status", "done").unwrap();

    // Verify state
    assert_eq!(db.state_get("agent:status").unwrap(), Some(Value::String("done".into())));
    assert_eq!(db.event_len().unwrap(), 2);
    assert_eq!(db.kv_get("result:search").unwrap(), Some(Value::String("found 5 relevant docs".into())));
}

// =============================================================================
// Multi-branch agent experiment
// =============================================================================

#[test]
fn multi_branch_experiment() {
    let mut db = db();

    // Set up baseline data on default branch
    db.kv_put("config:model", "gpt-4").unwrap();
    db.kv_put("config:temperature", "0.7").unwrap();

    // Run experiment A with different settings
    db.create_branch("experiment-a").unwrap();
    db.set_branch("experiment-a").unwrap();
    db.kv_put("config:model", "claude-3").unwrap();
    db.kv_put("config:temperature", "0.3").unwrap();
    db.kv_put("result:score", "0.92").unwrap();

    // Run experiment B with yet different settings
    db.set_branch("default").unwrap();
    db.create_branch("experiment-b").unwrap();
    db.set_branch("experiment-b").unwrap();
    db.kv_put("config:model", "gpt-4").unwrap();
    db.kv_put("config:temperature", "0.1").unwrap();
    db.kv_put("result:score", "0.88").unwrap();

    // Compare results
    db.set_branch("experiment-a").unwrap();
    let score_a = db.kv_get("result:score").unwrap().unwrap();

    db.set_branch("experiment-b").unwrap();
    let score_b = db.kv_get("result:score").unwrap().unwrap();

    assert_ne!(score_a, score_b);

    // Default branch has no results
    db.set_branch("default").unwrap();
    assert_eq!(db.kv_get("result:score").unwrap(), None);
}

// =============================================================================
// JSON + Vector: document with embeddings
// =============================================================================

#[test]
fn document_with_embedding() {
    let db = db();

    // Store a document
    db.json_set("article:1", "$", obj(&[
        ("title", Value::String("Rust for AI".into())),
        ("body", Value::String("Rust is great for building AI infrastructure".into())),
    ])).unwrap();

    // Store its embedding
    db.vector_create_collection("article_embeddings", 4, DistanceMetric::Cosine).unwrap();
    db.vector_upsert("article_embeddings", "article:1", vec![0.8, 0.2, 0.1, 0.0], None).unwrap();

    // Store another document + embedding
    db.json_set("article:2", "$", obj(&[
        ("title", Value::String("Python ML".into())),
        ("body", Value::String("Python dominates machine learning".into())),
    ])).unwrap();
    db.vector_upsert("article_embeddings", "article:2", vec![0.1, 0.9, 0.3, 0.0], None).unwrap();

    // Search by embedding
    let results = db.vector_search("article_embeddings", vec![0.7, 0.3, 0.1, 0.0], 2).unwrap();
    assert_eq!(results[0].key, "article:1");

    // Retrieve the document for the top result
    let title = db.json_get(&results[0].key, "title").unwrap();
    assert_eq!(title, Some(Value::String("Rust for AI".into())));
}

// =============================================================================
// State CAS for coordination
// =============================================================================

#[test]
fn cas_based_lock() {
    let db = db();

    // Acquire lock
    let v = db.state_cas("lock", None, "agent-1").unwrap();
    assert!(v.is_some(), "lock acquisition should succeed");

    // Second acquire fails (wrong expected counter)
    let v2 = db.state_cas("lock", None, "agent-2").unwrap();
    assert!(v2.is_none(), "second lock acquisition should fail");

    // Release lock by setting to new value with correct counter
    let version = v.unwrap();
    let v3 = db.state_cas("lock", Some(version), "free").unwrap();
    assert!(v3.is_some(), "lock release should succeed");
}

// =============================================================================
// Event log as audit trail
// =============================================================================

#[test]
fn audit_trail_across_operations() {
    let db = db();

    // Simulate a series of operations with audit events
    db.kv_put("user:1:name", "Alice").unwrap();
    db.event_append("audit", obj(&[
        ("action", Value::String("create_user".into())),
        ("user", Value::String("user:1".into())),
    ])).unwrap();

    db.kv_put("user:1:name", "Alice Smith").unwrap();
    db.event_append("audit", obj(&[
        ("action", Value::String("update_user".into())),
        ("user", Value::String("user:1".into())),
        ("field", Value::String("name".into())),
    ])).unwrap();

    db.kv_delete("user:1:name").unwrap();
    db.event_append("audit", obj(&[
        ("action", Value::String("delete_user".into())),
        ("user", Value::String("user:1".into())),
    ])).unwrap();

    // Audit trail has all operations
    let audit = db.event_get_by_type("audit").unwrap();
    assert_eq!(audit.len(), 3);

    // Data is gone but audit trail remains
    assert_eq!(db.kv_get("user:1:name").unwrap(), None);
}

// =============================================================================
// All primitives in one branch, then isolate
// =============================================================================

#[test]
fn all_primitives_isolated_on_branch_switch() {
    let mut db = db();

    // Populate all primitives on default
    db.kv_put("kv", "default").unwrap();
    db.state_set("state", "default").unwrap();
    db.event_append("stream", obj(&[("branch", Value::String("default".into()))])).unwrap();
    db.json_set("doc", "$", obj(&[("branch", Value::String("default".into()))])).unwrap();
    db.vector_create_collection("vecs", 3, DistanceMetric::Cosine).unwrap();
    db.vector_upsert("vecs", "v1", vec![1.0, 0.0, 0.0], None).unwrap();

    // Switch to new empty branch
    db.create_branch("empty").unwrap();
    db.set_branch("empty").unwrap();

    // Nothing visible
    assert_eq!(db.kv_get("kv").unwrap(), None);
    assert_eq!(db.state_get("state").unwrap(), None);
    assert_eq!(db.event_len().unwrap(), 0);
    assert_eq!(db.json_get("doc", "$").unwrap(), None);
    assert!(db.vector_list_collections().unwrap().is_empty());

    // Switch back — everything still there
    db.set_branch("default").unwrap();
    assert!(db.kv_get("kv").unwrap().is_some());
    assert!(db.state_get("state").unwrap().is_some());
    assert_eq!(db.event_len().unwrap(), 1);
    assert!(db.json_get("doc", "$").unwrap().is_some());
    assert_eq!(db.vector_list_collections().unwrap().len(), 1);
}
