//! Black-box tests for the Vector Store primitive.

use stratadb::{Strata, Value, DistanceMetric};
use std::collections::HashMap;

fn db() -> Strata {
    Strata::cache().expect("failed to open temp db")
}

// =============================================================================
// Collection lifecycle
// =============================================================================

#[test]
fn create_collection() {
    let db = db();
    let version = db.vector_create_collection("embeddings", 4, DistanceMetric::Cosine).unwrap();
    assert!(version > 0);
}

#[test]
fn create_duplicate_collection_fails() {
    let db = db();
    db.vector_create_collection("embeddings", 4, DistanceMetric::Cosine).unwrap();
    let result = db.vector_create_collection("embeddings", 4, DistanceMetric::Cosine);
    assert!(result.is_err());
}

#[test]
fn delete_collection() {
    let db = db();
    db.vector_create_collection("to_delete", 4, DistanceMetric::Cosine).unwrap();
    assert!(db.vector_delete_collection("to_delete").unwrap());
}

#[test]
fn delete_nonexistent_collection() {
    let db = db();
    let result = db.vector_delete_collection("ghost");
    // Should either return false or error — either way, no panic
    match result {
        Ok(false) => {}
        Err(_) => {}
        Ok(true) => panic!("should not successfully delete a nonexistent collection"),
    }
}

#[test]
fn list_collections() {
    let db = db();
    db.vector_create_collection("a", 4, DistanceMetric::Cosine).unwrap();
    db.vector_create_collection("b", 8, DistanceMetric::Euclidean).unwrap();

    let collections = db.vector_list_collections().unwrap();
    assert_eq!(collections.len(), 2);

    let names: Vec<&str> = collections.iter().map(|c| c.name.as_str()).collect();
    assert!(names.contains(&"a"));
    assert!(names.contains(&"b"));
}

// =============================================================================
// Upsert and Get
// =============================================================================

#[test]
fn upsert_and_get() {
    let db = db();
    db.vector_create_collection("vecs", 3, DistanceMetric::Cosine).unwrap();
    db.vector_upsert("vecs", "v1", vec![1.0, 0.0, 0.0], None).unwrap();

    let data = db.vector_get("vecs", "v1").unwrap();
    assert!(data.is_some());
    let data = data.unwrap();
    assert_eq!(data.key, "v1");
    assert_eq!(data.data.embedding, vec![1.0, 0.0, 0.0]);
}

#[test]
fn upsert_with_metadata() {
    let db = db();
    db.vector_create_collection("vecs", 3, DistanceMetric::Cosine).unwrap();

    let meta = Value::Object({
        let mut m = HashMap::new();
        m.insert("source".to_string(), Value::String("test".into()));
        m
    });
    db.vector_upsert("vecs", "v1", vec![1.0, 0.0, 0.0], Some(meta.clone())).unwrap();

    let data = db.vector_get("vecs", "v1").unwrap().unwrap();
    assert_eq!(data.data.metadata, Some(meta));
}

#[test]
fn upsert_overwrites() {
    let db = db();
    db.vector_create_collection("vecs", 3, DistanceMetric::Cosine).unwrap();
    db.vector_upsert("vecs", "v1", vec![1.0, 0.0, 0.0], None).unwrap();
    db.vector_upsert("vecs", "v1", vec![0.0, 1.0, 0.0], None).unwrap();

    let data = db.vector_get("vecs", "v1").unwrap().unwrap();
    assert_eq!(data.data.embedding, vec![0.0, 1.0, 0.0]);
}

#[test]
fn get_nonexistent_vector() {
    let db = db();
    db.vector_create_collection("vecs", 3, DistanceMetric::Cosine).unwrap();
    assert!(db.vector_get("vecs", "ghost").unwrap().is_none());
}

// =============================================================================
// Delete
// =============================================================================

#[test]
fn delete_vector() {
    let db = db();
    db.vector_create_collection("vecs", 3, DistanceMetric::Cosine).unwrap();
    db.vector_upsert("vecs", "v1", vec![1.0, 0.0, 0.0], None).unwrap();

    assert!(db.vector_delete("vecs", "v1").unwrap());
    assert!(db.vector_get("vecs", "v1").unwrap().is_none());
}

#[test]
fn delete_nonexistent_vector() {
    let db = db();
    db.vector_create_collection("vecs", 3, DistanceMetric::Cosine).unwrap();
    assert!(!db.vector_delete("vecs", "ghost").unwrap());
}

// =============================================================================
// Search
// =============================================================================

#[test]
fn search_cosine_nearest_neighbor() {
    let db = db();
    db.vector_create_collection("vecs", 4, DistanceMetric::Cosine).unwrap();

    db.vector_upsert("vecs", "north", vec![0.0, 1.0, 0.0, 0.0], None).unwrap();
    db.vector_upsert("vecs", "east", vec![1.0, 0.0, 0.0, 0.0], None).unwrap();
    db.vector_upsert("vecs", "south", vec![0.0, -1.0, 0.0, 0.0], None).unwrap();

    // Query for something close to "north"
    let results = db.vector_search("vecs", vec![0.1, 0.99, 0.0, 0.0], 3).unwrap();
    assert!(!results.is_empty());
    assert_eq!(results[0].key, "north");
}

#[test]
fn search_returns_k_results() {
    let db = db();
    db.vector_create_collection("vecs", 4, DistanceMetric::Cosine).unwrap();

    for i in 0..10 {
        let mut v = vec![0.0f32; 4];
        v[i % 4] = 1.0;
        db.vector_upsert("vecs", &format!("v{}", i), v, None).unwrap();
    }

    let results = db.vector_search("vecs", vec![1.0, 0.0, 0.0, 0.0], 5).unwrap();
    assert!(results.len() <= 5);
}

#[test]
fn search_empty_collection() {
    let db = db();
    db.vector_create_collection("vecs", 4, DistanceMetric::Cosine).unwrap();

    let results = db.vector_search("vecs", vec![1.0, 0.0, 0.0, 0.0], 10).unwrap();
    assert!(results.is_empty());
}

#[test]
fn search_scores_are_ordered() {
    let db = db();
    db.vector_create_collection("vecs", 4, DistanceMetric::Cosine).unwrap();

    db.vector_upsert("vecs", "close", vec![0.9, 0.1, 0.0, 0.0], None).unwrap();
    db.vector_upsert("vecs", "far", vec![0.0, 0.0, 0.0, 1.0], None).unwrap();
    db.vector_upsert("vecs", "medium", vec![0.5, 0.5, 0.0, 0.0], None).unwrap();

    let results = db.vector_search("vecs", vec![1.0, 0.0, 0.0, 0.0], 3).unwrap();
    // Scores should be non-increasing
    for i in 1..results.len() {
        assert!(results[i - 1].score >= results[i].score,
            "results not sorted by score: {} < {}", results[i - 1].score, results[i].score);
    }
}

// =============================================================================
// Distance metrics
// =============================================================================

#[test]
fn euclidean_metric() {
    let db = db();
    db.vector_create_collection("euc", 3, DistanceMetric::Euclidean).unwrap();

    db.vector_upsert("euc", "origin", vec![0.0, 0.0, 0.0], None).unwrap();
    db.vector_upsert("euc", "near", vec![0.1, 0.1, 0.1], None).unwrap();
    db.vector_upsert("euc", "far", vec![10.0, 10.0, 10.0], None).unwrap();

    let results = db.vector_search("euc", vec![0.0, 0.0, 0.0], 3).unwrap();
    assert_eq!(results[0].key, "origin");
}

#[test]
fn dot_product_metric() {
    let db = db();
    db.vector_create_collection("dot", 3, DistanceMetric::DotProduct).unwrap();

    db.vector_upsert("dot", "aligned", vec![1.0, 1.0, 1.0], None).unwrap();
    db.vector_upsert("dot", "orthogonal", vec![0.0, 0.0, 1.0], None).unwrap();

    let results = db.vector_search("dot", vec![1.0, 1.0, 0.0], 2).unwrap();
    assert_eq!(results[0].key, "aligned");
}

// =============================================================================
// Collection info
// =============================================================================

#[test]
fn collection_info_reflects_state() {
    let db = db();
    db.vector_create_collection("vecs", 128, DistanceMetric::Cosine).unwrap();

    let collections = db.vector_list_collections().unwrap();
    let info = collections.iter().find(|c| c.name == "vecs").unwrap();
    assert_eq!(info.dimension, 128);
}
