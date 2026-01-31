//! Dataset-driven Vector tests.
//!
//! Loads `data/vectors.json` and verifies collection creation, vector upsert,
//! search correctness, and metadata storage.

mod common;

use common::{load_vector_dataset, parse_metric, json_to_value, fresh_db};

#[test]
fn create_collections_and_upsert() {
    let ds = load_vector_dataset();
    let db = fresh_db();

    for coll in &ds.collections {
        let metric = parse_metric(&coll.metric);
        db.vector_create_collection(&coll.name, coll.dimension, metric).unwrap();

        for vec_entry in &coll.vectors {
            let meta = vec_entry.metadata.as_ref().map(|m| json_to_value(m));
            db.vector_upsert(&coll.name, &vec_entry.key, vec_entry.embedding.clone(), meta).unwrap();
        }
    }

    // Verify vectors exist
    for coll in &ds.collections {
        for vec_entry in &coll.vectors {
            let got = db.vector_get(&coll.name, &vec_entry.key).unwrap();
            assert!(got.is_some(), "missing vector: {}/{}", coll.name, vec_entry.key);
        }
    }
}

#[test]
fn search_returns_expected_top_result() {
    let ds = load_vector_dataset();
    let db = fresh_db();

    for coll in &ds.collections {
        let metric = parse_metric(&coll.metric);
        db.vector_create_collection(&coll.name, coll.dimension, metric).unwrap();
        for vec_entry in &coll.vectors {
            let meta = vec_entry.metadata.as_ref().map(|m| json_to_value(m));
            db.vector_upsert(&coll.name, &vec_entry.key, vec_entry.embedding.clone(), meta).unwrap();
        }
    }

    for q in &ds.search_queries {
        let results = db.vector_search(&q.collection, q.query.clone(), q.k).unwrap();

        assert!(
            !results.is_empty(),
            "search returned no results: {}",
            q.description
        );

        assert_eq!(
            results[0].key, q.expected_top,
            "search '{}': expected top={}, got top={}",
            q.description, q.expected_top, results[0].key
        );
    }
}

#[test]
fn search_returns_k_results() {
    let ds = load_vector_dataset();
    let db = fresh_db();

    for coll in &ds.collections {
        let metric = parse_metric(&coll.metric);
        db.vector_create_collection(&coll.name, coll.dimension, metric).unwrap();
        for vec_entry in &coll.vectors {
            let meta = vec_entry.metadata.as_ref().map(|m| json_to_value(m));
            db.vector_upsert(&coll.name, &vec_entry.key, vec_entry.embedding.clone(), meta).unwrap();
        }
    }

    for q in &ds.search_queries {
        let results = db.vector_search(&q.collection, q.query.clone(), q.k).unwrap();
        assert_eq!(
            results.len(),
            q.k as usize,
            "search '{}': expected {} results, got {}",
            q.description,
            q.k,
            results.len()
        );
    }
}

#[test]
fn search_scores_are_ordered() {
    let ds = load_vector_dataset();
    let db = fresh_db();

    for coll in &ds.collections {
        let metric = parse_metric(&coll.metric);
        db.vector_create_collection(&coll.name, coll.dimension, metric).unwrap();
        for vec_entry in &coll.vectors {
            let meta = vec_entry.metadata.as_ref().map(|m| json_to_value(m));
            db.vector_upsert(&coll.name, &vec_entry.key, vec_entry.embedding.clone(), meta).unwrap();
        }
    }

    for q in &ds.search_queries {
        let results = db.vector_search(&q.collection, q.query.clone(), q.k).unwrap();
        for window in results.windows(2) {
            assert!(
                window[0].score >= window[1].score,
                "search '{}': results not ordered: {} < {}",
                q.description,
                window[0].score,
                window[1].score
            );
        }
    }
}

#[test]
fn vector_delete_removes_entry() {
    let ds = load_vector_dataset();
    let db = fresh_db();

    let coll = &ds.collections[0];
    let metric = parse_metric(&coll.metric);
    db.vector_create_collection(&coll.name, coll.dimension, metric).unwrap();

    for vec_entry in &coll.vectors {
        let meta = vec_entry.metadata.as_ref().map(|m| json_to_value(m));
        db.vector_upsert(&coll.name, &vec_entry.key, vec_entry.embedding.clone(), meta).unwrap();
    }

    let target = &coll.vectors[0].key;
    db.vector_delete(&coll.name, target).unwrap();
    let got = db.vector_get(&coll.name, target).unwrap();
    assert!(got.is_none(), "deleted vector should return None");
}

#[test]
fn collection_list() {
    let ds = load_vector_dataset();
    let db = fresh_db();

    for coll in &ds.collections {
        let metric = parse_metric(&coll.metric);
        db.vector_create_collection(&coll.name, coll.dimension, metric).unwrap();
    }

    let collections = db.vector_list_collections().unwrap();
    assert_eq!(collections.len(), ds.collections.len());
    for coll in &ds.collections {
        assert!(
            collections.iter().any(|c| c.name == coll.name),
            "collection '{}' not in list",
            coll.name
        );
    }
}
