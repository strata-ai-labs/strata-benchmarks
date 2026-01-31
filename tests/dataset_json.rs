//! Dataset-driven JSON document tests.
//!
//! Loads `data/json_docs.json` and verifies document storage, path queries,
//! mutations, deletions, and prefix listing.

mod common;

use common::{load_json_dataset, json_to_value, value_to_json, fresh_db};

#[test]
fn insert_and_readback_all_documents() {
    let ds = load_json_dataset();
    let db = fresh_db();

    for doc in &ds.documents {
        db.json_set(&doc.key, "$", json_to_value(&doc.doc)).unwrap();
    }

    for doc in &ds.documents {
        let got = db.json_get(&doc.key, "$").unwrap();
        assert!(got.is_some(), "missing document: {}", doc.key);
        let got_json = value_to_json(&got.unwrap());
        assert_eq!(got_json, doc.doc, "document mismatch for key: {}", doc.key);
    }
}

#[test]
fn path_queries() {
    let ds = load_json_dataset();
    let db = fresh_db();

    for doc in &ds.documents {
        db.json_set(&doc.key, "$", json_to_value(&doc.doc)).unwrap();
    }

    for q in &ds.path_queries {
        let got = db.json_get(&q.key, &q.path).unwrap();
        assert!(
            got.is_some(),
            "path query returned None: key={} path={}",
            q.key, q.path
        );
        let got_json = value_to_json(&got.unwrap());
        assert_eq!(
            got_json, q.expected,
            "path query mismatch: key={} path={}",
            q.key, q.path
        );
    }
}

#[test]
fn mutations() {
    let ds = load_json_dataset();
    let db = fresh_db();

    for doc in &ds.documents {
        db.json_set(&doc.key, "$", json_to_value(&doc.doc)).unwrap();
    }

    for m in &ds.mutations {
        db.json_set(&m.key, &m.path, json_to_value(&m.new_value)).unwrap();
    }

    // Verify mutations took effect
    for m in &ds.mutations {
        let got = db.json_get(&m.key, &m.path).unwrap();
        assert!(got.is_some(), "mutated path missing: key={} path={}", m.key, m.path);
        let got_json = value_to_json(&got.unwrap());
        assert_eq!(
            got_json, m.new_value,
            "mutation verification failed: key={} path={}",
            m.key, m.path
        );
    }
}

#[test]
fn document_deletion() {
    let ds = load_json_dataset();
    let db = fresh_db();

    for doc in &ds.documents {
        db.json_set(&doc.key, "$", json_to_value(&doc.doc)).unwrap();
    }

    // Delete product:gadget-x entirely (path "$")
    let whole_doc_deletion = ds.deletions.iter().find(|d| d.path == "$").unwrap();
    db.json_delete(&whole_doc_deletion.key, &whole_doc_deletion.path).unwrap();

    let result = db.json_get(&whole_doc_deletion.key, "$").unwrap();
    assert!(result.is_none(), "deleted document should return None");
}

#[test]
fn field_deletion() {
    let ds = load_json_dataset();
    let db = fresh_db();

    for doc in &ds.documents {
        db.json_set(&doc.key, "$", json_to_value(&doc.doc)).unwrap();
    }

    // Delete preferences.language from user_profile:1001
    let field_deletion = ds.deletions.iter().find(|d| d.path != "$").unwrap();
    db.json_delete(&field_deletion.key, &field_deletion.path).unwrap();

    let result = db.json_get(&field_deletion.key, &field_deletion.path).unwrap();
    assert!(result.is_none(), "deleted field should return None");

    // Parent document should still exist
    let parent = db.json_get(&field_deletion.key, "$").unwrap();
    assert!(parent.is_some(), "parent doc should still exist after field delete");
}

#[test]
fn prefix_listing_counts() {
    let ds = load_json_dataset();
    let db = fresh_db();

    for doc in &ds.documents {
        db.json_set(&doc.key, "$", json_to_value(&doc.doc)).unwrap();
    }

    for (prefix, expected_count) in &ds.prefixes {
        let (results, _cursor) = db.json_list(Some(prefix.clone()), None, 1000).unwrap();
        assert_eq!(
            results.len(),
            *expected_count,
            "prefix '{}' expected {} docs, got {}",
            prefix,
            expected_count,
            results.len()
        );
    }
}

#[test]
fn mutations_dont_affect_other_documents() {
    let ds = load_json_dataset();
    let db = fresh_db();

    for doc in &ds.documents {
        db.json_set(&doc.key, "$", json_to_value(&doc.doc)).unwrap();
    }

    // Mutate widget-b price
    db.json_set("product:widget-b", "price", json_to_value(&serde_json::json!(44.99))).unwrap();

    // widget-a price should be unchanged
    let wa_price = db.json_get("product:widget-a", "price").unwrap().unwrap();
    let wa_json = value_to_json(&wa_price);
    assert_eq!(wa_json, serde_json::json!(29.99));
}
