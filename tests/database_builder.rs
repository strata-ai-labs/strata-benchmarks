//! Black-box tests for Database opening, config file, and durability modes.
//!
//! Tests exercise Database::open(), Database::cache(), config file behavior,
//! DurabilityMode enum, and persistence/recovery across reopens.

use stratadb::{Database, DurabilityMode, Strata, StrataConfig, Value};
use std::sync::Arc;

/// Write a strata.toml with always durability before opening.
fn write_always_config(path: &std::path::Path) {
    std::fs::create_dir_all(path).expect("create dir for config");
    std::fs::write(path.join("strata.toml"), "durability = \"always\"\n")
        .expect("write always config");
}

// =============================================================================
// Database::open() basics
// =============================================================================

#[test]
fn open_creates_database() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    assert!(db.is_open());
}

#[test]
fn open_creates_config_file() {
    let dir = tempfile::tempdir().unwrap();
    let _db = Database::open(dir.path()).unwrap();
    assert!(dir.path().join("strata.toml").exists());
}

#[test]
fn open_creates_nested_directory() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("nested").join("db");
    assert!(!db_path.exists());

    let _db = Database::open(&db_path).unwrap();
    assert!(db_path.exists());
    assert!(db_path.join("strata.toml").exists());
}

#[test]
fn open_with_always_config() {
    let dir = tempfile::tempdir().unwrap();
    write_always_config(dir.path());
    let db = Database::open(dir.path()).unwrap();
    assert!(db.is_open());
}

// =============================================================================
// Database::cache()
// =============================================================================

#[test]
fn cache_database_is_cache() {
    let db = Database::cache().unwrap();
    assert!(db.is_cache());
    assert!(db.is_open());
}

#[test]
fn cache_database_supports_operations() {
    let db = Database::cache().unwrap();
    let strata = Strata::from_database(db).unwrap();
    strata.kv_put("key", "value").unwrap();
    assert_eq!(strata.kv_get("key").unwrap(), Some(Value::String("value".into())));
}

#[test]
fn cache_databases_are_independent() {
    let db1 = Database::cache().unwrap();
    let db2 = Database::cache().unwrap();
    assert!(!Arc::ptr_eq(&db1, &db2));

    let s1 = Strata::from_database(db1).unwrap();
    let s2 = Strata::from_database(db2).unwrap();

    s1.kv_put("key", "from-db1").unwrap();
    assert_eq!(s2.kv_get("key").unwrap(), None);
}

// =============================================================================
// DurabilityMode enum
// =============================================================================

#[test]
fn durability_mode_default_is_standard() {
    let mode = DurabilityMode::default();
    assert_eq!(mode, DurabilityMode::Standard {
        interval_ms: 100,
        batch_size: 1000,
    });
}

#[test]
fn durability_mode_cache_does_not_require_wal() {
    let mode = DurabilityMode::Cache;
    assert!(!mode.requires_wal());
    assert!(!mode.requires_immediate_fsync());
}

#[test]
fn durability_mode_always_requires_immediate_fsync() {
    let mode = DurabilityMode::Always;
    assert!(mode.requires_immediate_fsync());
    assert!(mode.requires_wal());
}

#[test]
fn durability_mode_standard_requires_wal_but_not_immediate_fsync() {
    let mode = DurabilityMode::Standard { interval_ms: 100, batch_size: 1000 };
    assert!(mode.requires_wal());
    assert!(!mode.requires_immediate_fsync());
}

#[test]
fn durability_mode_descriptions() {
    assert!(!DurabilityMode::Cache.description().is_empty());
    assert!(!DurabilityMode::Always.description().is_empty());
    assert!(!DurabilityMode::default().description().is_empty());
}

// =============================================================================
// StrataConfig
// =============================================================================

#[test]
fn strata_config_default_is_standard() {
    let config = StrataConfig::default();
    let mode = config.durability_mode().unwrap();
    assert!(matches!(mode, DurabilityMode::Standard { .. }));
}

// =============================================================================
// Persistent database properties
// =============================================================================

#[test]
fn persistent_database_is_not_cache() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    assert!(!db.is_cache());
}

// =============================================================================
// Strata::from_database() integration
// =============================================================================

#[test]
fn from_database_with_always_mode() {
    let dir = tempfile::tempdir().unwrap();
    write_always_config(dir.path());
    let db = Database::open(dir.path()).unwrap();
    let strata = Strata::from_database(db).unwrap();

    strata.kv_put("key", "always").unwrap();
    assert_eq!(strata.kv_get("key").unwrap(), Some(Value::String("always".into())));
}

#[test]
fn from_database_with_standard_mode() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    let strata = Strata::from_database(db).unwrap();

    strata.kv_put("key", "standard").unwrap();
    assert_eq!(strata.kv_get("key").unwrap(), Some(Value::String("standard".into())));
}

#[test]
fn from_database_with_cache() {
    let db = Database::cache().unwrap();
    let strata = Strata::from_database(db).unwrap();

    strata.kv_put("key", "cache").unwrap();
    assert_eq!(strata.kv_get("key").unwrap(), Some(Value::String("cache".into())));
}

// =============================================================================
// All primitives work across durability modes
// =============================================================================

fn exercise_all_primitives(strata: &Strata) {
    // KV
    strata.kv_put("k", "v").unwrap();
    assert_eq!(strata.kv_get("k").unwrap(), Some(Value::String("v".into())));

    // State
    strata.state_set("cell", 42i64).unwrap();
    assert_eq!(strata.state_get("cell").unwrap(), Some(Value::Int(42)));

    // Event
    strata.event_append("stream", Value::Object(
        [("x".to_string(), Value::Int(1))].into_iter().collect()
    )).unwrap();
    assert_eq!(strata.event_len().unwrap(), 1);

    // JSON
    strata.json_set("doc", "$", Value::Object(
        [("field".to_string(), Value::String("val".into()))].into_iter().collect()
    )).unwrap();
    assert!(strata.json_get("doc", "field").unwrap().is_some());
}

#[test]
fn all_primitives_work_with_always() {
    let dir = tempfile::tempdir().unwrap();
    write_always_config(dir.path());
    let db = Database::open(dir.path()).unwrap();
    let strata = Strata::from_database(db).unwrap();
    exercise_all_primitives(&strata);
}

#[test]
fn all_primitives_work_with_standard() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    let strata = Strata::from_database(db).unwrap();
    exercise_all_primitives(&strata);
}

#[test]
fn all_primitives_work_with_cache() {
    let db = Database::cache().unwrap();
    let strata = Strata::from_database(db).unwrap();
    exercise_all_primitives(&strata);
}

// =============================================================================
// Graceful persistence across reopen
// =============================================================================

#[test]
fn always_mode_survives_graceful_reopen() {
    let dir = tempfile::tempdir().unwrap();
    write_always_config(dir.path());

    {
        let db = Database::open(dir.path()).unwrap();
        let strata = Strata::from_database(db).unwrap();
        strata.kv_put("persist", "always-data").unwrap();
        strata.flush().unwrap();
    }

    {
        let db = Database::open(dir.path()).unwrap();
        let strata = Strata::from_database(db).unwrap();
        assert_eq!(
            strata.kv_get("persist").unwrap(),
            Some(Value::String("always-data".into()))
        );
    }
}

#[test]
fn standard_mode_survives_graceful_reopen() {
    let dir = tempfile::tempdir().unwrap();

    {
        let db = Database::open(dir.path()).unwrap();
        let strata = Strata::from_database(db).unwrap();
        strata.kv_put("persist", "standard-data").unwrap();
        strata.flush().unwrap();
    }

    {
        let db = Database::open(dir.path()).unwrap();
        let strata = Strata::from_database(db).unwrap();
        assert_eq!(
            strata.kv_get("persist").unwrap(),
            Some(Value::String("standard-data".into()))
        );
    }
}

#[test]
fn always_mode_all_primitives_survive_graceful_reopen() {
    let dir = tempfile::tempdir().unwrap();
    write_always_config(dir.path());

    {
        let db = Database::open(dir.path()).unwrap();
        let strata = Strata::from_database(db).unwrap();

        strata.kv_put("k", "v").unwrap();
        strata.state_set("cell", 42i64).unwrap();
        strata.event_append("stream", Value::Object(
            [("x".to_string(), Value::Int(1))].into_iter().collect()
        )).unwrap();
        strata.json_set("doc", "$", Value::Object(
            [("f".to_string(), Value::String("val".into()))].into_iter().collect()
        )).unwrap();
        strata.flush().unwrap();
    }

    {
        let db = Database::open(dir.path()).unwrap();
        let strata = Strata::from_database(db).unwrap();

        assert_eq!(strata.kv_get("k").unwrap(), Some(Value::String("v".into())));
        assert_eq!(strata.state_get("cell").unwrap(), Some(Value::Int(42)));
        assert_eq!(strata.event_len().unwrap(), 1);
        assert!(strata.json_get("doc", "f").unwrap().is_some());
    }
}

// =============================================================================
// Crash simulation (Always mode survives without flush)
// =============================================================================

#[test]
fn always_mode_survives_crash_without_flush() {
    let dir = tempfile::tempdir().unwrap();
    write_always_config(dir.path());

    {
        let db = Database::open(dir.path()).unwrap();
        let strata = Strata::from_database(db).unwrap();
        strata.kv_put("crash-key", "survived").unwrap();
        strata.state_set("crash-cell", 99i64).unwrap();
        // NO flush — simulates crash
    }

    {
        let db = Database::open(dir.path()).unwrap();
        let strata = Strata::from_database(db).unwrap();
        assert_eq!(
            strata.kv_get("crash-key").unwrap(),
            Some(Value::String("survived".into())),
            "Always mode should survive crash (fsync on every commit)"
        );
        assert_eq!(
            strata.state_get("crash-cell").unwrap(),
            Some(Value::Int(99)),
            "State should also survive crash in always mode"
        );
    }
}

#[test]
fn always_mode_many_writes_survive_crash() {
    let dir = tempfile::tempdir().unwrap();
    write_always_config(dir.path());

    let n = 50;
    {
        let db = Database::open(dir.path()).unwrap();
        let strata = Strata::from_database(db).unwrap();
        for i in 0..n {
            strata.kv_put(&format!("key:{}", i), Value::Int(i)).unwrap();
        }
        // NO flush — crash
    }

    {
        let db = Database::open(dir.path()).unwrap();
        let strata = Strata::from_database(db).unwrap();
        for i in 0..n {
            assert_eq!(
                strata.kv_get(&format!("key:{}", i)).unwrap(),
                Some(Value::Int(i)),
                "key:{} should survive crash in always mode", i
            );
        }
    }
}

#[test]
fn always_mode_all_primitives_survive_crash() {
    let dir = tempfile::tempdir().unwrap();
    write_always_config(dir.path());

    {
        let db = Database::open(dir.path()).unwrap();
        let strata = Strata::from_database(db).unwrap();

        strata.kv_put("k", "v").unwrap();
        strata.state_set("cell", 42i64).unwrap();
        strata.event_append("stream", Value::Object(
            [("x".to_string(), Value::Int(1))].into_iter().collect()
        )).unwrap();
        strata.json_set("doc", "$", Value::Object(
            [("f".to_string(), Value::String("val".into()))].into_iter().collect()
        )).unwrap();
        // NO flush — crash
    }

    {
        let db = Database::open(dir.path()).unwrap();
        let strata = Strata::from_database(db).unwrap();

        assert_eq!(strata.kv_get("k").unwrap(), Some(Value::String("v".into())),
            "KV should survive crash in always mode");
        assert_eq!(strata.state_get("cell").unwrap(), Some(Value::Int(42)),
            "State should survive crash in always mode");
        assert_eq!(strata.event_len().unwrap(), 1,
            "Events should survive crash in always mode");
        assert!(strata.json_get("doc", "f").unwrap().is_some(),
            "JSON should survive crash in always mode");
    }
}

#[test]
fn standard_mode_without_flush_may_lose_data() {
    let dir = tempfile::tempdir().unwrap();

    {
        let db = Database::open(dir.path()).unwrap();
        let strata = Strata::from_database(db).unwrap();
        strata.kv_put("maybe-lost", "data").unwrap();
        // NO flush — data may or may not be on disk
    }

    {
        let db = Database::open(dir.path()).unwrap();
        let strata = Strata::from_database(db).unwrap();
        let result = strata.kv_get("maybe-lost").unwrap();
        // Both presence and absence are valid for standard mode without flush
        let _ = result;
    }
}

#[test]
fn standard_mode_with_flush_survives_crash() {
    let dir = tempfile::tempdir().unwrap();

    {
        let db = Database::open(dir.path()).unwrap();
        let strata = Strata::from_database(db).unwrap();
        strata.kv_put("flushed", "data").unwrap();
        strata.flush().unwrap();
    }

    {
        let db = Database::open(dir.path()).unwrap();
        let strata = Strata::from_database(db).unwrap();
        assert_eq!(
            strata.kv_get("flushed").unwrap(),
            Some(Value::String("data".into())),
            "Flushed data should survive crash in standard mode"
        );
    }
}

#[test]
fn recovery_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    write_always_config(dir.path());

    {
        let db = Database::open(dir.path()).unwrap();
        let strata = Strata::from_database(db).unwrap();
        strata.kv_put("key", "value").unwrap();
        strata.state_set("cell", 1i64).unwrap();
        // crash (no flush)
    }

    // Open and close multiple times — recovery should be idempotent
    for _ in 0..3 {
        let db = Database::open(dir.path()).unwrap();
        let strata = Strata::from_database(db).unwrap();
        assert_eq!(strata.kv_get("key").unwrap(), Some(Value::String("value".into())));
        assert_eq!(strata.state_get("cell").unwrap(), Some(Value::Int(1)));
        // drop without flush — simulates repeated crashes
    }
}

#[test]
fn crash_after_many_operations_recovers_consistently() {
    let dir = tempfile::tempdir().unwrap();
    write_always_config(dir.path());

    {
        let db = Database::open(dir.path()).unwrap();
        let strata = Strata::from_database(db).unwrap();

        strata.kv_put("counter", 0i64).unwrap();
        for i in 1..=10 {
            strata.kv_put("counter", Value::Int(i)).unwrap();
        }
        strata.kv_put("temp", "here").unwrap();
        strata.kv_delete("temp").unwrap();
        // crash
    }

    {
        let db = Database::open(dir.path()).unwrap();
        let strata = Strata::from_database(db).unwrap();

        assert_eq!(strata.kv_get("counter").unwrap(), Some(Value::Int(10)));
        assert_eq!(strata.kv_get("temp").unwrap(), None);
        let history = strata.kv_getv("counter").unwrap().unwrap();
        assert_eq!(history.len(), 11, "All 11 versions (0..=10) should survive");
    }
}

// =============================================================================
// Shutdown behavior
// =============================================================================

#[test]
fn shutdown_stops_accepting_transactions() {
    let db = Database::cache().unwrap();
    assert!(db.is_open());
    db.shutdown().unwrap();
    assert!(!db.is_open());
}

#[test]
fn shutdown_persistent_database() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    assert!(db.is_open());
    db.shutdown().unwrap();
    assert!(!db.is_open());
}

// =============================================================================
// Registry: same path returns same instance
// =============================================================================

#[test]
fn open_same_path_returns_same_instance() {
    let dir = tempfile::tempdir().unwrap();

    let db1 = Database::open(dir.path()).unwrap();
    let db2 = Database::open(dir.path()).unwrap();

    assert!(Arc::ptr_eq(&db1, &db2), "Same path should return same Arc");
}

// =============================================================================
// Modes produce equivalent results
// =============================================================================

#[test]
fn all_modes_produce_same_results() {
    let modes: Vec<(&str, Box<dyn Fn() -> Strata>)> = vec![
        ("cache", Box::new(|| {
            let db = Database::cache().unwrap();
            Strata::from_database(db).unwrap()
        })),
        ("standard", Box::new(|| {
            let dir = tempfile::tempdir().unwrap();
            let db = Database::open(dir.path()).unwrap();
            Strata::from_database(db).unwrap()
        })),
        ("always", Box::new(|| {
            let dir = tempfile::tempdir().unwrap();
            write_always_config(dir.path());
            let db = Database::open(dir.path()).unwrap();
            Strata::from_database(db).unwrap()
        })),
    ];

    for (name, factory) in &modes {
        let db = factory();

        db.kv_put("a", 1i64).unwrap();
        db.kv_put("a", 2i64).unwrap();
        db.kv_put("b", 10i64).unwrap();

        assert_eq!(db.kv_get("a").unwrap(), Some(Value::Int(2)),
            "mode={}: kv latest value", name);
        assert_eq!(db.kv_get("b").unwrap(), Some(Value::Int(10)),
            "mode={}: kv second key", name);

        let history = db.kv_getv("a").unwrap().unwrap();
        assert_eq!(history.len(), 2, "mode={}: version history length", name);
        assert_eq!(history[0].value, Value::Int(2), "mode={}: newest first", name);
        assert_eq!(history[1].value, Value::Int(1), "mode={}: oldest last", name);

        let mut keys = db.kv_list(None).unwrap();
        keys.sort();
        assert_eq!(keys, vec!["a", "b"], "mode={}: list keys", name);
    }
}
