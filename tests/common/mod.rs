//! Shared test utilities for loading dataset fixtures.

use std::collections::HashMap;
use std::path::PathBuf;

use serde::Deserialize;
use stratadb::{Strata, Value, DistanceMetric};

// =============================================================================
// Dataset root path
// =============================================================================

pub fn data_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data")
}

// =============================================================================
// KV dataset
// =============================================================================

#[derive(Deserialize)]
pub struct KvDataset {
    pub entries: Vec<KvEntry>,
    pub prefixes: HashMap<String, usize>,
    pub deletions: Vec<String>,
    pub overwrites: Vec<KvEntry>,
}

#[derive(Deserialize)]
pub struct KvEntry {
    pub key: String,
    pub value: JsonValue,
}

// =============================================================================
// State dataset
// =============================================================================

#[derive(Deserialize)]
pub struct StateDataset {
    pub cells: Vec<StateCell>,
    pub cas_sequences: Vec<CasSequence>,
    pub cas_conflicts: Vec<CasConflict>,
    pub init_cells: Vec<StateCell>,
}

#[derive(Deserialize)]
pub struct StateCell {
    pub cell: String,
    pub value: JsonValue,
}

#[derive(Deserialize)]
pub struct CasSequence {
    pub cell: String,
    pub steps: Vec<CasStep>,
}

#[derive(Deserialize)]
pub struct CasStep {
    pub expected_value: JsonValue,
    pub new_value: JsonValue,
}

#[derive(Deserialize)]
pub struct CasConflict {
    pub cell: String,
    pub description: String,
    pub setup: JsonValue,
    pub agent_1: JsonValue,
    pub agent_2: JsonValue,
    pub expected_winner: String,
}

// =============================================================================
// Event dataset
// =============================================================================

#[derive(Deserialize)]
pub struct EventDataset {
    pub events: Vec<EventEntry>,
    pub expected_counts: HashMap<String, usize>,
    pub total: usize,
}

#[derive(Deserialize)]
pub struct EventEntry {
    pub event_type: String,
    pub payload: serde_json::Value,
}

// =============================================================================
// JSON dataset
// =============================================================================

#[derive(Deserialize)]
pub struct JsonDataset {
    pub documents: Vec<JsonDoc>,
    pub path_queries: Vec<PathQuery>,
    pub mutations: Vec<PathMutation>,
    pub deletions: Vec<JsonDeletion>,
    pub prefixes: HashMap<String, usize>,
}

#[derive(Deserialize)]
pub struct JsonDoc {
    pub key: String,
    pub doc: serde_json::Value,
}

#[derive(Deserialize)]
pub struct PathQuery {
    pub key: String,
    pub path: String,
    pub expected: serde_json::Value,
}

#[derive(Deserialize)]
pub struct PathMutation {
    pub key: String,
    pub path: String,
    pub new_value: serde_json::Value,
}

#[derive(Deserialize)]
pub struct JsonDeletion {
    pub key: String,
    pub path: String,
}

// =============================================================================
// Vector dataset
// =============================================================================

#[derive(Deserialize)]
pub struct VectorDataset {
    pub collections: Vec<VectorCollection>,
    pub search_queries: Vec<SearchQuery>,
}

#[derive(Deserialize)]
pub struct VectorCollection {
    pub name: String,
    pub dimension: u64,
    pub metric: String,
    pub vectors: Vec<VectorEntry>,
}

#[derive(Deserialize)]
pub struct VectorEntry {
    pub key: String,
    pub embedding: Vec<f32>,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Deserialize)]
pub struct SearchQuery {
    pub collection: String,
    pub query: Vec<f32>,
    pub k: u64,
    pub description: String,
    pub expected_top: String,
}

// =============================================================================
// Branch dataset
// =============================================================================

#[derive(Deserialize)]
pub struct BranchDataset {
    pub branches: Vec<String>,
    pub per_branch_data: HashMap<String, BranchData>,
    pub isolation_checks: Vec<IsolationCheck>,
    pub cross_branch_comparison: CrossBranchComparison,
}

#[derive(Deserialize)]
pub struct BranchData {
    pub kv: Vec<KvEntry>,
    pub state: Vec<StateCell>,
    pub events: Vec<EventEntry>,
}

#[derive(Deserialize)]
pub struct IsolationCheck {
    pub description: String,
    pub on_branch: String,
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default)]
    pub expected_value: Option<JsonValue>,
    #[serde(default)]
    pub expected_event_count: Option<usize>,
}

#[derive(Deserialize)]
pub struct CrossBranchComparison {
    pub cell: String,
    pub expected: HashMap<String, f64>,
    pub winner: String,
}

// =============================================================================
// Value conversion
// =============================================================================

/// JSON-serialized Value representation matching our dataset format.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum JsonValue {
    Tagged(TaggedValue),
    Null,
}

#[derive(Debug, Clone, Deserialize)]
pub enum TaggedValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Bytes(Vec<u8>),
}

impl JsonValue {
    pub fn to_value(&self) -> Value {
        match self {
            JsonValue::Tagged(TaggedValue::String(s)) => Value::String(s.clone()),
            JsonValue::Tagged(TaggedValue::Int(i)) => Value::Int(*i),
            JsonValue::Tagged(TaggedValue::Float(f)) => Value::Float(*f),
            JsonValue::Tagged(TaggedValue::Bool(b)) => Value::Bool(*b),
            JsonValue::Tagged(TaggedValue::Bytes(b)) => Value::Bytes(b.clone()),
            JsonValue::Null => Value::Null,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, JsonValue::Null)
    }
}

/// Convert a serde_json::Value to a stratadb::Value
pub fn json_to_value(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else {
                Value::Float(n.as_f64().unwrap())
            }
        }
        serde_json::Value::String(s) => Value::String(s.clone()),
        serde_json::Value::Array(arr) => {
            Value::Array(arr.iter().map(json_to_value).collect())
        }
        serde_json::Value::Object(obj) => {
            let map: HashMap<String, Value> = obj
                .iter()
                .map(|(k, v)| (k.clone(), json_to_value(v)))
                .collect();
            Value::Object(map)
        }
    }
}

/// Convert a stratadb::Value to serde_json::Value for comparison
pub fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::json!(*i),
        Value::Float(f) => serde_json::json!(*f),
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::Bytes(b) => serde_json::json!(b),
        Value::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(value_to_json).collect())
        }
        Value::Object(obj) => {
            let map: serde_json::Map<String, serde_json::Value> = obj
                .iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect();
            serde_json::Value::Object(map)
        }
    }
}

// =============================================================================
// Dataset loaders
// =============================================================================

pub fn load_kv_dataset() -> KvDataset {
    let path = data_dir().join("kv.json");
    let content = std::fs::read_to_string(&path).expect("failed to read kv.json");
    serde_json::from_str(&content).expect("failed to parse kv.json")
}

pub fn load_state_dataset() -> StateDataset {
    let path = data_dir().join("state.json");
    let content = std::fs::read_to_string(&path).expect("failed to read state.json");
    serde_json::from_str(&content).expect("failed to parse state.json")
}

pub fn load_event_dataset() -> EventDataset {
    let path = data_dir().join("events.json");
    let content = std::fs::read_to_string(&path).expect("failed to read events.json");
    serde_json::from_str(&content).expect("failed to parse events.json")
}

pub fn load_json_dataset() -> JsonDataset {
    let path = data_dir().join("json_docs.json");
    let content = std::fs::read_to_string(&path).expect("failed to read json_docs.json");
    serde_json::from_str(&content).expect("failed to parse json_docs.json")
}

pub fn load_vector_dataset() -> VectorDataset {
    let path = data_dir().join("vectors.json");
    let content = std::fs::read_to_string(&path).expect("failed to read vectors.json");
    serde_json::from_str(&content).expect("failed to parse vectors.json")
}

pub fn load_branch_dataset() -> BranchDataset {
    let path = data_dir().join("branches.json");
    let content = std::fs::read_to_string(&path).expect("failed to read branches.json");
    serde_json::from_str(&content).expect("failed to parse branches.json")
}

// =============================================================================
// Helpers
// =============================================================================

pub fn parse_metric(s: &str) -> DistanceMetric {
    match s {
        "cosine" => DistanceMetric::Cosine,
        "euclidean" => DistanceMetric::Euclidean,
        "dot_product" => DistanceMetric::DotProduct,
        other => panic!("unknown metric: {}", other),
    }
}

pub fn fresh_db() -> Strata {
    Strata::open_temp().expect("failed to open temp db")
}
