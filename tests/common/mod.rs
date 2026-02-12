//! Shared test utilities for loading JSONL dataset fixtures.

use std::collections::HashMap;
use std::io::BufRead;
use std::path::PathBuf;

use serde::Deserialize;
use stratadb::{DistanceMetric, Strata, Value};

// =============================================================================
// Dataset root path
// =============================================================================

pub fn data_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data")
}

// =============================================================================
// KV dataset
// =============================================================================

pub struct KvDataset {
    pub entries: Vec<KvEntry>,
    pub prefixes: HashMap<String, usize>,
    pub deletions: Vec<String>,
    pub overwrites: Vec<KvEntry>,
}

pub struct KvEntry {
    pub key: String,
    pub value: JsonValue,
}

#[derive(Deserialize)]
#[serde(tag = "_s")]
enum KvRecord {
    #[serde(rename = "entry")]
    Entry { key: String, value: JsonValue },
    #[serde(rename = "prefix")]
    Prefix { prefix: String, count: usize },
    #[serde(rename = "deletion")]
    Deletion { key: String },
    #[serde(rename = "overwrite")]
    Overwrite { key: String, value: JsonValue },
}

// =============================================================================
// State dataset
// =============================================================================

pub struct StateDataset {
    pub cells: Vec<StateCell>,
    pub cas_sequences: Vec<CasSequence>,
    pub cas_conflicts: Vec<CasConflict>,
    pub init_cells: Vec<StateCell>,
}

pub struct StateCell {
    pub cell: String,
    pub value: JsonValue,
}

pub struct CasSequence {
    pub cell: String,
    pub steps: Vec<CasStep>,
}

pub struct CasStep {
    pub expected_value: JsonValue,
    pub new_value: JsonValue,
}

pub struct CasConflict {
    pub cell: String,
    pub description: String,
    pub setup: JsonValue,
    pub agent_1: JsonValue,
    pub agent_2: JsonValue,
    pub expected_winner: String,
}

#[derive(Deserialize)]
struct CasStepRaw {
    expected_value: JsonValue,
    new_value: JsonValue,
}

#[derive(Deserialize)]
#[serde(tag = "_s")]
enum StateRecord {
    #[serde(rename = "cell")]
    Cell { cell: String, value: JsonValue },
    #[serde(rename = "cas_sequence")]
    CasSequence { cell: String, steps: Vec<CasStepRaw> },
    #[serde(rename = "cas_conflict")]
    CasConflict {
        cell: String,
        description: String,
        setup: JsonValue,
        agent_1: JsonValue,
        agent_2: JsonValue,
        expected_winner: String,
    },
    #[serde(rename = "init_cell")]
    InitCell { cell: String, value: JsonValue },
}

// =============================================================================
// Event dataset
// =============================================================================

pub struct EventDataset {
    pub events: Vec<EventEntry>,
    pub expected_counts: HashMap<String, usize>,
    pub total: usize,
}

pub struct EventEntry {
    pub event_type: String,
    pub payload: serde_json::Value,
}

#[derive(Deserialize)]
#[serde(tag = "_s")]
enum EventRecord {
    #[serde(rename = "event")]
    Event {
        event_type: String,
        payload: serde_json::Value,
    },
    #[serde(rename = "expected_count")]
    ExpectedCount { event_type: String, count: usize },
    #[serde(rename = "meta")]
    Meta { total: usize },
}

// =============================================================================
// JSON dataset
// =============================================================================

pub struct JsonDataset {
    pub documents: Vec<JsonDoc>,
    pub path_queries: Vec<PathQuery>,
    pub mutations: Vec<PathMutation>,
    pub deletions: Vec<JsonDeletion>,
    pub prefixes: HashMap<String, usize>,
}

pub struct JsonDoc {
    pub key: String,
    pub doc: serde_json::Value,
}

pub struct PathQuery {
    pub key: String,
    pub path: String,
    pub expected: serde_json::Value,
}

pub struct PathMutation {
    pub key: String,
    pub path: String,
    pub new_value: serde_json::Value,
}

pub struct JsonDeletion {
    pub key: String,
    pub path: String,
}

#[derive(Deserialize)]
#[serde(tag = "_s")]
enum JsonRecord {
    #[serde(rename = "document")]
    Document { key: String, doc: serde_json::Value },
    #[serde(rename = "path_query")]
    PathQuery {
        key: String,
        path: String,
        expected: serde_json::Value,
    },
    #[serde(rename = "mutation")]
    Mutation {
        key: String,
        path: String,
        new_value: serde_json::Value,
    },
    #[serde(rename = "deletion")]
    Deletion { key: String, path: String },
    #[serde(rename = "prefix")]
    Prefix { prefix: String, count: usize },
}

// =============================================================================
// Vector dataset
// =============================================================================

pub struct VectorDataset {
    pub collections: Vec<VectorCollection>,
    pub search_queries: Vec<SearchQuery>,
}

pub struct VectorCollection {
    pub name: String,
    pub dimension: u64,
    pub metric: String,
    pub vectors: Vec<VectorEntry>,
}

pub struct VectorEntry {
    pub key: String,
    pub embedding: Vec<f32>,
    pub metadata: Option<serde_json::Value>,
}

pub struct SearchQuery {
    pub collection: String,
    pub query: Vec<f32>,
    pub k: u64,
    pub description: String,
    pub expected_top: String,
}

#[derive(Deserialize)]
#[serde(tag = "_s")]
enum VectorRecord {
    #[serde(rename = "collection")]
    Collection {
        name: String,
        dimension: u64,
        metric: String,
    },
    #[serde(rename = "vector")]
    Vector {
        collection: String,
        key: String,
        embedding: Vec<f32>,
        metadata: Option<serde_json::Value>,
    },
    #[serde(rename = "search_query")]
    SearchQuery {
        collection: String,
        query: Vec<f32>,
        k: u64,
        description: String,
        expected_top: String,
    },
}

// =============================================================================
// Branch dataset
// =============================================================================

pub struct BranchDataset {
    pub branches: Vec<String>,
    pub per_branch_data: HashMap<String, BranchData>,
    pub isolation_checks: Vec<IsolationCheck>,
    pub cross_branch_comparison: CrossBranchComparison,
}

pub struct BranchData {
    pub kv: Vec<KvEntry>,
    pub state: Vec<StateCell>,
    pub events: Vec<EventEntry>,
}

pub struct IsolationCheck {
    pub description: String,
    pub on_branch: String,
    pub key: Option<String>,
    pub expected_value: Option<JsonValue>,
    pub expected_event_count: Option<usize>,
}

pub struct CrossBranchComparison {
    pub cell: String,
    pub expected: HashMap<String, f64>,
    pub winner: String,
}

#[derive(Deserialize)]
#[serde(tag = "_s")]
enum BranchRecord {
    #[serde(rename = "branch")]
    Branch { name: String },
    #[serde(rename = "branch_kv")]
    BranchKv {
        branch: String,
        key: String,
        value: JsonValue,
    },
    #[serde(rename = "branch_state")]
    BranchState {
        branch: String,
        cell: String,
        value: JsonValue,
    },
    #[serde(rename = "branch_event")]
    BranchEvent {
        branch: String,
        event_type: String,
        payload: serde_json::Value,
    },
    #[serde(rename = "isolation_check")]
    IsolationCheck {
        description: String,
        on_branch: String,
        #[serde(default)]
        key: Option<String>,
        #[serde(default)]
        expected_value: Option<JsonValue>,
        #[serde(default)]
        expected_event_count: Option<usize>,
    },
    #[serde(rename = "cross_branch_comparison")]
    CrossBranchComparison {
        cell: String,
        expected: HashMap<String, f64>,
        winner: String,
    },
}

// =============================================================================
// Dirty dataset
// =============================================================================

pub struct DirtyDataset {
    pub kv_roundtrips: Vec<DirtyKv>,
    pub kv_rejects: Vec<DirtyKv>,
    pub state_roundtrips: Vec<DirtyState>,
    pub event_roundtrips: Vec<DirtyEvent>,
    pub json_roundtrips: Vec<DirtyJson>,
    pub vector_roundtrips: Vec<DirtyVector>,
    pub cross_kv_json: Vec<DirtyCrossKvJson>,
    pub cross_branch_dirty: Vec<DirtyCrossBranch>,
}

pub struct DirtyKv {
    pub desc: String,
    pub key: String,
    pub value: JsonValue,
}

pub struct DirtyState {
    pub desc: String,
    pub cell: String,
    pub value: JsonValue,
}

pub struct DirtyEvent {
    pub desc: String,
    pub event_type: String,
    pub payload: serde_json::Value,
}

pub struct DirtyJson {
    pub desc: String,
    pub key: String,
    pub doc: serde_json::Value,
}

pub struct DirtyVector {
    pub desc: String,
    pub collection: String,
    pub key: String,
    pub embedding: Vec<f32>,
}

pub struct DirtyCrossKvJson {
    pub desc: String,
    pub key: String,
    pub kv_value: JsonValue,
    pub json_doc: serde_json::Value,
}

pub struct DirtyCrossBranch {
    pub desc: String,
    pub branch: String,
    pub key: String,
    pub kv_value: JsonValue,
    pub cell: String,
    pub state_value: JsonValue,
    pub event_type: String,
    pub event_payload: serde_json::Value,
}

#[derive(Deserialize)]
#[serde(tag = "_s")]
enum DirtyRecord {
    #[serde(rename = "kv")]
    Kv {
        desc: String,
        key: String,
        value: JsonValue,
    },
    #[serde(rename = "kv_reject")]
    KvReject {
        desc: String,
        key: String,
        value: JsonValue,
    },
    #[serde(rename = "state")]
    State {
        desc: String,
        cell: String,
        value: JsonValue,
    },
    #[serde(rename = "event")]
    Event {
        desc: String,
        event_type: String,
        payload: serde_json::Value,
    },
    #[serde(rename = "json")]
    Json {
        desc: String,
        key: String,
        doc: serde_json::Value,
    },
    #[serde(rename = "vector")]
    Vector {
        desc: String,
        collection: String,
        key: String,
        embedding: Vec<f32>,
    },
    #[serde(rename = "cross_kv_json")]
    CrossKvJson {
        desc: String,
        key: String,
        kv_value: JsonValue,
        json_doc: serde_json::Value,
    },
    #[serde(rename = "cross_branch_dirty")]
    CrossBranchDirty {
        desc: String,
        branch: String,
        key: String,
        kv_value: JsonValue,
        cell: String,
        state_value: JsonValue,
        event_type: String,
        event_payload: serde_json::Value,
    },
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
        serde_json::Value::Array(arr) => Value::Array(arr.iter().map(json_to_value).collect()),
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
        Value::Array(arr) => serde_json::Value::Array(arr.iter().map(value_to_json).collect()),
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
// JSONL reader helper
// =============================================================================

fn read_jsonl<T: serde::de::DeserializeOwned>(filename: &str) -> Vec<T> {
    let path = data_dir().join(filename);
    let file = std::fs::File::open(&path).unwrap_or_else(|e| panic!("failed to open {}: {}", filename, e));
    let reader = std::io::BufReader::new(file);
    reader
        .lines()
        .enumerate()
        .filter_map(|(line_num, line)| {
            let line = line.unwrap_or_else(|e| panic!("{}:{}: read error: {}", filename, line_num + 1, e));
            let trimmed = line.trim();
            if trimmed.is_empty() {
                return None;
            }
            Some(serde_json::from_str(trimmed).unwrap_or_else(|e| {
                panic!("{}:{}: parse error: {}\n  line: {}", filename, line_num + 1, e, trimmed)
            }))
        })
        .collect()
}

// =============================================================================
// Dataset loaders
// =============================================================================

pub fn load_kv_dataset() -> KvDataset {
    let records: Vec<KvRecord> = read_jsonl("kv.jsonl");
    let mut ds = KvDataset {
        entries: Vec::new(),
        prefixes: HashMap::new(),
        deletions: Vec::new(),
        overwrites: Vec::new(),
    };
    for r in records {
        match r {
            KvRecord::Entry { key, value } => ds.entries.push(KvEntry { key, value }),
            KvRecord::Prefix { prefix, count } => { ds.prefixes.insert(prefix, count); }
            KvRecord::Deletion { key } => ds.deletions.push(key),
            KvRecord::Overwrite { key, value } => ds.overwrites.push(KvEntry { key, value }),
        }
    }
    ds
}

pub fn load_state_dataset() -> StateDataset {
    let records: Vec<StateRecord> = read_jsonl("state.jsonl");
    let mut ds = StateDataset {
        cells: Vec::new(),
        cas_sequences: Vec::new(),
        cas_conflicts: Vec::new(),
        init_cells: Vec::new(),
    };
    for r in records {
        match r {
            StateRecord::Cell { cell, value } => ds.cells.push(StateCell { cell, value }),
            StateRecord::CasSequence { cell, steps } => {
                let steps = steps
                    .into_iter()
                    .map(|s| CasStep {
                        expected_value: s.expected_value,
                        new_value: s.new_value,
                    })
                    .collect();
                ds.cas_sequences.push(CasSequence { cell, steps });
            }
            StateRecord::CasConflict {
                cell, description, setup, agent_1, agent_2, expected_winner,
            } => ds.cas_conflicts.push(CasConflict {
                cell, description, setup, agent_1, agent_2, expected_winner,
            }),
            StateRecord::InitCell { cell, value } => ds.init_cells.push(StateCell { cell, value }),
        }
    }
    ds
}

pub fn load_event_dataset() -> EventDataset {
    let records: Vec<EventRecord> = read_jsonl("events.jsonl");
    let mut ds = EventDataset {
        events: Vec::new(),
        expected_counts: HashMap::new(),
        total: 0,
    };
    for r in records {
        match r {
            EventRecord::Event { event_type, payload } => {
                ds.events.push(EventEntry { event_type, payload });
            }
            EventRecord::ExpectedCount { event_type, count } => {
                ds.expected_counts.insert(event_type, count);
            }
            EventRecord::Meta { total } => ds.total = total,
        }
    }
    ds
}

pub fn load_json_dataset() -> JsonDataset {
    let records: Vec<JsonRecord> = read_jsonl("json_docs.jsonl");
    let mut ds = JsonDataset {
        documents: Vec::new(),
        path_queries: Vec::new(),
        mutations: Vec::new(),
        deletions: Vec::new(),
        prefixes: HashMap::new(),
    };
    for r in records {
        match r {
            JsonRecord::Document { key, doc } => ds.documents.push(JsonDoc { key, doc }),
            JsonRecord::PathQuery { key, path, expected } => {
                ds.path_queries.push(PathQuery { key, path, expected });
            }
            JsonRecord::Mutation { key, path, new_value } => {
                ds.mutations.push(PathMutation { key, path, new_value });
            }
            JsonRecord::Deletion { key, path } => ds.deletions.push(JsonDeletion { key, path }),
            JsonRecord::Prefix { prefix, count } => { ds.prefixes.insert(prefix, count); }
        }
    }
    ds
}

pub fn load_vector_dataset() -> VectorDataset {
    let records: Vec<VectorRecord> = read_jsonl("vectors.jsonl");
    let mut collections: HashMap<String, VectorCollection> = HashMap::new();
    let mut search_queries = Vec::new();

    for r in records {
        match r {
            VectorRecord::Collection { name, dimension, metric } => {
                collections.insert(
                    name.clone(),
                    VectorCollection { name, dimension, metric, vectors: Vec::new() },
                );
            }
            VectorRecord::Vector { collection, key, embedding, metadata } => {
                collections
                    .get_mut(&collection)
                    .unwrap_or_else(|| panic!("vector references unknown collection: {}", collection))
                    .vectors
                    .push(VectorEntry { key, embedding, metadata });
            }
            VectorRecord::SearchQuery { collection, query, k, description, expected_top } => {
                search_queries.push(SearchQuery { collection, query, k, description, expected_top });
            }
        }
    }

    // Preserve insertion order by sorting by name (alphabetical matches original order)
    let mut colls: Vec<VectorCollection> = collections.into_values().collect();
    colls.sort_by(|a, b| a.name.cmp(&b.name));

    VectorDataset { collections: colls, search_queries }
}

pub fn load_branch_dataset() -> BranchDataset {
    let records: Vec<BranchRecord> = read_jsonl("branches.jsonl");
    let mut branches = Vec::new();
    let mut per_branch_data: HashMap<String, BranchData> = HashMap::new();
    let mut isolation_checks = Vec::new();
    let mut cross_branch_comparison = None;

    for r in records {
        match r {
            BranchRecord::Branch { name } => {
                branches.push(name.clone());
                per_branch_data.entry(name).or_insert_with(|| BranchData {
                    kv: Vec::new(),
                    state: Vec::new(),
                    events: Vec::new(),
                });
            }
            BranchRecord::BranchKv { branch, key, value } => {
                per_branch_data
                    .entry(branch)
                    .or_insert_with(|| BranchData { kv: Vec::new(), state: Vec::new(), events: Vec::new() })
                    .kv
                    .push(KvEntry { key, value });
            }
            BranchRecord::BranchState { branch, cell, value } => {
                per_branch_data
                    .entry(branch)
                    .or_insert_with(|| BranchData { kv: Vec::new(), state: Vec::new(), events: Vec::new() })
                    .state
                    .push(StateCell { cell, value });
            }
            BranchRecord::BranchEvent { branch, event_type, payload } => {
                per_branch_data
                    .entry(branch)
                    .or_insert_with(|| BranchData { kv: Vec::new(), state: Vec::new(), events: Vec::new() })
                    .events
                    .push(EventEntry { event_type, payload });
            }
            BranchRecord::IsolationCheck {
                description, on_branch, key, expected_value, expected_event_count,
            } => {
                isolation_checks.push(IsolationCheck {
                    description, on_branch, key, expected_value, expected_event_count,
                });
            }
            BranchRecord::CrossBranchComparison { cell, expected, winner } => {
                cross_branch_comparison = Some(CrossBranchComparison { cell, expected, winner });
            }
        }
    }

    BranchDataset {
        branches,
        per_branch_data,
        isolation_checks,
        cross_branch_comparison: cross_branch_comparison.expect("missing cross_branch_comparison record"),
    }
}

pub fn load_dirty_dataset() -> DirtyDataset {
    let records: Vec<DirtyRecord> = read_jsonl("dirty.jsonl");
    let mut ds = DirtyDataset {
        kv_roundtrips: Vec::new(),
        kv_rejects: Vec::new(),
        state_roundtrips: Vec::new(),
        event_roundtrips: Vec::new(),
        json_roundtrips: Vec::new(),
        vector_roundtrips: Vec::new(),
        cross_kv_json: Vec::new(),
        cross_branch_dirty: Vec::new(),
    };
    for r in records {
        match r {
            DirtyRecord::Kv { desc, key, value } => {
                ds.kv_roundtrips.push(DirtyKv { desc, key, value });
            }
            DirtyRecord::KvReject { desc, key, value } => {
                ds.kv_rejects.push(DirtyKv { desc, key, value });
            }
            DirtyRecord::State { desc, cell, value } => {
                ds.state_roundtrips.push(DirtyState { desc, cell, value });
            }
            DirtyRecord::Event { desc, event_type, payload } => {
                ds.event_roundtrips.push(DirtyEvent { desc, event_type, payload });
            }
            DirtyRecord::Json { desc, key, doc } => {
                ds.json_roundtrips.push(DirtyJson { desc, key, doc });
            }
            DirtyRecord::Vector { desc, collection, key, embedding } => {
                ds.vector_roundtrips.push(DirtyVector { desc, collection, key, embedding });
            }
            DirtyRecord::CrossKvJson { desc, key, kv_value, json_doc } => {
                ds.cross_kv_json.push(DirtyCrossKvJson { desc, key, kv_value, json_doc });
            }
            DirtyRecord::CrossBranchDirty {
                desc, branch, key, kv_value, cell, state_value, event_type, event_payload,
            } => {
                ds.cross_branch_dirty.push(DirtyCrossBranch {
                    desc, branch, key, kv_value, cell, state_value, event_type, event_payload,
                });
            }
        }
    }
    ds
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
    Strata::cache().expect("failed to open temp db")
}
