//! Redis-Comparison Benchmark for StrataDB
//!
//! Runs the same operations as `redis-benchmark` (default suite) using Strata's
//! API, so results can be placed side-by-side for comparison.
//!
//! By default, matches redis-benchmark's default behavior (no key randomization,
//! all operations hit the same key). Use `-r <keyspace>` to enable random keys
//! (equivalent to `redis-benchmark -r <keyspace>`).
//!
//! Run: `cargo bench --bench redis_compare`
//! Random keys: `cargo bench --bench redis_compare -- -r 100000`
//! Quick: `cargo bench --bench redis_compare -- --durability cache -q`
//! CSV:  `cargo bench --bench redis_compare -- --csv`

#[allow(unused)]
#[path = "../harness/mod.rs"]
mod harness;

use harness::recorder::ResultRecorder;
use harness::{create_db, print_hardware_info, BenchDb, DurabilityConfig};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use strata_benchmarks::schema::{BenchmarkMetrics, BenchmarkResult};
use stratadb::{Command, Value};

// ---------------------------------------------------------------------------
// Parameters (matching redis-benchmark defaults)
// ---------------------------------------------------------------------------

const DEFAULT_REQUESTS: usize = 100_000;
const DEFAULT_PAYLOAD_SIZE: usize = 3;

// ---------------------------------------------------------------------------
// Random data generator (matching redis-benchmark's genBenchmarkRandomData)
// ---------------------------------------------------------------------------

/// Generate random data matching redis-benchmark's genBenchmarkRandomData.
/// Uses the same LCG: state = state * 1103515245 + 12345, output '0'+((state>>16)&63).
fn gen_benchmark_random_data(count: usize) -> Vec<u8> {
    let mut state: u32 = 1234;
    let mut data = Vec::with_capacity(count);
    for _ in 0..count {
        state = state.wrapping_mul(1103515245).wrapping_add(12345);
        data.push(b'0' + ((state >> 16) & 63) as u8);
    }
    data
}

// ---------------------------------------------------------------------------
// Key generation (matching redis-benchmark's randomizeClientKey)
// ---------------------------------------------------------------------------

/// Redis key format: "key:NNNNNNNNNNNN" where N is a 12-digit zero-padded number.
/// When keyspace=0 (default), all operations hit the fixed key "key:000000000000".
/// When keyspace>0, keys are randomized in range [0, keyspace).
///
/// This matches redis-benchmark's __rand_int__ substitution which writes a
/// 12-digit number into the command buffer (see randomizeClientKey, line 377).
struct KeyGen {
    keyspace: u64,
    /// Simple LCG matching libc random() used by redis-benchmark.
    rng_state: u64,
}

impl KeyGen {
    fn new(keyspace: u64) -> Self {
        Self {
            keyspace,
            rng_state: 0xdeadbeef,
        }
    }

    #[inline]
    fn next_rand(&mut self) -> u64 {
        self.rng_state = self
            .rng_state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.rng_state >> 33
    }

    /// Generate a key like redis-benchmark's "key:__rand_int__" pattern.
    /// Without -r: always returns "key:000000000000" (same key every time).
    /// With -r N: returns "key:NNNNNNNNNNNN" with N in [0, keyspace).
    #[inline]
    fn key(&mut self, prefix: &str) -> String {
        if self.keyspace == 0 {
            format!("{}:000000000000", prefix)
        } else {
            let idx = self.next_rand() % self.keyspace;
            format!("{}:{:012}", prefix, idx)
        }
    }
}

// ---------------------------------------------------------------------------
// Benchmark result
// ---------------------------------------------------------------------------

struct BenchResult {
    name: String,
    redis_equiv: String,
    total_ops: usize,
    elapsed: Duration,
    ops_per_sec: f64,
    avg_latency: Duration,
    p50: Duration,
    p95: Duration,
    p99: Duration,
    min: Duration,
    max: Duration,
}

// ---------------------------------------------------------------------------
// Core measurement function
// ---------------------------------------------------------------------------

/// Run a benchmark. No warmup phase — matches redis-benchmark which starts
/// timing immediately (see benchmark() at line 946).
fn run_bench(
    name: &str,
    redis_equiv: &str,
    total_ops: usize,
    mut bench_fn: impl FnMut(&mut KeyGen),
    keygen: &mut KeyGen,
) -> BenchResult {
    // Measure every operation
    let mut latencies = Vec::with_capacity(total_ops);
    let wall_start = Instant::now();

    for _ in 0..total_ops {
        let op_start = Instant::now();
        bench_fn(keygen);
        latencies.push(op_start.elapsed());
    }

    let elapsed = wall_start.elapsed();

    // Compute statistics
    latencies.sort_unstable();
    let len = latencies.len();
    let sum: Duration = latencies.iter().sum();

    BenchResult {
        name: name.to_string(),
        redis_equiv: redis_equiv.to_string(),
        total_ops: len,
        elapsed,
        ops_per_sec: len as f64 / elapsed.as_secs_f64(),
        avg_latency: sum / len as u32,
        p50: latencies[len * 50 / 100],
        p95: latencies[(len * 95 / 100).min(len - 1)],
        p99: latencies[(len * 99 / 100).min(len - 1)],
        min: latencies[0],
        max: latencies[len - 1],
    }
}

// ---------------------------------------------------------------------------
// Output formatters
// ---------------------------------------------------------------------------

fn duration_ms(d: Duration) -> f64 {
    d.as_nanos() as f64 / 1_000_000.0
}

fn print_verbose(r: &BenchResult, payload_size: usize) {
    eprintln!("====== {} ======", r.name);
    if !r.redis_equiv.is_empty() {
        eprintln!("  redis equivalent: {}", r.redis_equiv);
    }
    eprintln!(
        "  {} requests completed in {:.2} seconds",
        r.total_ops,
        r.elapsed.as_secs_f64()
    );
    eprintln!("  1 parallel client (embedded, no network)");
    eprintln!("  {} bytes payload", payload_size);
    eprintln!();
    eprintln!(
        "  throughput summary: {:.2} requests per second",
        r.ops_per_sec
    );
    eprintln!("  latency summary (msec):");
    eprintln!(
        "          avg       min       p50       p95       p99       max"
    );
    eprintln!(
        "      {:>8.3}  {:>8.3}  {:>8.3}  {:>8.3}  {:>8.3}  {:>8.3}",
        duration_ms(r.avg_latency),
        duration_ms(r.min),
        duration_ms(r.p50),
        duration_ms(r.p95),
        duration_ms(r.p99),
        duration_ms(r.max),
    );
    eprintln!();
}

fn print_quiet(r: &BenchResult) {
    eprintln!(
        "{}: {:.2} requests per second, p50={:.3} msec",
        r.name,
        r.ops_per_sec,
        duration_ms(r.p50),
    );
}

fn print_csv_header() {
    println!(
        "\"test\",\"rps\",\"avg_latency_ms\",\"min_latency_ms\",\"p50_latency_ms\",\"p95_latency_ms\",\"p99_latency_ms\",\"max_latency_ms\""
    );
}

fn print_csv_row(r: &BenchResult) {
    println!(
        "\"{}\",{:.2},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3}",
        r.name,
        r.ops_per_sec,
        duration_ms(r.avg_latency),
        duration_ms(r.min),
        duration_ms(r.p50),
        duration_ms(r.p95),
        duration_ms(r.p99),
        duration_ms(r.max),
    );
}

// ---------------------------------------------------------------------------
// Recording helper
// ---------------------------------------------------------------------------

fn record_redis_result(recorder: &mut ResultRecorder, r: &BenchResult, mode: &DurabilityConfig) {
    let mut params = HashMap::new();
    params.insert("durability".into(), serde_json::json!(mode.label()));
    params.insert("redis_equiv".into(), serde_json::json!(r.redis_equiv));

    recorder.record(BenchmarkResult {
        benchmark: format!("redis-compare/{}/{}", r.name, mode.label()),
        category: "redis-compare".to_string(),
        parameters: params,
        metrics: BenchmarkMetrics {
            ops_per_sec: Some(r.ops_per_sec),
            p50_ns: Some(r.p50.as_nanos() as u64),
            p95_ns: Some(r.p95.as_nanos() as u64),
            p99_ns: Some(r.p99.as_nanos() as u64),
            min_ns: Some(r.min.as_nanos() as u64),
            max_ns: Some(r.max.as_nanos() as u64),
            avg_ns: Some(r.avg_latency.as_nanos() as u64),
            samples: Some(r.total_ops as u64),
            ..Default::default()
        },
    });
}

// ---------------------------------------------------------------------------
// Test definitions
//
// Each test matches the exact redis-benchmark default command.
// Tests share the same database within a durability mode, just like
// redis-benchmark shares the same Redis instance across all tests.
// LRANGE_100 is the exception — it uses a fresh database because
// kv_list prefix scan degrades with unrelated keys.
// ---------------------------------------------------------------------------

/// PING_INLINE: "PING\r\n" (redis-benchmark.c line 1880)
fn bench_ping(db: &BenchDb, n: usize, keygen: &mut KeyGen) -> BenchResult {
    run_bench("PING_INLINE", "PING_INLINE", n, |_kg| {
        db.db.ping().unwrap();
    }, keygen)
}

/// SET: "SET key:__rand_int__ <data>" (redis-benchmark.c line 1889)
/// Without -r: all writes go to the same key (hot-key benchmark).
fn bench_set(db: &BenchDb, n: usize, data: &Value, keygen: &mut KeyGen) -> BenchResult {
    run_bench("SET", "SET", n, |kg| {
        let key = kg.key("key");
        db.db.kv_put(&key, data.clone()).unwrap();
    }, keygen)
}

/// GET: "GET key:__rand_int__" (redis-benchmark.c line 1895)
/// In redis-benchmark, GET runs after SET so the key already exists.
/// Without -r: reads the same key SET wrote.
fn bench_get(db: &BenchDb, n: usize, keygen: &mut KeyGen) -> BenchResult {
    run_bench("GET", "GET", n, |kg| {
        let key = kg.key("key");
        let _ = db.db.kv_get(&key);
    }, keygen)
}

/// INCR: "INCR counter:__rand_int__" (redis-benchmark.c line 1901)
/// Redis INCR is a single atomic O(1) command.
/// Strata equivalent requires state_read + state_set (2 operations).
fn bench_incr(db: &BenchDb, n: usize, keygen: &mut KeyGen) -> BenchResult {
    run_bench("INCR", "INCR (state_read+state_set)", n, |kg| {
        let cell = kg.key("counter");
        let current = db.db.state_get(&cell).unwrap();
        let val = match current {
            Some(Value::Int(v)) => v,
            _ => 0,
        };
        db.db.state_set(&cell, Value::Int(val + 1)).unwrap();
    }, keygen)
}

/// HSET: "HSET myhash element:__rand_int__ <data>" (redis-benchmark.c line 1938)
/// Redis HSET is O(1) hash field set. Strata has no native hash type.
/// We use kv_put with composite key "myhash:element:X" which is the closest
/// in terms of cost/complexity to Redis HSET.
fn bench_hset(db: &BenchDb, n: usize, data: &Value, keygen: &mut KeyGen) -> BenchResult {
    run_bench("HSET", "HSET (kv_put composite key)", n, |kg| {
        let key = kg.key("myhash:element");
        db.db.kv_put(&key, data.clone()).unwrap();
    }, keygen)
}

/// MSET (10 keys): "MSET key:__rand_int__ <data>" x10 (redis-benchmark.c line 2000)
/// Redis MSET is a single atomic command. Without -r, all 10 keys are the same.
/// Strata equivalent uses Session + TxnBegin + 10x KvPut + TxnCommit.
fn bench_mset_10(db: &BenchDb, n: usize, data: &Value, keygen: &mut KeyGen) -> BenchResult {
    run_bench("MSET (10 keys)", "MSET (10 keys) via txn", n, |kg| {
        let mut session = db.db.session();
        session
            .execute(Command::TxnBegin {
                branch: None,
                options: None,
            })
            .unwrap();
        for _ in 0..10 {
            let key = kg.key("key");
            session
                .execute(Command::KvPut {
                    branch: None,
                    key,
                    value: data.clone(),
                })
                .unwrap();
        }
        session.execute(Command::TxnCommit).unwrap();
    }, keygen)
}

/// XADD: "XADD mystream * myfield <data>" (redis-benchmark.c line 2015)
/// Stream append with auto-generated ID. This is a close match.
fn bench_xadd(db: &BenchDb, n: usize, data: &Value, keygen: &mut KeyGen) -> BenchResult {
    let mut payload_map = HashMap::new();
    payload_map.insert("myfield".to_string(), data.clone());
    let payload = Value::Object(payload_map);

    run_bench("XADD", "XADD", n, |_kg| {
        db.db.event_append("mystream", payload.clone()).unwrap();
    }, keygen)
}

/// LRANGE_100: "LRANGE mylist 0 99" (redis-benchmark.c line 1977)
/// Redis: indexed list access on a single pre-filled list, O(S+N).
/// Strata: kv_list prefix scan returning 100 keys. NOT equivalent —
/// kv_list scans the key namespace, not an indexed list.
/// Uses a fresh database to avoid scanning unrelated keys.
fn bench_lrange_100(mode: DurabilityConfig, n: usize, data: &Value, keygen: &mut KeyGen) -> BenchResult {
    let bench_db = create_db(mode);
    // Pre-populate 100 keys to scan (analogous to LPUSH filling the list)
    for i in 0..100u64 {
        bench_db
            .db
            .kv_put(&format!("mylist:{:06}", i), data.clone())
            .unwrap();
    }

    run_bench(
        "LRANGE_100 (first 100 elements)",
        "LRANGE_100 (kv_list prefix scan — NOT equivalent)",
        n,
        |_kg| {
            let _ = bench_db.db.kv_list(Some("mylist:")).unwrap();
        },
        keygen,
    )
}

// --- Strata-unique bonus tests ---

fn bench_state_set(db: &BenchDb, n: usize, data: &Value, keygen: &mut KeyGen) -> BenchResult {
    run_bench("STATE_SET", "(Strata unique)", n, |kg| {
        let cell = kg.key("cell");
        db.db.state_set(&cell, data.clone()).unwrap();
    }, keygen)
}

fn bench_state_read(db: &BenchDb, n: usize, keygen: &mut KeyGen) -> BenchResult {
    // Pre-populate one cell so reads return data
    db.db
        .state_set("rcell:000000000000", Value::Int(42))
        .unwrap();

    run_bench("STATE_READ", "(Strata unique)", n, |kg| {
        let cell = kg.key("rcell");
        let _ = db.db.state_get(&cell).unwrap();
    }, keygen)
}

fn bench_event_read(db: &BenchDb, n: usize, keygen: &mut KeyGen) -> BenchResult {
    // Pre-populate events to read back (scale with n)
    let event_count = (n as u64).min(10_000).max(1);
    let payload = Value::Object(HashMap::from([(
        "data".to_string(),
        Value::Int(0),
    )]));
    for _ in 0..event_count {
        db.db.event_append("readstream", payload.clone()).unwrap();
    }

    run_bench("EVENT_READ", "(Strata unique)", n, |kg| {
        let seq = (kg.next_rand() % event_count) + 1;
        let _ = db.db.event_get(seq).unwrap();
    }, keygen)
}

fn bench_kv_delete(db: &BenchDb, n: usize, data: &Value, keygen: &mut KeyGen) -> BenchResult {
    // Pre-populate keys to delete (scale with n)
    let keyspace = (n as u64).min(100_000).max(1);
    for i in 0..keyspace {
        db.db
            .kv_put(&format!("dkey:{:012}", i), data.clone())
            .unwrap();
    }

    run_bench("KV_DELETE", "DEL (bonus)", n, |kg| {
        if kg.keyspace == 0 {
            let _ = db.db.kv_delete("dkey:000000000000");
        } else {
            let idx = kg.next_rand() % keyspace;
            let key = format!("dkey:{:012}", idx);
            let _ = db.db.kv_delete(&key);
        }
    }, keygen)
}

const SKIPPED_REDIS_TESTS: &[&str] = &[
    "PING_MBULK", "LPUSH", "RPUSH", "LPOP", "RPOP", "SADD", "SPOP",
    "LRANGE_300", "LRANGE_500", "LRANGE_600", "ZADD", "ZPOPMIN",
];

// ---------------------------------------------------------------------------
// CLI parsing
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Config {
    requests: usize,
    payload_size: usize,
    keyspace: u64,
    durability: Vec<DurabilityConfig>,
    tests: Option<Vec<String>>,
    csv: bool,
    quiet: bool,
}

fn parse_args() -> Config {
    let args: Vec<String> = std::env::args().collect();
    let mut config = Config {
        requests: DEFAULT_REQUESTS,
        payload_size: DEFAULT_PAYLOAD_SIZE,
        keyspace: 0, // default: no randomization, same key every time (matches redis-benchmark)
        durability: DurabilityConfig::ALL.to_vec(),
        tests: None,
        csv: false,
        quiet: false,
    };

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-n" => {
                i += 1;
                config.requests = args[i].parse().unwrap_or(DEFAULT_REQUESTS);
            }
            "-d" => {
                i += 1;
                config.payload_size = args[i].parse().unwrap_or(DEFAULT_PAYLOAD_SIZE);
            }
            "-r" => {
                i += 1;
                config.keyspace = args[i].parse().unwrap_or(0);
            }
            "--durability" => {
                i += 1;
                config.durability = match args[i].as_str() {
                    "cache" => vec![DurabilityConfig::Cache],
                    "standard" => vec![DurabilityConfig::Standard],
                    "always" => vec![DurabilityConfig::Always],
                    _ => DurabilityConfig::ALL.to_vec(),
                };
            }
            "-t" => {
                i += 1;
                let names: Vec<String> = args[i]
                    .split(',')
                    .map(|s| s.trim().to_uppercase())
                    .collect();
                config.tests = Some(names);
            }
            "--csv" => config.csv = true,
            "-q" => config.quiet = true,
            _ => {}
        }
        i += 1;
    }

    config
}

// ---------------------------------------------------------------------------
// Test filter
// ---------------------------------------------------------------------------

fn test_is_selected(name: &str, filter: &Option<Vec<String>>) -> bool {
    match filter {
        None => true,
        Some(names) => names.iter().any(|f| name.to_uppercase().starts_with(&f.to_uppercase())),
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    let config = parse_args();
    print_hardware_info();

    // Generate random payload data matching redis-benchmark's genBenchmarkRandomData
    let data_bytes = gen_benchmark_random_data(config.payload_size);
    let data = Value::Bytes(data_bytes);

    if !config.csv {
        eprintln!("=== StrataDB Redis-Comparison Benchmark ===");
        eprintln!("NOTE: Not an apples-to-apples comparison.");
        eprintln!("- Strata is embedded (no network overhead, no serialization)");
        eprintln!("- Redis is client-server (TCP roundtrip, RESP protocol encoding)");
        eprintln!("- Compare to redis-benchmark run on the same hardware");
        eprintln!();
        if config.keyspace == 0 {
            eprintln!(
                "Parameters: {} requests, {} bytes payload, no key randomization (same key)",
                config.requests, config.payload_size
            );
            eprintln!("  (use -r <keyspace> to enable random keys, e.g. -r 100000)");
        } else {
            eprintln!(
                "Parameters: {} requests, {} bytes payload, keyspace {} (random keys)",
                config.requests, config.payload_size, config.keyspace
            );
        }
        eprintln!();
    }

    if config.csv {
        print_csv_header();
    }

    let mut recorder = ResultRecorder::new("redis-compare");

    for mode in &config.durability {
        if !config.csv {
            let redis_equiv = match mode {
                DurabilityConfig::Cache => "Redis no persistence (save \"\", appendonly no)",
                DurabilityConfig::Standard => "Redis appendfsync everysec (default)",
                DurabilityConfig::Always => "Redis appendfsync always",
            };
            eprintln!(
                "--- durability: {} (comparable to: {}) ---",
                mode.label(),
                redis_equiv
            );
            eprintln!();
        }

        // Shared database for all tests in this durability mode
        // (matches Redis where all tests share the same instance)
        let bench_db = create_db(*mode);

        // --- Redis-equivalent tests (in redis-benchmark's exact order) ---

        if test_is_selected("PING", &config.tests) {
            let mut kg = KeyGen::new(config.keyspace);
            let result = bench_ping(&bench_db, config.requests, &mut kg);
            print_result(&result, &config);
            record_redis_result(&mut recorder, &result, mode);
        }

        if test_is_selected("SET", &config.tests) {
            let mut kg = KeyGen::new(config.keyspace);
            let result = bench_set(&bench_db, config.requests, &data, &mut kg);
            print_result(&result, &config);
            record_redis_result(&mut recorder, &result, mode);
        }

        if test_is_selected("GET", &config.tests) {
            let mut kg = KeyGen::new(config.keyspace);
            let result = bench_get(&bench_db, config.requests, &mut kg);
            print_result(&result, &config);
            record_redis_result(&mut recorder, &result, mode);
        }

        if test_is_selected("INCR", &config.tests) {
            let mut kg = KeyGen::new(config.keyspace);
            let result = bench_incr(&bench_db, config.requests, &mut kg);
            print_result(&result, &config);
            record_redis_result(&mut recorder, &result, mode);
        }

        if test_is_selected("HSET", &config.tests) {
            let mut kg = KeyGen::new(config.keyspace);
            let result = bench_hset(&bench_db, config.requests, &data, &mut kg);
            print_result(&result, &config);
            record_redis_result(&mut recorder, &result, mode);
        }

        if test_is_selected("MSET", &config.tests) {
            let mut kg = KeyGen::new(config.keyspace);
            let result = bench_mset_10(&bench_db, config.requests, &data, &mut kg);
            print_result(&result, &config);
            record_redis_result(&mut recorder, &result, mode);
        }

        if test_is_selected("XADD", &config.tests) {
            let mut kg = KeyGen::new(config.keyspace);
            let result = bench_xadd(&bench_db, config.requests, &data, &mut kg);
            print_result(&result, &config);
            record_redis_result(&mut recorder, &result, mode);
        }

        if test_is_selected("LRANGE", &config.tests) {
            let mut kg = KeyGen::new(config.keyspace);
            let result = bench_lrange_100(*mode, config.requests, &data, &mut kg);
            print_result(&result, &config);
            record_redis_result(&mut recorder, &result, mode);
        }

        // --- Strata-unique bonus tests ---

        if test_is_selected("STATE_SET", &config.tests) {
            let mut kg = KeyGen::new(config.keyspace);
            let result = bench_state_set(&bench_db, config.requests, &data, &mut kg);
            print_result(&result, &config);
            record_redis_result(&mut recorder, &result, mode);
        }

        if test_is_selected("STATE_READ", &config.tests) {
            let mut kg = KeyGen::new(config.keyspace);
            let result = bench_state_read(&bench_db, config.requests, &mut kg);
            print_result(&result, &config);
            record_redis_result(&mut recorder, &result, mode);
        }

        if test_is_selected("EVENT_READ", &config.tests) {
            let mut kg = KeyGen::new(config.keyspace);
            let result = bench_event_read(&bench_db, config.requests, &mut kg);
            print_result(&result, &config);
            record_redis_result(&mut recorder, &result, mode);
        }

        if test_is_selected("KV_DELETE", &config.tests) {
            let mut kg = KeyGen::new(config.keyspace);
            let result = bench_kv_delete(&bench_db, config.requests, &data, &mut kg);
            print_result(&result, &config);
            record_redis_result(&mut recorder, &result, mode);
        }

        // List skipped Redis tests
        if !config.csv && !config.quiet {
            eprintln!("--- Skipped (no Strata equivalent) ---");
            for name in SKIPPED_REDIS_TESTS {
                eprintln!("  {}: N/A", name);
            }
            eprintln!();
        }
    }

    if !config.csv {
        eprintln!("=== Benchmark complete ===");
    }
    let _ = recorder.save();
}

fn print_result(result: &BenchResult, config: &Config) {
    if config.csv {
        print_csv_row(result);
    } else if config.quiet {
        print_quiet(result);
    } else {
        print_verbose(result, config.payload_size);
    }
}
