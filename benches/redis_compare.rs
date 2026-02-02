//! Redis-Comparison Benchmark for StrataDB
//!
//! Runs the same operations as `redis-benchmark` (default suite) using Strata's
//! API, so results can be placed side-by-side for comparison.
//!
//! Run: `cargo bench --bench redis_compare`
//! Quick: `cargo bench --bench redis_compare -- --durability cache -q`
//! CSV:  `cargo bench --bench redis_compare -- --csv`

#[allow(unused)]
#[path = "harness/mod.rs"]
mod harness;

use harness::{create_db, print_hardware_info, DurabilityConfig};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use stratadb::{Command, Value};

// ---------------------------------------------------------------------------
// Parameters (matching redis-benchmark defaults)
// ---------------------------------------------------------------------------

const DEFAULT_REQUESTS: usize = 100_000;
const DEFAULT_PAYLOAD_SIZE: usize = 3;
const KEYSPACE_SIZE: u64 = 100_000;
const WARMUP_REQUESTS: usize = 1_000;
const INCR_CELLS: u64 = 1_000;
const HSET_DOCS: u64 = 100;

// ---------------------------------------------------------------------------
// Fast LCG RNG (same as scaling.rs)
// ---------------------------------------------------------------------------

struct LcgRng {
    state: u64,
}

impl LcgRng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    #[inline]
    fn next_u64(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.state >> 33
    }

    #[inline]
    fn rand_key(&mut self) -> String {
        let idx = self.next_u64() % KEYSPACE_SIZE;
        format!("key:{:012}", idx)
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

fn run_bench(
    name: &str,
    redis_equiv: &str,
    total_ops: usize,
    warmup_ops: usize,
    _payload_size: usize,
    mut bench_fn: impl FnMut(&mut LcgRng),
) -> BenchResult {
    let mut rng = LcgRng::new(0xdeadbeef);

    // Warmup
    for _ in 0..warmup_ops {
        bench_fn(&mut rng);
    }

    // Reset RNG for measurement
    rng = LcgRng::new(0xcafebabe);

    // Measure every operation
    let mut latencies = Vec::with_capacity(total_ops);
    let wall_start = Instant::now();

    for _ in 0..total_ops {
        let op_start = Instant::now();
        bench_fn(&mut rng);
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
// Test definitions
//
// Each test receives a DurabilityConfig and creates its own fresh database.
// This avoids cross-test contamination (e.g. prefix scans becoming slow due
// to hundreds of thousands of keys accumulated from prior tests).
// ---------------------------------------------------------------------------

fn bench_ping(mode: DurabilityConfig, n: usize, _payload_size: usize) -> BenchResult {
    let bench_db = create_db(mode);
    let db = &bench_db.db;
    run_bench("PING", "PING_INLINE", n, WARMUP_REQUESTS, _payload_size, |_rng| {
        db.ping().unwrap();
    })
}

fn bench_set(mode: DurabilityConfig, n: usize, payload_size: usize) -> BenchResult {
    let bench_db = create_db(mode);
    let db = &bench_db.db;
    let val = Value::Bytes(vec![0x78; payload_size]);
    run_bench("SET", "SET", n, WARMUP_REQUESTS, payload_size, |rng| {
        let key = rng.rand_key();
        db.kv_put(&key, val.clone()).unwrap();
    })
}

fn bench_get(mode: DurabilityConfig, n: usize, payload_size: usize) -> BenchResult {
    let bench_db = create_db(mode);
    let db = &bench_db.db;
    // Setup: pre-populate keys (scale with n to keep setup time proportional)
    let keyspace = (n as u64).min(KEYSPACE_SIZE);
    let val = Value::Bytes(vec![0x78; payload_size]);
    for i in 0..keyspace {
        db.kv_put(&format!("key:{:012}", i), val.clone()).unwrap();
    }

    run_bench("GET", "GET", n, WARMUP_REQUESTS, payload_size, |rng| {
        let idx = rng.next_u64() % keyspace;
        let key = format!("key:{:012}", idx);
        let _ = db.kv_get(&key).unwrap();
    })
}

fn bench_incr(mode: DurabilityConfig, n: usize, payload_size: usize) -> BenchResult {
    let bench_db = create_db(mode);
    let db = &bench_db.db;
    // Setup: initialize counter cells (scale with n)
    let cells = (n as u64).min(INCR_CELLS);
    for i in 0..cells {
        db.state_set(&format!("counter:{}", i), Value::Int(0))
            .unwrap();
    }

    run_bench("INCR", "INCR", n, WARMUP_REQUESTS, payload_size, |rng| {
        let idx = rng.next_u64() % cells;
        let cell = format!("counter:{}", idx);
        let current = db.state_read(&cell).unwrap();
        let val = match current {
            Some(Value::Int(v)) => v,
            _ => 0,
        };
        db.state_set(&cell, Value::Int(val + 1)).unwrap();
    })
}

fn bench_hset(mode: DurabilityConfig, n: usize, payload_size: usize) -> BenchResult {
    let bench_db = create_db(mode);
    let db = &bench_db.db;
    // Setup: pre-create hash documents as empty objects
    let empty = Value::Object(HashMap::new());
    for i in 0..HSET_DOCS {
        db.json_set(&format!("myhash:{}", i), "$", empty.clone())
            .unwrap();
    }

    let val = Value::Bytes(vec![0x78; payload_size]);
    run_bench("HSET", "HSET", n, WARMUP_REQUESTS, payload_size, |rng| {
        let doc_idx = rng.next_u64() % HSET_DOCS;
        let field_idx = rng.next_u64() % KEYSPACE_SIZE;
        let key = format!("myhash:{}", doc_idx);
        let path = format!("$.element_{}", field_idx);
        db.json_set(&key, &path, val.clone()).unwrap();
    })
}

fn bench_mset_10(mode: DurabilityConfig, n: usize, payload_size: usize) -> BenchResult {
    let bench_db = create_db(mode);
    let db = &bench_db.db;
    let val = Value::Bytes(vec![0x78; payload_size]);
    run_bench(
        "MSET (10 keys)",
        "MSET (10 keys)",
        n,
        WARMUP_REQUESTS,
        payload_size,
        |rng| {
            let mut session = db.session();
            session
                .execute(Command::TxnBegin {
                    branch: None,
                    options: None,
                })
                .unwrap();
            for _ in 0..10 {
                let key = rng.rand_key();
                session
                    .execute(Command::KvPut {
                        branch: None,
                        key,
                        value: val.clone(),
                    })
                    .unwrap();
            }
            session.execute(Command::TxnCommit).unwrap();
        },
    )
}

fn bench_xadd(mode: DurabilityConfig, n: usize, payload_size: usize) -> BenchResult {
    let bench_db = create_db(mode);
    let db = &bench_db.db;
    let mut payload_map = HashMap::new();
    payload_map.insert(
        "myfield".to_string(),
        Value::Bytes(vec![0x78; payload_size]),
    );
    let payload = Value::Object(payload_map);

    run_bench("XADD", "XADD", n, WARMUP_REQUESTS, payload_size, |_rng| {
        db.event_append("mystream", payload.clone()).unwrap();
    })
}

fn bench_lrange_100(mode: DurabilityConfig, n: usize, payload_size: usize) -> BenchResult {
    // Fresh database so kv_list only scans the 100 list keys, not 300K+ from prior tests
    let bench_db = create_db(mode);
    let db = &bench_db.db;
    // Setup: populate 100 keys with prefix
    let val = Value::Bytes(vec![0x78; payload_size]);
    for i in 0..100u64 {
        db.kv_put(&format!("listkey:{:06}", i), val.clone())
            .unwrap();
    }

    run_bench(
        "LRANGE_100",
        "LRANGE_100 (kv_list)",
        n,
        WARMUP_REQUESTS,
        payload_size,
        |_rng| {
            let _ = db.kv_list(Some("listkey:")).unwrap();
        },
    )
}

// --- Strata-unique bonus tests ---

fn bench_state_set(mode: DurabilityConfig, n: usize, payload_size: usize) -> BenchResult {
    let bench_db = create_db(mode);
    let db = &bench_db.db;
    let val = Value::Bytes(vec![0x78; payload_size]);
    run_bench(
        "STATE_SET",
        "(Strata unique)",
        n,
        WARMUP_REQUESTS,
        payload_size,
        |rng| {
            let idx = rng.next_u64() % INCR_CELLS;
            let cell = format!("cell:{}", idx);
            db.state_set(&cell, val.clone()).unwrap();
        },
    )
}

fn bench_state_read(mode: DurabilityConfig, n: usize, payload_size: usize) -> BenchResult {
    let bench_db = create_db(mode);
    let db = &bench_db.db;
    // Setup: populate cells (scale with n)
    let cells = (n as u64).min(INCR_CELLS);
    let val = Value::Bytes(vec![0x78; payload_size]);
    for i in 0..cells {
        db.state_set(&format!("rcell:{}", i), val.clone()).unwrap();
    }

    run_bench(
        "STATE_READ",
        "(Strata unique)",
        n,
        WARMUP_REQUESTS,
        payload_size,
        |rng| {
            let idx = rng.next_u64() % cells;
            let cell = format!("rcell:{}", idx);
            let _ = db.state_read(&cell).unwrap();
        },
    )
}

fn bench_event_read(mode: DurabilityConfig, n: usize, payload_size: usize) -> BenchResult {
    let bench_db = create_db(mode);
    let db = &bench_db.db;
    // Setup: append events to read back (scale with n to keep setup proportional)
    let mut payload_map = HashMap::new();
    payload_map.insert(
        "data".to_string(),
        Value::Bytes(vec![0x78; payload_size]),
    );
    let payload = Value::Object(payload_map);
    let event_count = (n as u64).min(10_000);
    for _ in 0..event_count {
        db.event_append("readstream", payload.clone()).unwrap();
    }

    run_bench(
        "EVENT_READ",
        "(Strata unique)",
        n,
        WARMUP_REQUESTS,
        payload_size,
        |rng| {
            let seq = (rng.next_u64() % event_count) + 1;
            let _ = db.event_read(seq).unwrap();
        },
    )
}

fn bench_kv_delete(mode: DurabilityConfig, n: usize, payload_size: usize) -> BenchResult {
    let bench_db = create_db(mode);
    let db = &bench_db.db;
    // Setup: pre-populate keys (scale with n to keep setup proportional)
    let keyspace = (n as u64).min(KEYSPACE_SIZE);
    let val = Value::Bytes(vec![0x78; payload_size]);
    for i in 0..keyspace {
        db.kv_put(&format!("dkey:{:012}", i), val.clone()).unwrap();
    }

    run_bench(
        "KV_DELETE",
        "DEL (bonus)",
        n,
        WARMUP_REQUESTS,
        payload_size,
        |rng| {
            let idx = rng.next_u64() % keyspace;
            let key = format!("dkey:{:012}", idx);
            let _ = db.kv_delete(&key).unwrap();
        },
    )
}

// ---------------------------------------------------------------------------
// Test registry
// ---------------------------------------------------------------------------

struct TestDef {
    name: &'static str,
    run: fn(DurabilityConfig, usize, usize) -> BenchResult,
}

const ALL_TESTS: &[TestDef] = &[
    TestDef { name: "PING", run: bench_ping },
    TestDef { name: "SET", run: bench_set },
    TestDef { name: "GET", run: bench_get },
    TestDef { name: "INCR", run: bench_incr },
    TestDef { name: "HSET", run: bench_hset },
    TestDef { name: "MSET", run: bench_mset_10 },
    TestDef { name: "XADD", run: bench_xadd },
    TestDef { name: "LRANGE_100", run: bench_lrange_100 },
    TestDef { name: "STATE_SET", run: bench_state_set },
    TestDef { name: "STATE_READ", run: bench_state_read },
    TestDef { name: "EVENT_READ", run: bench_event_read },
    TestDef { name: "KV_DELETE", run: bench_kv_delete },
];

const SKIPPED_REDIS_TESTS: &[&str] = &[
    "LPUSH", "RPUSH", "LPOP", "RPOP", "SADD", "SPOP",
    "LRANGE_300", "LRANGE_500", "LRANGE_600", "ZADD", "ZPOPMIN",
];

// ---------------------------------------------------------------------------
// CLI parsing
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Config {
    requests: usize,
    payload_size: usize,
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
// Main
// ---------------------------------------------------------------------------

fn main() {
    let config = parse_args();
    print_hardware_info();

    if !config.csv {
        eprintln!("=== StrataDB Redis-Comparison Benchmark ===");
        eprintln!("NOTE: Not an apples-to-apples comparison.");
        eprintln!("- Strata is embedded (no network overhead, no serialization)");
        eprintln!("- Redis is client-server (TCP roundtrip, RESP protocol encoding)");
        eprintln!("- Compare to redis-benchmark run on the same hardware");
        eprintln!();
        eprintln!(
            "Parameters: {} requests, {} bytes payload, keyspace {}",
            config.requests, config.payload_size, KEYSPACE_SIZE
        );
        eprintln!();
    }

    if config.csv {
        print_csv_header();
    }

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

        for test in ALL_TESTS {
            // Filter tests if -t was specified
            if let Some(ref filter) = config.tests {
                if !filter.iter().any(|f| test.name.starts_with(f.as_str())) {
                    continue;
                }
            }

            let result = (test.run)(*mode, config.requests, config.payload_size);

            if config.csv {
                print_csv_row(&result);
            } else if config.quiet {
                print_quiet(&result);
            } else {
                print_verbose(&result, config.payload_size);
            }
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
}
