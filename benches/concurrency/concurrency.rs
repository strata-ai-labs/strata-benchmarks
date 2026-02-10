//! Scaling & Concurrency Benchmark Suite
//!
//! Multi-threaded scaling benchmarks that measure throughput as a function of
//! thread count. Separate from the Criterion-based single-thread benchmarks.
//!
//! Run: `cargo bench --bench concurrency`
//! Quick: `cargo bench --bench concurrency -- --threads 1,2,4`

#[allow(unused)]
#[path = "../harness/mod.rs"]
mod harness;

use harness::recorder::ResultRecorder;
use harness::scaling::{
    parse_thread_counts, physical_cores, print_table_header, print_table_row,
    run_scaling_experiment, ReservoirSampler, ScalingResult, ThreadResult,
};
use harness::{create_db, DurabilityConfig};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::Instant;
use strata_benchmarks::schema::{BenchmarkMetrics, BenchmarkResult};
use stratadb::Value;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const WARMUP_SECS: u64 = 1;
const MEASURE_SECS: u64 = 5;

/// Number of keys to pre-populate for read-heavy workloads.
const PREPOPULATE_KEYS: usize = 100_000;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Simple LCG for key selection (fast, deterministic, no rand dependency).
#[inline]
fn fast_rand(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *state >> 33
}

fn record_scaling_result(
    recorder: &mut ResultRecorder,
    workload: &str,
    mode: &DurabilityConfig,
    result: &ScalingResult,
) {
    let mut params = HashMap::new();
    params.insert("durability".into(), serde_json::json!(mode.label()));
    params.insert("workload".into(), serde_json::json!(workload));

    recorder.record(BenchmarkResult {
        benchmark: format!("concurrency/{}/{}/{}t", workload, mode.label(), result.threads),
        category: "concurrency".to_string(),
        parameters: params,
        metrics: BenchmarkMetrics {
            ops_per_sec: Some(result.ops_per_sec),
            p50_ns: Some(result.p50.as_nanos() as u64),
            p95_ns: Some(result.p95.as_nanos() as u64),
            p99_ns: Some(result.p99.as_nanos() as u64),
            samples: Some(result.total_ops),
            threads: Some(result.threads),
            abort_rate_pct: if result.abort_rate_pct > 0.0 {
                Some(result.abort_rate_pct)
            } else {
                None
            },
            ..Default::default()
        },
    });
}

// ---------------------------------------------------------------------------
// Workload: KV GET (read-only, no contention)
// ---------------------------------------------------------------------------

fn run_kv_get_scaling(
    thread_sweep: &[usize],
    mode: DurabilityConfig,
    recorder: &mut ResultRecorder,
) {
    eprintln!(
        "\n=== KV GET (read-only, no contention) | durability: {} ===",
        mode.label()
    );

    let bench_db = create_db(mode);

    // Pre-populate keys
    eprint!("  Pre-populating {} keys...", PREPOPULATE_KEYS);
    for i in 0..PREPOPULATE_KEYS {
        bench_db
            .db
            .kv_put(&format!("key{:06}", i), Value::Int(i as i64))
            .expect("pre-populate failed");
    }
    eprintln!(" done.");

    print_table_header();

    for &n in thread_sweep {
        let result =
            run_scaling_experiment(&bench_db.db, n, WARMUP_SECS, MEASURE_SECS, move |tid, strata, stop| {
                let mut sampler = ReservoirSampler::with_seed(tid as u64);
                let mut ops = 0u64;
                let mut rng = tid as u64 ^ 0x12345678;

                while !stop.load(Ordering::Relaxed) {
                    let idx = fast_rand(&mut rng) % PREPOPULATE_KEYS as u64;
                    let key = format!("key{:06}", idx);

                    let start = Instant::now();
                    let _ = strata.kv_get(&key);
                    sampler.record(start.elapsed());
                    ops += 1;
                }

                ThreadResult {
                    ops,
                    aborts: 0,
                    latencies: sampler.into_samples(),
                }
            });
        print_table_row(&result);
        record_scaling_result(recorder, "kv_get", &mode, &result);
    }
}

// ---------------------------------------------------------------------------
// Workload: KV PUT (independent keys, no contention)
// ---------------------------------------------------------------------------

fn run_kv_put_independent_scaling(
    thread_sweep: &[usize],
    mode: DurabilityConfig,
    recorder: &mut ResultRecorder,
) {
    eprintln!(
        "\n=== KV PUT (independent keys, no contention) | durability: {} ===",
        mode.label()
    );

    print_table_header();

    for &n in thread_sweep {
        // Fresh database per thread count to avoid accumulation effects
        let bench_db = create_db(mode);
        let result =
            run_scaling_experiment(&bench_db.db, n, WARMUP_SECS, MEASURE_SECS, move |tid, strata, stop| {
                let mut sampler = ReservoirSampler::with_seed(tid as u64);
                let mut ops = 0u64;
                let mut seq = 0u64;

                while !stop.load(Ordering::Relaxed) {
                    let key = format!("t{}_{}", tid, seq);
                    seq += 1;

                    let start = Instant::now();
                    let _ = strata.kv_put(&key, Value::Int(seq as i64));
                    sampler.record(start.elapsed());
                    ops += 1;
                }

                ThreadResult {
                    ops,
                    aborts: 0,
                    latencies: sampler.into_samples(),
                }
            });
        print_table_row(&result);
        record_scaling_result(recorder, "kv_put_independent", &mode, &result);
    }
}

// ---------------------------------------------------------------------------
// Workload: KV PUT (hot key, maximum contention)
// ---------------------------------------------------------------------------

fn run_kv_put_hot_scaling(
    thread_sweep: &[usize],
    mode: DurabilityConfig,
    recorder: &mut ResultRecorder,
) {
    eprintln!(
        "\n=== KV PUT (hot key, maximum contention) | durability: {} ===",
        mode.label()
    );

    print_table_header();

    for &n in thread_sweep {
        let bench_db = create_db(mode);

        // Pre-populate the hot key
        bench_db
            .db
            .kv_put("hot", Value::Int(0))
            .expect("pre-populate hot key failed");

        let result =
            run_scaling_experiment(&bench_db.db, n, WARMUP_SECS, MEASURE_SECS, move |tid, strata, stop| {
                let mut sampler = ReservoirSampler::with_seed(tid as u64);
                let mut ops = 0u64;
                let mut aborts = 0u64;
                let mut seq = 0u64;

                while !stop.load(Ordering::Relaxed) {
                    seq += 1;
                    let start = Instant::now();
                    match strata.kv_put("hot", Value::Int(seq as i64)) {
                        Ok(_) => {
                            sampler.record(start.elapsed());
                            ops += 1;
                        }
                        Err(_) => {
                            aborts += 1;
                        }
                    }
                }

                ThreadResult {
                    ops,
                    aborts,
                    latencies: sampler.into_samples(),
                }
            });
        print_table_row(&result);
        record_scaling_result(recorder, "kv_put_hot", &mode, &result);
    }
}

// ---------------------------------------------------------------------------
// Workload: Mixed 90/10 (90% get, 10% put, low contention)
// ---------------------------------------------------------------------------

fn run_mixed_90_10_scaling(
    thread_sweep: &[usize],
    mode: DurabilityConfig,
    recorder: &mut ResultRecorder,
) {
    eprintln!(
        "\n=== MIXED 90/10 (90% get, 10% put, low contention) | durability: {} ===",
        mode.label()
    );

    let bench_db = create_db(mode);

    // Pre-populate keys
    eprint!("  Pre-populating {} keys...", PREPOPULATE_KEYS);
    for i in 0..PREPOPULATE_KEYS {
        bench_db
            .db
            .kv_put(&format!("key{:06}", i), Value::Int(i as i64))
            .expect("pre-populate failed");
    }
    eprintln!(" done.");

    print_table_header();

    for &n in thread_sweep {
        let result =
            run_scaling_experiment(&bench_db.db, n, WARMUP_SECS, MEASURE_SECS, move |tid, strata, stop| {
                let mut sampler = ReservoirSampler::with_seed(tid as u64);
                let mut ops = 0u64;
                let mut rng = tid as u64 ^ 0xfeedface;
                let mut seq = 0u64;

                while !stop.load(Ordering::Relaxed) {
                    let coin = fast_rand(&mut rng) % 10;
                    let start = Instant::now();

                    if coin == 0 {
                        // 10% writes -- thread-unique keys to avoid contention
                        seq += 1;
                        let key = format!("mix_t{}_{}", tid, seq);
                        let _ = strata.kv_put(&key, Value::Int(seq as i64));
                    } else {
                        // 90% reads -- random from pre-populated set
                        let idx = fast_rand(&mut rng) % PREPOPULATE_KEYS as u64;
                        let key = format!("key{:06}", idx);
                        let _ = strata.kv_get(&key);
                    }

                    sampler.record(start.elapsed());
                    ops += 1;
                }

                ThreadResult {
                    ops,
                    aborts: 0,
                    latencies: sampler.into_samples(),
                }
            });
        print_table_row(&result);
        record_scaling_result(recorder, "mixed_90_10", &mode, &result);
    }
}

// ---------------------------------------------------------------------------
// Durability modes to test
// ---------------------------------------------------------------------------

fn durability_modes() -> Vec<DurabilityConfig> {
    DurabilityConfig::ALL.to_vec()
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    // Parse --threads argument if provided
    let args: Vec<String> = std::env::args().collect();
    let thread_sweep = if let Some(pos) = args.iter().position(|a| a == "--threads") {
        if let Some(val) = args.get(pos + 1) {
            parse_thread_counts(val)
        } else {
            harness::scaling::thread_counts()
        }
    } else {
        harness::scaling::thread_counts()
    };

    // Hardware info
    let cores = physical_cores();
    eprintln!("=== Scaling & Concurrency Benchmark Suite ===");
    eprintln!("Physical cores (available_parallelism): {}", cores);
    eprintln!("Thread sweep: {:?}", thread_sweep);
    eprintln!(
        "Measurement: {}s warmup + {}s measure per run",
        WARMUP_SECS, MEASURE_SECS
    );
    eprintln!();

    let mut recorder = ResultRecorder::new("concurrency");

    for mode in durability_modes() {
        run_kv_get_scaling(&thread_sweep, mode, &mut recorder);
        run_kv_put_independent_scaling(&thread_sweep, mode, &mut recorder);
        run_kv_put_hot_scaling(&thread_sweep, mode, &mut recorder);
        run_mixed_90_10_scaling(&thread_sweep, mode, &mut recorder);
    }

    eprintln!("\n=== Benchmark complete ===");
    let _ = recorder.save();
}
