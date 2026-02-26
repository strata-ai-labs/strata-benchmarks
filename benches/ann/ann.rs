//! ANN (Approximate Nearest Neighbor) Benchmark for StrataDB
//!
//! Measures the standard ANN trade-off: Recall@k vs Queries Per Second (QPS),
//! following ann-benchmarks.com methodology with synthetic clustered data.
//!
//! Run:    `cargo bench --bench ann`
//! Quick:  `cargo bench --bench ann -- -q`
//! Custom: `cargo bench --bench ann -- --scales 10000,50000 --ks 1,10`
//! CSV:    `cargo bench --bench ann -- --csv`

#[allow(unused)]
#[path = "../harness/mod.rs"]
mod harness;

mod dataset;

use dataset::{compute_ground_truth, compute_recall, generate_dataset};
use harness::recorder::ResultRecorder;
use harness::{create_db, print_hardware_info, DurabilityConfig};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use strata_benchmarks::schema::{BenchmarkMetrics, BenchmarkResult};
use stratadb::DistanceMetric;

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

const DEFAULT_SCALES: &[usize] = &[10_000, 50_000, 100_000];
const DEFAULT_KS: &[usize] = &[1, 10, 100];
const DEFAULT_QUERIES: usize = 100;
const DIM: usize = 128;
const SEED: u64 = 0xA00_2026;

// ---------------------------------------------------------------------------
// Result type
// ---------------------------------------------------------------------------

struct AnnResult {
    scale: usize,
    k: usize,
    build_qps: f64,
    search_qps: f64,
    recall: f64,
    latencies: Vec<Duration>,
    p50: Duration,
    p95: Duration,
    p99: Duration,
}

// ---------------------------------------------------------------------------
// Output helpers
// ---------------------------------------------------------------------------

fn fmt_num(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

fn fmt_duration(d: Duration) -> String {
    let nanos = d.as_nanos();
    if nanos < 1_000 {
        format!("{:>7}ns", nanos)
    } else if nanos < 1_000_000 {
        format!("{:>6.1}us", nanos as f64 / 1_000.0)
    } else if nanos < 1_000_000_000 {
        format!("{:>6.1}ms", nanos as f64 / 1_000_000.0)
    } else {
        format!("{:>6.2}s ", nanos as f64 / 1_000_000_000.0)
    }
}

fn scale_label(n: usize) -> String {
    if n >= 1_000_000 {
        format!("{}m", n / 1_000_000)
    } else if n >= 1_000 {
        format!("{}k", n / 1_000)
    } else {
        format!("{}", n)
    }
}

fn print_table_header() {
    eprintln!(
        "  {:>10}  {:>5}  {:>10}  {:>10}  {:>8}  {:>10}  {:>10}  {:>10}",
        "scale", "k", "build QPS", "search QPS", "recall", "p50", "p95", "p99"
    );
}

fn print_table_row(r: &AnnResult) {
    eprintln!(
        "  {:>10}  {:>5}  {:>10}  {:>10}  {:>8.4}  {:>10}  {:>10}  {:>10}",
        fmt_num(r.scale as u64),
        r.k,
        fmt_num(r.build_qps as u64),
        fmt_num(r.search_qps as u64),
        r.recall,
        fmt_duration(r.p50),
        fmt_duration(r.p95),
        fmt_duration(r.p99),
    );
}

fn print_quiet(r: &AnnResult) {
    eprintln!(
        "ann {}@k={}: recall={:.4}, search={} QPS, build={} QPS, p50={}",
        fmt_num(r.scale as u64),
        r.k,
        r.recall,
        fmt_num(r.search_qps as u64),
        fmt_num(r.build_qps as u64),
        fmt_duration(r.p50),
    );
}

fn print_csv_header() {
    println!(
        "\"scale\",\"k\",\"dim\",\"build_qps\",\"search_qps\",\"recall\",\"p50_us\",\"p95_us\",\"p99_us\""
    );
}

fn print_csv_row(r: &AnnResult) {
    println!(
        "{},{},{},{:.2},{:.2},{:.6},{:.1},{:.1},{:.1}",
        r.scale,
        r.k,
        DIM,
        r.build_qps,
        r.search_qps,
        r.recall,
        r.p50.as_nanos() as f64 / 1_000.0,
        r.p95.as_nanos() as f64 / 1_000.0,
        r.p99.as_nanos() as f64 / 1_000.0,
    );
}

fn print_reference_points() {
    eprintln!();
    eprintln!("  Published reference points (ann-benchmarks.com, 128d cosine, ~1M vectors):");
    eprintln!("    hnswlib   ~25,000 QPS @ 0.95 recall");
    eprintln!("    FAISS-IVF ~10,000 QPS @ 0.90 recall");
    eprintln!("    Annoy     ~ 5,000 QPS @ 0.85 recall");
    eprintln!("    ScaNN     ~30,000 QPS @ 0.95 recall");
}

// ---------------------------------------------------------------------------
// JSON recording
// ---------------------------------------------------------------------------

fn record_result(recorder: &mut ResultRecorder, r: &AnnResult, config: &Config) {
    let mut params = HashMap::new();
    params.insert("scale".into(), serde_json::json!(r.scale));
    params.insert("k".into(), serde_json::json!(r.k));
    params.insert("dim".into(), serde_json::json!(DIM));
    params.insert("recall".into(), serde_json::json!(r.recall));
    params.insert("build_qps".into(), serde_json::json!(r.build_qps));
    params.insert("queries".into(), serde_json::json!(config.queries));
    params.insert("durability".into(), serde_json::json!(config.durability.label()));
    params.insert("metric".into(), serde_json::json!("cosine"));

    recorder.record(BenchmarkResult {
        benchmark: format!("ann/{}/k{}/{}d", scale_label(r.scale), r.k, DIM),
        category: "ann".to_string(),
        parameters: params,
        metrics: BenchmarkMetrics {
            ops_per_sec: Some(r.search_qps),
            p50_ns: Some(r.p50.as_nanos() as u64),
            p95_ns: Some(r.p95.as_nanos() as u64),
            p99_ns: Some(r.p99.as_nanos() as u64),
            samples: Some(r.latencies.len() as u64),
            ..Default::default()
        },
    });
}

// ---------------------------------------------------------------------------
// CLI parsing
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Config {
    scales: Vec<usize>,
    ks: Vec<usize>,
    queries: usize,
    durability: DurabilityConfig,
    csv: bool,
    quiet: bool,
}

fn parse_args() -> Config {
    let args: Vec<String> = std::env::args().collect();
    let mut config = Config {
        scales: DEFAULT_SCALES.to_vec(),
        ks: DEFAULT_KS.to_vec(),
        queries: DEFAULT_QUERIES,
        durability: DurabilityConfig::Cache,
        csv: false,
        quiet: false,
    };

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--scales" => {
                i += 1;
                if i < args.len() {
                    config.scales = args[i]
                        .split(',')
                        .filter_map(|s| s.trim().parse().ok())
                        .collect();
                }
            }
            "--ks" => {
                i += 1;
                if i < args.len() {
                    config.ks = args[i]
                        .split(',')
                        .filter_map(|s| s.trim().parse().ok())
                        .collect();
                }
            }
            "--queries" => {
                i += 1;
                if i < args.len() {
                    config.queries = args[i].parse().unwrap_or(DEFAULT_QUERIES);
                }
            }
            "--durability" => {
                i += 1;
                if i < args.len() {
                    config.durability = match args[i].as_str() {
                        "cache" => DurabilityConfig::Cache,
                        "standard" => DurabilityConfig::Standard,
                        "always" => DurabilityConfig::Always,
                        _ => DurabilityConfig::Cache,
                    };
                }
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

    if !config.csv && !config.quiet {
        eprintln!("=== StrataDB ANN Benchmark ===");
        eprintln!("Measures Recall@k vs QPS (ann-benchmarks.com methodology)");
        eprintln!();
        eprintln!(
            "Parameters: {}d, cosine, {} queries, {} mode",
            DIM, config.queries, config.durability.label()
        );
        eprintln!(
            "Scales: {:?}",
            config.scales
        );
        eprintln!(
            "k values: {:?}",
            config.ks
        );
        eprintln!();
    }

    if config.csv {
        print_csv_header();
    }

    let mut recorder = ResultRecorder::new("ann");
    let max_k = *config.ks.iter().max().unwrap_or(&10);

    for &scale in &config.scales {
        // Phase 1: Generate dataset
        if !config.csv && !config.quiet {
            eprint!(
                "  Generating {} vectors ({}d, {} clusters)...",
                fmt_num(scale as u64),
                DIM,
                10
            );
        }
        let gen_start = Instant::now();
        let dataset = generate_dataset(scale, config.queries, DIM, SEED);
        let gen_elapsed = gen_start.elapsed();
        if !config.csv && !config.quiet {
            eprintln!(" {:.2}s", gen_elapsed.as_secs_f64());
        }

        // Phase 2: Compute brute-force ground truth (at max k)
        if !config.csv && !config.quiet {
            eprint!("  Computing ground truth (brute-force, k={})...", max_k);
        }
        let gt_start = Instant::now();
        let ground_truth = compute_ground_truth(&dataset, max_k);
        let gt_elapsed = gt_start.elapsed();
        if !config.csv && !config.quiet {
            eprintln!(" {:.2}s", gt_elapsed.as_secs_f64());
        }

        // Phase 3: Build index (insert all vectors)
        if !config.csv && !config.quiet {
            eprint!("  Building index ({} vectors)...", fmt_num(scale as u64));
        }
        let db = create_db(config.durability);
        db.db
            .vector_create_collection("ann_bench", DIM as u64, DistanceMetric::Cosine)
            .unwrap();

        let build_start = Instant::now();
        for i in 0..scale {
            db.db
                .vector_upsert(
                    "ann_bench",
                    &dataset.train_keys[i],
                    dataset.train_vectors[i].clone(),
                    None,
                )
                .unwrap();
        }
        let build_elapsed = build_start.elapsed();
        let build_qps = scale as f64 / build_elapsed.as_secs_f64();

        if !config.csv && !config.quiet {
            eprintln!(
                " {:.2}s ({} inserts/s)",
                build_elapsed.as_secs_f64(),
                fmt_num(build_qps as u64)
            );
        }

        // Print scale header
        if !config.csv && !config.quiet {
            eprintln!();
            eprintln!(
                "--- {} vectors, {}d, cosine ---",
                fmt_num(scale as u64),
                DIM
            );
            print_table_header();
        }

        // Phase 4: Search for each k value
        for &k in &config.ks {
            // Truncate ground truth to this k
            let gt_k = dataset::GroundTruth {
                neighbors: ground_truth
                    .neighbors
                    .iter()
                    .map(|nn| nn.iter().take(k).copied().collect())
                    .collect(),
                k,
            };

            let mut latencies = Vec::with_capacity(config.queries);
            let mut ann_results = Vec::with_capacity(config.queries);

            let search_start = Instant::now();
            for q in 0..config.queries {
                let query = dataset.query_vectors[q].clone();
                let op_start = Instant::now();
                let results = db.db.vector_search("ann_bench", query, k as u64).unwrap();
                latencies.push(op_start.elapsed());

                let keys: Vec<String> = results.iter().map(|m| m.key.clone()).collect();
                ann_results.push(keys);
            }
            let search_elapsed = search_start.elapsed();
            let search_qps = config.queries as f64 / search_elapsed.as_secs_f64();

            // Compute recall
            let recall = compute_recall(&ann_results, &gt_k, &dataset);

            // Compute percentiles
            latencies.sort_unstable();
            let len = latencies.len();
            let p50 = latencies[len * 50 / 100];
            let p95 = latencies[(len * 95 / 100).min(len - 1)];
            let p99 = latencies[(len * 99 / 100).min(len - 1)];

            let result = AnnResult {
                scale,
                k,
                build_qps,
                search_qps,
                recall,
                latencies,
                p50,
                p95,
                p99,
            };

            // Output
            if config.csv {
                print_csv_row(&result);
            } else if config.quiet {
                print_quiet(&result);
            } else {
                print_table_row(&result);
            }

            record_result(&mut recorder, &result, &config);
        }

        if !config.csv && !config.quiet {
            eprintln!();
        }
    }

    if !config.csv && !config.quiet {
        print_reference_points();
        eprintln!();
        eprintln!("=== ANN benchmark complete ===");
    }
    let _ = recorder.save();
}
