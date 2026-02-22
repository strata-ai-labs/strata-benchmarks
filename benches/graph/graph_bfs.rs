//! LDBC Graphalytics BFS Benchmark for StrataDB
//!
//! Validates BFS traversal correctness against LDBC reference output and measures
//! throughput via EVPS (Edges + Vertices processed per second).
//!
//! Uses a custom harness (matching fill-level and redis-compare patterns) since BFS
//! is a whole-graph operation, not per-operation latency.
//!
//! Run:           `cargo bench --bench graph_bfs`
//! Quick:         `cargo bench --bench graph_bfs -- -q`
//! Validate only: `cargo bench --bench graph_bfs -- --validate-only`
//! CSV:           `cargo bench --bench graph_bfs -- --csv`
//! Custom data:   `cargo bench --bench graph_bfs -- --dataset path/to/ldbc/dir`

#[allow(unused)]
#[path = "../harness/mod.rs"]
mod harness;

#[allow(unused)]
mod ldbc;

use harness::recorder::ResultRecorder;
use harness::{create_db, print_hardware_info, BenchDb, DurabilityConfig};
use ldbc::{BfsReference, LdbcDataset, UNREACHABLE};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;
use strata_benchmarks::schema::{BenchmarkMetrics, BenchmarkResult};

// ---------------------------------------------------------------------------
// Default parameters
// ---------------------------------------------------------------------------

const DEFAULT_RUNS: usize = 10;

fn default_dataset_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data/graph/example-directed")
}

// ---------------------------------------------------------------------------
// CLI configuration
// ---------------------------------------------------------------------------

struct Config {
    dataset: PathBuf,
    source: Option<u64>,
    runs: usize,
    validate_only: bool,
    no_validate: bool,
    csv: bool,
    quiet: bool,
}

fn parse_args() -> Config {
    let args: Vec<String> = std::env::args().collect();
    let mut config = Config {
        dataset: default_dataset_dir(),
        source: None,
        runs: DEFAULT_RUNS,
        validate_only: false,
        no_validate: false,
        csv: false,
        quiet: false,
    };

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--dataset" => {
                i += 1;
                if i < args.len() {
                    config.dataset = PathBuf::from(&args[i]);
                }
            }
            "--source" => {
                i += 1;
                if i < args.len() {
                    config.source = args[i].parse().ok();
                }
            }
            "--runs" => {
                i += 1;
                if i < args.len() {
                    config.runs = args[i].parse().unwrap_or(DEFAULT_RUNS);
                }
            }
            "--validate-only" => config.validate_only = true,
            "--no-validate" => config.no_validate = true,
            "--csv" => config.csv = true,
            "-q" => config.quiet = true,
            _ => {}
        }
        i += 1;
    }

    config
}

// ---------------------------------------------------------------------------
// Graph loading
// ---------------------------------------------------------------------------

fn load_graph(db: &BenchDb, dataset: &LdbcDataset) -> std::time::Duration {
    let start = Instant::now();

    db.db.graph_create("ldbc").expect("graph_create failed");

    for &vid in &dataset.vertices {
        db.db
            .graph_add_node("ldbc", &vid.to_string(), None, None)
            .expect("graph_add_node failed");
    }

    for &(src, dst) in &dataset.edges {
        db.db
            .graph_add_edge(
                "ldbc",
                &src.to_string(),
                &dst.to_string(),
                "E",
                None,
                None,
            )
            .expect("graph_add_edge failed");
    }

    start.elapsed()
}

// ---------------------------------------------------------------------------
// BFS execution
// ---------------------------------------------------------------------------

struct BfsRun {
    elapsed: std::time::Duration,
    depths: HashMap<String, usize>,
}

fn run_bfs(db: &BenchDb, source: u64) -> BfsRun {
    let start = Instant::now();
    let result = db
        .db
        .graph_bfs(
            "ldbc",
            &source.to_string(),
            usize::MAX,
            None,
            None,
            Some("both"),
        )
        .expect("graph_bfs failed");
    let elapsed = start.elapsed();

    BfsRun {
        elapsed,
        depths: result.depths,
    }
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

struct ValidationResult {
    pass: bool,
    mismatches: usize,
    details: Vec<String>,
}

fn validate_bfs(
    dataset: &LdbcDataset,
    bfs_depths: &HashMap<String, usize>,
    reference: &BfsReference,
) -> ValidationResult {
    let mut mismatches = 0;
    let mut details = Vec::new();

    for &vid in &dataset.vertices {
        let ref_depth = reference.depths.get(&vid).copied().unwrap_or(UNREACHABLE);
        let actual_depth = bfs_depths.get(&vid.to_string()).copied();

        if ref_depth == UNREACHABLE {
            // Vertex should be unreachable
            if let Some(actual) = actual_depth {
                mismatches += 1;
                if details.len() < 10 {
                    details.push(format!(
                        "vertex {}: expected unreachable, got depth {}",
                        vid, actual
                    ));
                }
            }
        } else {
            // Vertex should be at specific depth
            match actual_depth {
                None => {
                    mismatches += 1;
                    if details.len() < 10 {
                        details.push(format!(
                            "vertex {}: expected depth {}, but not visited",
                            vid, ref_depth
                        ));
                    }
                }
                Some(actual) if actual as i64 != ref_depth => {
                    mismatches += 1;
                    if details.len() < 10 {
                        details.push(format!(
                            "vertex {}: expected depth {}, got {}",
                            vid, ref_depth, actual
                        ));
                    }
                }
                _ => {} // match
            }
        }
    }

    ValidationResult {
        pass: mismatches == 0,
        mismatches,
        details,
    }
}

// ---------------------------------------------------------------------------
// Output formatters
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

fn print_csv_header() {
    println!("\"run\",\"bfs_time_ms\",\"evps\",\"vertices\",\"edges\"");
}

fn print_csv_row(run: usize, bfs_ms: f64, evps: f64, vertices: usize, edges: usize) {
    println!("{},{:.3},{:.0},{},{}", run, bfs_ms, evps, vertices, edges);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    let config = parse_args();
    print_hardware_info();

    // Load dataset
    let dataset = LdbcDataset::load(&config.dataset).unwrap_or_else(|e| {
        eprintln!("Failed to load dataset from {}: {}", config.dataset.display(), e);
        std::process::exit(1);
    });

    let source = config
        .source
        .or(dataset.bfs_source)
        .unwrap_or(dataset.vertices[0]);

    if !config.csv {
        eprintln!("=== LDBC Graphalytics BFS Benchmark ===");
        eprintln!("Dataset:  {} ({} vertices, {} edges, {})",
            dataset.name,
            fmt_num(dataset.vertices.len() as u64),
            fmt_num(dataset.edges.len() as u64),
            if dataset.directed { "directed" } else { "undirected" },
        );
        eprintln!("Source:   {}", source);
        eprintln!("Runs:     {}", config.runs);
        eprintln!("Direction: both (LDBC BFS treats edges as undirected)");
        eprintln!();
    }

    // Load graph into Strata
    let db = create_db(DurabilityConfig::Cache);

    if !config.csv && !config.quiet {
        eprint!("Loading graph into Strata...");
    }
    let load_time = load_graph(&db, &dataset);
    if !config.csv && !config.quiet {
        eprintln!(" done ({:.3}ms)", load_time.as_secs_f64() * 1000.0);
    }

    // Load BFS reference for validation
    let reference = if !config.no_validate {
        let bfs_path = config.dataset.join(format!("{}-BFS", dataset.name));
        if bfs_path.exists() {
            Some(BfsReference::load(&bfs_path).unwrap_or_else(|e| {
                eprintln!("Failed to load BFS reference: {}", e);
                std::process::exit(1);
            }))
        } else {
            if !config.csv && !config.quiet {
                eprintln!("No BFS reference file found, skipping validation.");
            }
            None
        }
    } else {
        None
    };

    // Run BFS
    let mut run_times = Vec::with_capacity(config.runs);
    let total_elements = (dataset.vertices.len() + dataset.edges.len()) as f64;

    if config.csv {
        print_csv_header();
    }

    for run in 0..config.runs {
        let bfs_run = run_bfs(&db, source);
        let bfs_ms = bfs_run.elapsed.as_secs_f64() * 1000.0;
        let evps = total_elements / bfs_run.elapsed.as_secs_f64();
        run_times.push(bfs_run.elapsed);

        // Validate first run (or all runs in validate-only mode)
        if let Some(ref reference) = reference {
            if run == 0 || config.validate_only {
                let validation = validate_bfs(&dataset, &bfs_run.depths, reference);
                if !config.csv {
                    if validation.pass {
                        eprintln!("Validation: PASS ({} vertices checked)", dataset.vertices.len());
                    } else {
                        eprintln!(
                            "Validation: FAIL ({} mismatches out of {} vertices)",
                            validation.mismatches,
                            dataset.vertices.len()
                        );
                        for detail in &validation.details {
                            eprintln!("  {}", detail);
                        }
                    }
                }
                if !validation.pass && config.validate_only {
                    std::process::exit(1);
                }
            }
        }

        if config.validate_only {
            if !config.csv {
                eprintln!("Validate-only mode, skipping remaining runs.");
            }
            return;
        }

        if config.csv {
            print_csv_row(run + 1, bfs_ms, evps, dataset.vertices.len(), dataset.edges.len());
        } else if config.quiet {
            if run == 0 {
                eprintln!(
                    "BFS: {:.3}ms, EVPS: {:.0}, |V|={}, |E|={}",
                    bfs_ms, evps, dataset.vertices.len(), dataset.edges.len()
                );
            }
        }
    }

    // Compute percentiles from run times
    run_times.sort_unstable();
    let len = run_times.len();
    let p50 = run_times[len * 50 / 100];
    let p95 = run_times[(len * 95 / 100).min(len - 1)];
    let p99 = run_times[(len * 99 / 100).min(len - 1)];
    let min = run_times[0];
    let max = run_times[len - 1];
    let sum: std::time::Duration = run_times.iter().sum();
    let avg = sum / len as u32;

    let avg_evps = total_elements / avg.as_secs_f64();

    if !config.csv && !config.quiet {
        eprintln!();
        eprintln!("--- BFS Results ({} runs) ---", len);
        eprintln!(
            "  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}",
            "avg", "p50", "p95", "p99", "min", "max"
        );
        eprintln!(
            "  {:>9.3}ms  {:>9.3}ms  {:>9.3}ms  {:>9.3}ms  {:>9.3}ms  {:>9.3}ms",
            avg.as_secs_f64() * 1000.0,
            p50.as_secs_f64() * 1000.0,
            p95.as_secs_f64() * 1000.0,
            p99.as_secs_f64() * 1000.0,
            min.as_secs_f64() * 1000.0,
            max.as_secs_f64() * 1000.0,
        );
        eprintln!();
        eprintln!(
            "EVPS (avg):   {:.0}  (|V|+|E|={} / {:.6}s)",
            avg_evps,
            total_elements as u64,
            avg.as_secs_f64(),
        );
        eprintln!(
            "Load time:    {:.3}ms",
            load_time.as_secs_f64() * 1000.0,
        );
        eprintln!();
    }

    // Record results
    let mut recorder = ResultRecorder::new("graph-bfs");
    let mut params = HashMap::new();
    params.insert("dataset".into(), serde_json::json!(dataset.name));
    params.insert("source".into(), serde_json::json!(source));
    params.insert("vertices".into(), serde_json::json!(dataset.vertices.len()));
    params.insert("edges".into(), serde_json::json!(dataset.edges.len()));
    params.insert("direction".into(), serde_json::json!("both"));

    recorder.record(BenchmarkResult {
        benchmark: format!("graph-bfs/{}/{}V-{}E", dataset.name, dataset.vertices.len(), dataset.edges.len()),
        category: "graph-bfs".to_string(),
        parameters: params,
        metrics: BenchmarkMetrics {
            ops_per_sec: Some(avg_evps),
            p50_ns: Some(p50.as_nanos() as u64),
            p95_ns: Some(p95.as_nanos() as u64),
            p99_ns: Some(p99.as_nanos() as u64),
            min_ns: Some(min.as_nanos() as u64),
            max_ns: Some(max.as_nanos() as u64),
            avg_ns: Some(avg.as_nanos() as u64),
            samples: Some(len as u64),
            ..Default::default()
        },
    });

    if !config.csv {
        eprintln!("=== Benchmark complete ===");
    }
    let _ = recorder.save();
}
