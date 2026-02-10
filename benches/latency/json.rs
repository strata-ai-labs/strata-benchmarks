//! JSON primitive benchmarks: set_root, set_path, get, list
//!
//! All benchmarks report latency percentiles.

#[allow(unused)]
#[path = "../harness/mod.rs"]
mod harness;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use criterion::{criterion_group, BenchmarkId, Criterion, Throughput};
use harness::recorder::ResultRecorder;
use harness::{
    create_db, json_document, measure_with_counters, report_counters, report_percentiles,
    DurabilityConfig, PERCENTILE_SAMPLES, WARMUP_COUNT,
};
use stratadb::Value;

static RECORDER: Mutex<Option<ResultRecorder>> = Mutex::new(None);

fn json_set_root(c: &mut Criterion) {
    let mut group = c.benchmark_group("json/set_root");
    group.throughput(Throughput::Elements(1));

    eprintln!("\n--- Latency Percentiles: json/set_root ---");
    for mode in DurabilityConfig::ALL {
        let bench_db = create_db(mode);
        let counter = AtomicU64::new(0);
        group.bench_function(BenchmarkId::new("durability", mode.label()), |b| {
            b.iter(|| {
                let i = counter.fetch_add(1, Ordering::Relaxed);
                bench_db
                    .db
                    .json_set(&format!("doc:{}", i), "$", json_document(i))
                    .unwrap();
            });
        });

        let pct_counter = AtomicU64::new(u64::MAX / 2);
        let label = format!("json/set_root/{}", mode.label());
        let (p, counters) = measure_with_counters(&bench_db, PERCENTILE_SAMPLES, || {
            let i = pct_counter.fetch_add(1, Ordering::Relaxed);
            bench_db
                .db
                .json_set(&format!("doc:{}", i), "$", json_document(i))
                .unwrap();
        });
        report_percentiles(&label, &p);
        report_counters(&label, &counters, PERCENTILE_SAMPLES as u64);

        if let Some(rec) = RECORDER.lock().unwrap().as_mut() {
            let mut params = HashMap::new();
            params.insert("durability".into(), serde_json::json!(mode.label()));
            rec.record_latency(&label, params, &p, Some(&counters), PERCENTILE_SAMPLES as u64);
        }
    }
    group.finish();
}

fn json_set_path(c: &mut Criterion) {
    let mut group = c.benchmark_group("json/set_path");
    group.throughput(Throughput::Elements(1));

    eprintln!("\n--- Latency Percentiles: json/set_path ---");
    for mode in DurabilityConfig::ALL {
        let bench_db = create_db(mode);
        for i in 0..1000u64 {
            bench_db
                .db
                .json_set(&format!("doc:{}", i), "$", json_document(i))
                .unwrap();
        }
        let counter = AtomicU64::new(0);
        group.bench_function(BenchmarkId::new("durability", mode.label()), |b| {
            b.iter(|| {
                let i = counter.fetch_add(1, Ordering::Relaxed) % 1000;
                bench_db
                    .db
                    .json_set(
                        &format!("doc:{}", i),
                        "$.metadata.mid_score",
                        Value::Float(i as f64 * 2.5),
                    )
                    .unwrap();
            });
        });

        let pct_counter = AtomicU64::new(0);
        let label = format!("json/set_path/{}", mode.label());
        let (p, counters) = measure_with_counters(&bench_db, PERCENTILE_SAMPLES, || {
            let i = pct_counter.fetch_add(1, Ordering::Relaxed) % 1000;
            bench_db
                .db
                .json_set(
                    &format!("doc:{}", i),
                    "$.metadata.mid_score",
                    Value::Float(i as f64 * 2.5),
                )
                .unwrap();
        });
        report_percentiles(&label, &p);
        report_counters(&label, &counters, PERCENTILE_SAMPLES as u64);

        if let Some(rec) = RECORDER.lock().unwrap().as_mut() {
            let mut params = HashMap::new();
            params.insert("durability".into(), serde_json::json!(mode.label()));
            rec.record_latency(&label, params, &p, Some(&counters), PERCENTILE_SAMPLES as u64);
        }
    }
    group.finish();
}

fn json_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("json/get");
    group.throughput(Throughput::Elements(1));

    eprintln!("\n--- Latency Percentiles: json/get ---");
    for mode in DurabilityConfig::ALL {
        let bench_db = create_db(mode);
        for i in 0..WARMUP_COUNT {
            bench_db
                .db
                .json_set(&format!("doc:{}", i), "$", json_document(i))
                .unwrap();
        }
        let counter = AtomicU64::new(0);
        group.bench_function(BenchmarkId::new("durability", mode.label()), |b| {
            b.iter(|| {
                let i = counter.fetch_add(1, Ordering::Relaxed) % WARMUP_COUNT;
                bench_db.db.json_get(&format!("doc:{}", i), "$").unwrap();
            });
        });

        let pct_counter = AtomicU64::new(0);
        let label = format!("json/get/{}", mode.label());
        let (p, counters) = measure_with_counters(&bench_db, PERCENTILE_SAMPLES, || {
            let i = pct_counter.fetch_add(1, Ordering::Relaxed) % WARMUP_COUNT;
            bench_db.db.json_get(&format!("doc:{}", i), "$").unwrap();
        });
        report_percentiles(&label, &p);
        report_counters(&label, &counters, PERCENTILE_SAMPLES as u64);

        if let Some(rec) = RECORDER.lock().unwrap().as_mut() {
            let mut params = HashMap::new();
            params.insert("durability".into(), serde_json::json!(mode.label()));
            rec.record_latency(&label, params, &p, Some(&counters), PERCENTILE_SAMPLES as u64);
        }
    }
    group.finish();
}

fn json_list(c: &mut Criterion) {
    let mut group = c.benchmark_group("json/list");
    group.throughput(Throughput::Elements(1));

    eprintln!("\n--- Latency Percentiles: json/list ---");
    for mode in DurabilityConfig::ALL {
        let bench_db = create_db(mode);
        for i in 0..1000u64 {
            bench_db
                .db
                .json_set(&format!("bench:{}", i), "$", json_document(i))
                .unwrap();
        }
        group.bench_function(BenchmarkId::new("durability", mode.label()), |b| {
            b.iter(|| {
                bench_db
                    .db
                    .json_list(Some("bench:".to_string()), None, 100)
                    .unwrap();
            });
        });

        let label = format!("json/list/{}", mode.label());
        let (p, counters) = measure_with_counters(&bench_db, PERCENTILE_SAMPLES, || {
            bench_db
                .db
                .json_list(Some("bench:".to_string()), None, 100)
                .unwrap();
        });
        report_percentiles(&label, &p);
        report_counters(&label, &counters, PERCENTILE_SAMPLES as u64);

        if let Some(rec) = RECORDER.lock().unwrap().as_mut() {
            let mut params = HashMap::new();
            params.insert("durability".into(), serde_json::json!(mode.label()));
            rec.record_latency(&label, params, &p, Some(&counters), PERCENTILE_SAMPLES as u64);
        }
    }
    group.finish();
}

criterion_group!(benches, json_set_root, json_set_path, json_get, json_list);

fn main() {
    *RECORDER.lock().unwrap() = Some(ResultRecorder::new("latency"));
    benches();
    if let Some(recorder) = RECORDER.lock().unwrap().take() {
        let _ = recorder.save();
    }
}
