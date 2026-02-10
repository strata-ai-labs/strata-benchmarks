//! Vector primitive benchmarks: upsert, search, get
//!
//! Reduced sample_size because vector operations are inherently slower.
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
    create_db, measure_with_counters, report_counters, report_percentiles, vector_128d,
    DurabilityConfig, PERCENTILE_SAMPLES, WARMUP_COUNT,
};
use stratadb::DistanceMetric;

static RECORDER: Mutex<Option<ResultRecorder>> = Mutex::new(None);

fn vector_upsert(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector/upsert");
    group.throughput(Throughput::Elements(1));
    group.sample_size(50);

    eprintln!("\n--- Latency Percentiles: vector/upsert ---");
    for mode in DurabilityConfig::ALL {
        let bench_db = create_db(mode);
        bench_db
            .db
            .vector_create_collection("bench_col", 128, DistanceMetric::Cosine)
            .unwrap();
        let counter = AtomicU64::new(0);
        group.bench_function(BenchmarkId::new("durability", mode.label()), |b| {
            b.iter(|| {
                let i = counter.fetch_add(1, Ordering::Relaxed);
                bench_db
                    .db
                    .vector_upsert("bench_col", &format!("vec_{}", i), vector_128d(i), None)
                    .unwrap();
            });
        });

        let pct_counter = AtomicU64::new(u64::MAX / 2);
        let samples = match mode {
            DurabilityConfig::Always => 200,
            _ => PERCENTILE_SAMPLES,
        };
        let label = format!("vector/upsert/{}", mode.label());
        let (p, counters) = measure_with_counters(&bench_db, samples, || {
            let i = pct_counter.fetch_add(1, Ordering::Relaxed);
            bench_db
                .db
                .vector_upsert("bench_col", &format!("vec_{}", i), vector_128d(i), None)
                .unwrap();
        });
        report_percentiles(&label, &p);
        report_counters(&label, &counters, samples as u64);

        if let Some(rec) = RECORDER.lock().unwrap().as_mut() {
            let mut params = HashMap::new();
            params.insert("durability".into(), serde_json::json!(mode.label()));
            rec.record_latency(&label, params, &p, Some(&counters), samples as u64);
        }
    }
    group.finish();
}

fn vector_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector/search");
    group.throughput(Throughput::Elements(1));
    group.sample_size(20);

    eprintln!("\n--- Latency Percentiles: vector/search ---");
    for mode in DurabilityConfig::ALL {
        let bench_db = create_db(mode);
        bench_db
            .db
            .vector_create_collection("bench_col", 128, DistanceMetric::Cosine)
            .unwrap();
        for i in 0..WARMUP_COUNT {
            bench_db
                .db
                .vector_upsert("bench_col", &format!("vec_{}", i), vector_128d(i), None)
                .unwrap();
        }
        let counter = AtomicU64::new(0);
        group.bench_function(BenchmarkId::new("durability", mode.label()), |b| {
            b.iter(|| {
                let i = counter.fetch_add(1, Ordering::Relaxed);
                bench_db
                    .db
                    .vector_search("bench_col", vector_128d(WARMUP_COUNT + i), 10)
                    .unwrap();
            });
        });

        let pct_counter = AtomicU64::new(0);
        let label = format!("vector/search/{}", mode.label());
        let (p, counters) = measure_with_counters(&bench_db, 200, || {
            let i = pct_counter.fetch_add(1, Ordering::Relaxed);
            bench_db
                .db
                .vector_search("bench_col", vector_128d(WARMUP_COUNT + i), 10)
                .unwrap();
        });
        report_percentiles(&label, &p);
        report_counters(&label, &counters, 200);

        if let Some(rec) = RECORDER.lock().unwrap().as_mut() {
            let mut params = HashMap::new();
            params.insert("durability".into(), serde_json::json!(mode.label()));
            rec.record_latency(&label, params, &p, Some(&counters), 200);
        }
    }
    group.finish();
}

fn vector_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector/get");
    group.throughput(Throughput::Elements(1));
    group.sample_size(50);

    eprintln!("\n--- Latency Percentiles: vector/get ---");
    for mode in DurabilityConfig::ALL {
        let bench_db = create_db(mode);
        bench_db
            .db
            .vector_create_collection("bench_col", 128, DistanceMetric::Cosine)
            .unwrap();
        for i in 0..WARMUP_COUNT {
            bench_db
                .db
                .vector_upsert("bench_col", &format!("vec_{}", i), vector_128d(i), None)
                .unwrap();
        }
        let counter = AtomicU64::new(0);
        group.bench_function(BenchmarkId::new("durability", mode.label()), |b| {
            b.iter(|| {
                let i = counter.fetch_add(1, Ordering::Relaxed) % WARMUP_COUNT;
                bench_db
                    .db
                    .vector_get("bench_col", &format!("vec_{}", i))
                    .unwrap();
            });
        });

        let pct_counter = AtomicU64::new(0);
        let label = format!("vector/get/{}", mode.label());
        let (p, counters) = measure_with_counters(&bench_db, PERCENTILE_SAMPLES, || {
            let i = pct_counter.fetch_add(1, Ordering::Relaxed) % WARMUP_COUNT;
            bench_db
                .db
                .vector_get("bench_col", &format!("vec_{}", i))
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

criterion_group!(benches, vector_upsert, vector_search, vector_get);

fn main() {
    *RECORDER.lock().unwrap() = Some(ResultRecorder::new("latency"));
    benches();
    if let Some(recorder) = RECORDER.lock().unwrap().take() {
        let _ = recorder.save();
    }
}
