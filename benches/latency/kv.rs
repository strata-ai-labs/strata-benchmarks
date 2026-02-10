//! KV primitive benchmarks: put, get, delete, list_prefix
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
    create_db, kv_key, kv_key_with_prefix, kv_value, measure_with_counters, report_counters,
    report_percentiles, DurabilityConfig, PERCENTILE_SAMPLES, WARMUP_COUNT,
};

static RECORDER: Mutex<Option<ResultRecorder>> = Mutex::new(None);

fn kv_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("kv/put");
    group.throughput(Throughput::Elements(1));

    eprintln!("\n--- Latency Percentiles: kv/put ---");
    for mode in DurabilityConfig::ALL {
        let bench_db = create_db(mode);
        let counter = AtomicU64::new(0);
        group.bench_function(BenchmarkId::new("durability", mode.label()), |b| {
            b.iter(|| {
                let i = counter.fetch_add(1, Ordering::Relaxed);
                bench_db.db.kv_put(&kv_key(i), kv_value()).unwrap();
            });
        });

        let pct_counter = AtomicU64::new(u64::MAX / 2);
        let label = format!("kv/put/{}", mode.label());
        let (p, counters) = measure_with_counters(&bench_db, PERCENTILE_SAMPLES, || {
            let i = pct_counter.fetch_add(1, Ordering::Relaxed);
            bench_db.db.kv_put(&kv_key(i), kv_value()).unwrap();
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

fn kv_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("kv/get");
    group.throughput(Throughput::Elements(1));

    eprintln!("\n--- Latency Percentiles: kv/get ---");
    for mode in DurabilityConfig::ALL {
        let bench_db = create_db(mode);
        for i in 0..WARMUP_COUNT {
            bench_db.db.kv_put(&kv_key(i), kv_value()).unwrap();
        }
        let counter = AtomicU64::new(0);
        group.bench_function(BenchmarkId::new("durability", mode.label()), |b| {
            b.iter(|| {
                let i = counter.fetch_add(1, Ordering::Relaxed) % WARMUP_COUNT;
                bench_db.db.kv_get(&kv_key(i)).unwrap();
            });
        });

        let pct_counter = AtomicU64::new(0);
        let label = format!("kv/get/{}", mode.label());
        let (p, counters) = measure_with_counters(&bench_db, PERCENTILE_SAMPLES, || {
            let i = pct_counter.fetch_add(1, Ordering::Relaxed) % WARMUP_COUNT;
            bench_db.db.kv_get(&kv_key(i)).unwrap();
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

fn kv_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("kv/delete");
    group.throughput(Throughput::Elements(1));

    eprintln!("\n--- Latency Percentiles: kv/delete ---");
    for mode in DurabilityConfig::ALL {
        let bench_db = create_db(mode);
        let counter = AtomicU64::new(0);
        group.bench_function(BenchmarkId::new("durability", mode.label()), |b| {
            b.iter(|| {
                let i = counter.fetch_add(1, Ordering::Relaxed);
                let key = kv_key(i);
                bench_db.db.kv_put(&key, kv_value()).unwrap();
                bench_db.db.kv_delete(&key).unwrap();
            });
        });

        let pct_counter = AtomicU64::new(u64::MAX / 2);
        let label = format!("kv/delete/{}", mode.label());
        let (p, counters) = measure_with_counters(&bench_db, PERCENTILE_SAMPLES, || {
            let i = pct_counter.fetch_add(1, Ordering::Relaxed);
            let key = kv_key(i);
            bench_db.db.kv_put(&key, kv_value()).unwrap();
            bench_db.db.kv_delete(&key).unwrap();
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

fn kv_list_prefix(c: &mut Criterion) {
    let mut group = c.benchmark_group("kv/list_prefix");
    group.throughput(Throughput::Elements(1));

    eprintln!("\n--- Latency Percentiles: kv/list_prefix ---");
    for mode in DurabilityConfig::ALL {
        let bench_db = create_db(mode);
        for i in 0..1000u64 {
            bench_db
                .db
                .kv_put(&kv_key_with_prefix("alpha:", i), kv_value())
                .unwrap();
            bench_db
                .db
                .kv_put(&kv_key_with_prefix("beta:", i), kv_value())
                .unwrap();
        }
        group.bench_function(BenchmarkId::new("durability", mode.label()), |b| {
            b.iter(|| {
                bench_db.db.kv_list(Some("alpha:")).unwrap();
            });
        });

        let label = format!("kv/list_prefix/{}", mode.label());
        let (p, counters) = measure_with_counters(&bench_db, PERCENTILE_SAMPLES, || {
            bench_db.db.kv_list(Some("alpha:")).unwrap();
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

criterion_group!(benches, kv_put, kv_get, kv_delete, kv_list_prefix);

fn main() {
    *RECORDER.lock().unwrap() = Some(ResultRecorder::new("latency"));
    benches();
    if let Some(recorder) = RECORDER.lock().unwrap().take() {
        let _ = recorder.save();
    }
}
