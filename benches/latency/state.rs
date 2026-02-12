//! State primitive benchmarks: set, read, cas
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
    create_db, measure_with_counters, report_counters, report_percentiles, state_value,
    DurabilityConfig, PERCENTILE_SAMPLES,
};

static RECORDER: Mutex<Option<ResultRecorder>> = Mutex::new(None);

const CELL_POOL_SIZE: u64 = 100;

fn state_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("state/set");
    group.throughput(Throughput::Elements(1));

    eprintln!("\n--- Latency Percentiles: state/set ---");
    for mode in DurabilityConfig::ALL {
        let bench_db = create_db(mode);
        let counter = AtomicU64::new(0);
        group.bench_function(BenchmarkId::new("durability", mode.label()), |b| {
            b.iter(|| {
                let i = counter.fetch_add(1, Ordering::Relaxed) % CELL_POOL_SIZE;
                bench_db
                    .db
                    .state_set(&format!("cell_{}", i), state_value())
                    .unwrap();
            });
        });

        let pct_counter = AtomicU64::new(0);
        let label = format!("state/set/{}", mode.label());
        let (p, counters) = measure_with_counters(&bench_db, PERCENTILE_SAMPLES, || {
            let i = pct_counter.fetch_add(1, Ordering::Relaxed) % CELL_POOL_SIZE;
            bench_db
                .db
                .state_set(&format!("cell_{}", i), state_value())
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

fn state_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("state/read");
    group.throughput(Throughput::Elements(1));

    eprintln!("\n--- Latency Percentiles: state/read ---");
    for mode in DurabilityConfig::ALL {
        let bench_db = create_db(mode);
        for i in 0..CELL_POOL_SIZE {
            bench_db
                .db
                .state_set(&format!("cell_{}", i), state_value())
                .unwrap();
        }
        let counter = AtomicU64::new(0);
        group.bench_function(BenchmarkId::new("durability", mode.label()), |b| {
            b.iter(|| {
                let i = counter.fetch_add(1, Ordering::Relaxed) % CELL_POOL_SIZE;
                bench_db.db.state_get(&format!("cell_{}", i)).unwrap();
            });
        });

        let pct_counter = AtomicU64::new(0);
        let label = format!("state/read/{}", mode.label());
        let (p, counters) = measure_with_counters(&bench_db, PERCENTILE_SAMPLES, || {
            let i = pct_counter.fetch_add(1, Ordering::Relaxed) % CELL_POOL_SIZE;
            bench_db.db.state_get(&format!("cell_{}", i)).unwrap();
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

fn state_cas(c: &mut Criterion) {
    let mut group = c.benchmark_group("state/cas");
    group.throughput(Throughput::Elements(1));

    eprintln!("\n--- Latency Percentiles: state/cas ---");
    for mode in DurabilityConfig::ALL {
        let bench_db = create_db(mode);
        bench_db.db.state_set("cas_cell", state_value()).unwrap();
        let version_counter = AtomicU64::new(1);
        group.bench_function(BenchmarkId::new("durability", mode.label()), |b| {
            b.iter(|| {
                let expected = version_counter.load(Ordering::Relaxed);
                let result = bench_db
                    .db
                    .state_cas("cas_cell", Some(expected), state_value())
                    .unwrap();
                if let Some(new_version) = result {
                    version_counter.store(new_version, Ordering::Relaxed);
                }
            });
        });

        let label = format!("state/cas/{}", mode.label());
        let (p, counters) = measure_with_counters(&bench_db, PERCENTILE_SAMPLES, || {
            let expected = version_counter.load(Ordering::Relaxed);
            let result = bench_db
                .db
                .state_cas("cas_cell", Some(expected), state_value())
                .unwrap();
            if let Some(new_version) = result {
                version_counter.store(new_version, Ordering::Relaxed);
            }
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

criterion_group!(benches, state_set, state_read, state_cas);

fn main() {
    *RECORDER.lock().unwrap() = Some(ResultRecorder::new("latency"));
    benches();
    if let Some(recorder) = RECORDER.lock().unwrap().take() {
        let _ = recorder.save();
    }
}
