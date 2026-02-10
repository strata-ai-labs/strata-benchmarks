//! Branch primitive benchmarks: create, switch, delete
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
    counter_delta, create_db, measure_with_counters, report_counters, report_percentiles,
    snapshot_counters, DurabilityConfig, PERCENTILE_SAMPLES,
};
use harness::measure_percentiles;

static RECORDER: Mutex<Option<ResultRecorder>> = Mutex::new(None);

fn branch_create(c: &mut Criterion) {
    let mut group = c.benchmark_group("branch/create");
    group.throughput(Throughput::Elements(1));

    eprintln!("\n--- Latency Percentiles: branch/create ---");
    for mode in DurabilityConfig::ALL {
        let bench_db = create_db(mode);
        let counter = AtomicU64::new(0);
        group.bench_function(BenchmarkId::new("durability", mode.label()), |b| {
            b.iter(|| {
                let i = counter.fetch_add(1, Ordering::Relaxed);
                bench_db
                    .db
                    .create_branch(&format!("bench_branch_{}", i))
                    .unwrap();
            });
        });

        let pct_counter = AtomicU64::new(u64::MAX / 2);
        let label = format!("branch/create/{}", mode.label());
        let (p, counters) = measure_with_counters(&bench_db, PERCENTILE_SAMPLES, || {
            let i = pct_counter.fetch_add(1, Ordering::Relaxed);
            bench_db
                .db
                .create_branch(&format!("bench_branch_{}", i))
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

fn branch_switch(c: &mut Criterion) {
    let mut group = c.benchmark_group("branch/switch");
    group.throughput(Throughput::Elements(1));

    eprintln!("\n--- Latency Percentiles: branch/switch ---");
    for mode in DurabilityConfig::ALL {
        let mut bench_db = create_db(mode);
        for i in 0..100u64 {
            bench_db
                .db
                .create_branch(&format!("switch_branch_{}", i))
                .unwrap();
        }
        let counter = AtomicU64::new(0);
        group.bench_function(BenchmarkId::new("durability", mode.label()), |b| {
            b.iter(|| {
                let i = counter.fetch_add(1, Ordering::Relaxed) % 100;
                bench_db
                    .db
                    .set_branch(&format!("switch_branch_{}", i))
                    .unwrap();
            });
        });

        let pct_counter = AtomicU64::new(0);
        let label = format!("branch/switch/{}", mode.label());
        // Manual counter snapshot because set_branch takes &mut self
        let before = snapshot_counters(&bench_db);
        let p = measure_percentiles(PERCENTILE_SAMPLES, || {
            let i = pct_counter.fetch_add(1, Ordering::Relaxed) % 100;
            bench_db
                .db
                .set_branch(&format!("switch_branch_{}", i))
                .unwrap();
        });
        let after = snapshot_counters(&bench_db);
        let counters = counter_delta(&before, &after);
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

fn branch_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("branch/delete");
    group.throughput(Throughput::Elements(1));

    eprintln!("\n--- Latency Percentiles: branch/delete ---");
    for mode in DurabilityConfig::ALL {
        let bench_db = create_db(mode);
        let counter = AtomicU64::new(0);
        group.bench_function(BenchmarkId::new("durability", mode.label()), |b| {
            b.iter(|| {
                let i = counter.fetch_add(1, Ordering::Relaxed);
                let name = format!("del_branch_{}", i);
                bench_db.db.create_branch(&name).unwrap();
                bench_db.db.delete_branch(&name).unwrap();
            });
        });

        let pct_counter = AtomicU64::new(u64::MAX / 2);
        let label = format!("branch/delete/{}", mode.label());
        let (p, counters) = measure_with_counters(&bench_db, PERCENTILE_SAMPLES, || {
            let i = pct_counter.fetch_add(1, Ordering::Relaxed);
            let name = format!("del_branch_{}", i);
            bench_db.db.create_branch(&name).unwrap();
            bench_db.db.delete_branch(&name).unwrap();
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

criterion_group!(benches, branch_create, branch_switch, branch_delete);

fn main() {
    *RECORDER.lock().unwrap() = Some(ResultRecorder::new("latency"));
    benches();
    if let Some(recorder) = RECORDER.lock().unwrap().take() {
        let _ = recorder.save();
    }
}
