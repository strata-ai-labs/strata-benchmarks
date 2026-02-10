//! Event primitive benchmarks: append, read, read_by_type
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
    create_db, event_payload, measure_with_counters, report_counters, report_percentiles,
    DurabilityConfig, PERCENTILE_SAMPLES, WARMUP_COUNT,
};

static RECORDER: Mutex<Option<ResultRecorder>> = Mutex::new(None);

fn event_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("event/append");
    group.throughput(Throughput::Elements(1));

    eprintln!("\n--- Latency Percentiles: event/append ---");
    for mode in DurabilityConfig::ALL {
        let bench_db = create_db(mode);
        group.bench_function(BenchmarkId::new("durability", mode.label()), |b| {
            b.iter(|| {
                bench_db
                    .db
                    .event_append("bench_event", event_payload())
                    .unwrap();
            });
        });

        let label = format!("event/append/{}", mode.label());
        let (p, counters) = measure_with_counters(&bench_db, PERCENTILE_SAMPLES, || {
            bench_db
                .db
                .event_append("bench_event", event_payload())
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

fn event_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("event/read");
    group.throughput(Throughput::Elements(1));

    eprintln!("\n--- Latency Percentiles: event/read ---");
    for mode in DurabilityConfig::ALL {
        let bench_db = create_db(mode);
        for _ in 0..WARMUP_COUNT {
            bench_db
                .db
                .event_append("bench_event", event_payload())
                .unwrap();
        }
        let counter = AtomicU64::new(0);
        group.bench_function(BenchmarkId::new("durability", mode.label()), |b| {
            b.iter(|| {
                let seq = counter.fetch_add(1, Ordering::Relaxed) % WARMUP_COUNT;
                bench_db.db.event_read(seq).unwrap();
            });
        });

        let pct_counter = AtomicU64::new(0);
        let label = format!("event/read/{}", mode.label());
        let (p, counters) = measure_with_counters(&bench_db, PERCENTILE_SAMPLES, || {
            let seq = pct_counter.fetch_add(1, Ordering::Relaxed) % WARMUP_COUNT;
            bench_db.db.event_read(seq).unwrap();
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

fn event_read_by_type(c: &mut Criterion) {
    let mut group = c.benchmark_group("event/read_by_type");
    group.throughput(Throughput::Elements(1));

    eprintln!("\n--- Latency Percentiles: event/read_by_type ---");
    for mode in DurabilityConfig::ALL {
        let bench_db = create_db(mode);
        for i in 0..1000u64 {
            let event_type = if i % 2 == 0 { "type_a" } else { "type_b" };
            bench_db
                .db
                .event_append(event_type, event_payload())
                .unwrap();
        }
        group.bench_function(BenchmarkId::new("durability", mode.label()), |b| {
            b.iter(|| {
                bench_db.db.event_read_by_type("type_a").unwrap();
            });
        });

        let label = format!("event/read_by_type/{}", mode.label());
        let (p, counters) = measure_with_counters(&bench_db, PERCENTILE_SAMPLES, || {
            bench_db.db.event_read_by_type("type_a").unwrap();
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

criterion_group!(benches, event_append, event_read, event_read_by_type);

fn main() {
    *RECORDER.lock().unwrap() = Some(ResultRecorder::new("latency"));
    benches();
    if let Some(recorder) = RECORDER.lock().unwrap().take() {
        let _ = recorder.save();
    }
}
