//! Placeholder benchmark file. Will be expanded into the full benchmark suite
//! described in roadmap/performance-characterization.md.

use criterion::{criterion_group, criterion_main, Criterion};
use stratadb::Strata;

fn kv_put_benchmark(c: &mut Criterion) {
    let db = Strata::open_temp().unwrap();

    c.bench_function("kv_put", |b| {
        let mut i = 0u64;
        b.iter(|| {
            db.kv_put(&format!("key:{}", i), "value").unwrap();
            i += 1;
        });
    });
}

fn kv_get_benchmark(c: &mut Criterion) {
    let db = Strata::open_temp().unwrap();
    // Pre-populate
    for i in 0..10_000 {
        db.kv_put(&format!("key:{}", i), "value").unwrap();
    }

    c.bench_function("kv_get", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let _ = db.kv_get(&format!("key:{}", i % 10_000)).unwrap();
            i += 1;
        });
    });
}

criterion_group!(benches, kv_put_benchmark, kv_get_benchmark);
criterion_main!(benches);
