# strata-benchmarks

Black-box tests and performance benchmarks for StrataDB. Exercises the public API across all six primitives, three durability modes, and branch isolation. Correctness first, then performance characterization.

## Benchmark Categories

### [Latency](benches/latency/README.md)
Single-threaded latency for all six primitives (KV, State, Event, JSON, Vector, Branch) across three durability modes. Reports Criterion statistics and p50/p95/p99 percentiles with WAL counter breakdowns.

```bash
cargo bench --bench kv
cargo bench --bench state
cargo bench --bench event
cargo bench --bench json
cargo bench --bench vector
cargo bench --bench branch
```

### [Concurrency](benches/concurrency/README.md)
Multi-threaded scaling benchmarks — throughput and latency as a function of thread count. Includes read-only, write-only, hot-key contention, and mixed workloads.

```bash
cargo bench --bench concurrency
cargo bench --bench concurrency -- --threads 1,2,4
```

### [Redis Comparison](benches/redis-compare/README.md)
Runs the same operations as `redis-benchmark` using StrataDB's API for side-by-side comparison. Matches redis-benchmark's key format, payload sizes, and randomization behavior.

```bash
cargo bench --bench redis_compare
cargo bench --bench redis_compare -- -r 100000
```

### [Fill Level](benches/fill-level/README.md)
Measures how latency and throughput degrade as database size grows (0 to 250K keys). Shows the performance curve for put, get, delete, and JSON operations.

```bash
cargo bench --bench fill_level
cargo bench --bench fill_level -- --levels 0,1000,5000,10000
```

## Comparing Results

All benchmarks save structured JSON results to `results/`. Compare two runs:

```bash
cargo run --bin bench-compare -- results/baseline.json results/candidate.json
```

See [results/SCHEMA.md](results/SCHEMA.md) for the JSON format and cross-SDK compatibility guide.
