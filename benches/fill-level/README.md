# Fill-Level Benchmarks

Measures how operation latency and throughput degrade as database size grows. Tests run at increasing fill levels (number of pre-existing keys), showing the performance curve for each operation.

## Operations

| Operation | Description |
|-----------|-------------|
| kv_put | Write a new key-value pair |
| kv_get | Read a random existing key |
| kv_delete | Delete a random existing key |
| json_set | Write a JSON document at root path |
| json_get | Read a JSON document |

## Methodology

- **Fill levels**: 0, 10K, 50K, 100K, 250K pre-existing keys (customizable via `--levels`)
- **Measurement**: 10,000 operations per fill level per test
- **Value size**: 64 bytes (to focus on engine overhead, not payload I/O)
- **Durability**: Configurable via `--durability` (defaults to all three modes)
- **Latency**: Reports p50, p95, p99, min, max, avg, and ops/sec

## Running

```bash
# Full run (all levels, all operations)
cargo bench --bench fill_level

# Quick run (fewer operations)
cargo bench --bench fill_level -- -q

# Custom fill levels
cargo bench --bench fill_level -- --levels 0,1000,5000,10000

# Single operation
cargo bench --bench fill_level -- -t kv_put

# CSV output
cargo bench --bench fill_level -- --csv
```

## Output

Results are saved to `results/fill-level-<timestamp>-<commit>.json`.
