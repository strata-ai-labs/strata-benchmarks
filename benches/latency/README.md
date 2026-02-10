# Latency Benchmarks

Single-threaded latency benchmarks for all six StrataDB primitives. Each operation is measured across three durability modes (cache, flush, always) and reports both Criterion statistical measurements and explicit latency percentiles (p50, p95, p99).

## Operations Benchmarked

| File | Operations | Notes |
|------|-----------|-------|
| `kv.rs` | put, get, delete, list_prefix | Value-size sweep (128B, 1KB, 8KB) for put/get |
| `state.rs` | set, read, cas | 100-cell pool for set/read; CAS with version tracking |
| `event.rs` | append, read, read_by_type | Two event types for read_by_type filtering |
| `json.rs` | set_root, set_path, get, list | Root vs nested path writes; prefix-based listing |
| `vector.rs` | upsert, search, get | 128-dimension cosine similarity; reduced sample sizes |
| `branch.rs` | create, switch, delete | 100-branch pool for switch cycling |

## Methodology

- **Framework**: Criterion 0.5 for statistical benchmarks, plus explicit percentile collection
- **Durability modes**: `cache` (no fsync), `flush` (flush to OS), `always` (fsync every write)
- **Percentile samples**: 1,000 per measurement (200 for vector operations)
- **WAL counters**: appends/op and syncs/op reported alongside latency

## Running

```bash
# Run all latency benchmarks
cargo bench --bench kv
cargo bench --bench state
cargo bench --bench event
cargo bench --bench json
cargo bench --bench vector
cargo bench --bench branch

# Quick run (fewer iterations)
cargo bench --bench kv -- --quick
```

## Output

Results are saved to `results/latency-<timestamp>-<commit>.json`.
