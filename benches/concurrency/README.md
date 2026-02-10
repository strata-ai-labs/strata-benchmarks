# Concurrency Benchmarks

Multi-threaded scaling benchmarks that measure throughput and latency as a function of thread count. Uses a custom harness (not Criterion) to sweep across thread counts and measure contention effects.

## Workloads

| Workload | Description |
|----------|-------------|
| KV GET | Read-only, no contention. 100K pre-populated keys, random access. |
| KV PUT (independent) | Write-only, no contention. Each thread writes to its own key space. |
| KV PUT (hot key) | Write-only, maximum contention. All threads write to the same key. |
| Mixed 90/10 | 90% reads / 10% writes with low contention. Realistic read-heavy workload. |

## Methodology

- **Thread sweep**: 1, 2, 4, 8, ... up to 2x physical cores (customizable via `--threads`)
- **Durability modes**: cache, flush, always (all three run by default)
- **Measurement**: 1s warmup + 5s measurement per thread count
- **Latency sampling**: Reservoir sampling (10K samples per thread) for p50/p95/p99
- **Abort tracking**: Reports abort rate for contended workloads (hot key)

## Running

```bash
# Full run (all workloads, all durability modes, all thread counts)
cargo bench --bench concurrency

# Quick run (specific thread counts)
cargo bench --bench concurrency -- --threads 1,2,4
```

## Output

Results are saved to `results/concurrency-<timestamp>-<commit>.json`.
