# Redis Comparison Benchmarks

Runs the same operations as `redis-benchmark` (the default Redis benchmarking tool) using StrataDB's API, so results can be placed side-by-side for an apples-to-apples comparison.

## Operations

Mirrors the default `redis-benchmark` suite:

| Operation | Description |
|-----------|-------------|
| PING_INLINE | No-op baseline (Strata: `kv_get` on missing key) |
| PING_MBULK | No-op baseline (same as PING_INLINE for Strata) |
| SET | `kv_put` with 3-byte payload (matches redis-benchmark default) |
| GET | `kv_get` on previously set keys |
| INCR | `state_cas` increment (closest Strata equivalent) |
| LPUSH | `event_append` (closest Strata equivalent) |
| RPUSH | `event_append` (same primitive) |
| LPOP / RPOP | `event_read` by sequence |
| SADD | `kv_put` for set member storage |
| HSET | `json_set` at a path |
| MSET | Batch of 10 `kv_put` operations |

## Methodology

- **Key format**: Matches redis-benchmark's `key:NNNNNNNNNNNN` (12-digit zero-padded)
- **Randomization**: Default is fixed key (like redis-benchmark); use `-r <keyspace>` for random keys
- **Payload**: 3-byte random data by default (matches redis-benchmark), configurable via `-d`
- **Requests**: 100,000 per test by default, configurable via `-n`

## Running

```bash
# Full run (matches redis-benchmark defaults)
cargo bench --bench redis_compare

# Random keys across 100K keyspace
cargo bench --bench redis_compare -- -r 100000

# Quick run with specific durability
cargo bench --bench redis_compare -- --durability cache -q

# CSV output
cargo bench --bench redis_compare -- --csv
```

## Output

Results are saved to `results/redis-compare-<timestamp>-<commit>.json`.
