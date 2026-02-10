# Benchmark Results Schema

All benchmark categories produce JSON result files with a shared schema. This enables cross-run comparisons and future cross-SDK (Python, Node) compatibility.

## File Naming

```
results/<category>-<YYYY-MM-DDThh-mm-ss>Z-<commit>.json
```

Examples:
- `results/latency-2025-01-15T14-30-00Z-abc1234.json`
- `results/concurrency-2025-01-15T14-35-00Z-abc1234.json`

## JSON Structure

```json
{
  "schema_version": 1,
  "metadata": {
    "timestamp": "2025-01-15T14:30:00Z",
    "git_commit": "abc1234",
    "git_branch": "main",
    "git_dirty": false,
    "sdk": "rust",
    "sdk_version": "0.1.0",
    "hardware": {
      "cpu": "AMD Ryzen 9 7950X",
      "cores": 32,
      "ram_gb": 64,
      "os": "linux",
      "arch": "x86_64"
    }
  },
  "results": [
    {
      "benchmark": "kv/put/128B/cache",
      "category": "latency",
      "parameters": {
        "durability": "cache",
        "value_size": "128B"
      },
      "metrics": {
        "p50_ns": 1500,
        "p95_ns": 3200,
        "p99_ns": 8500,
        "min_ns": 900,
        "max_ns": 150000,
        "avg_ns": 2100,
        "samples": 1000,
        "wal_appends_per_op": 1.0,
        "wal_syncs_per_op": 0.0
      }
    }
  ]
}
```

## Field Reference

### Top Level

| Field | Type | Description |
|-------|------|-------------|
| `schema_version` | `u32` | Always `1` for this version |
| `metadata` | object | Run environment and git info |
| `results` | array | Individual benchmark measurements |

### `metadata`

| Field | Type | Description |
|-------|------|-------------|
| `timestamp` | string | ISO 8601 UTC timestamp |
| `git_commit` | string? | Short commit hash (omitted if not in a repo) |
| `git_branch` | string? | Branch name |
| `git_dirty` | bool? | `true` if uncommitted changes |
| `sdk` | string | SDK identifier: `"rust"`, `"python"`, `"node"` |
| `sdk_version` | string | Crate/package version |
| `hardware.cpu` | string | CPU model string |
| `hardware.cores` | int | Logical core count |
| `hardware.ram_gb` | int | Total RAM in GB |
| `hardware.os` | string | OS identifier |
| `hardware.arch` | string | CPU architecture |

### `results[]`

| Field | Type | Description |
|-------|------|-------------|
| `benchmark` | string | Unique name (e.g. `"kv/put/128B/cache"`) |
| `category` | string | One of: `latency`, `concurrency`, `redis-compare`, `fill-level` |
| `parameters` | object | Benchmark-specific key-value pairs |
| `metrics` | object | Measured values (all optional) |

### `metrics`

All fields are optional. Only fields relevant to the benchmark type are present.

| Field | Type | Used By | Description |
|-------|------|---------|-------------|
| `ops_per_sec` | float | concurrency, redis-compare, fill-level | Operations per second |
| `p50_ns` | int | all | Median latency in nanoseconds |
| `p95_ns` | int | all | 95th percentile latency |
| `p99_ns` | int | all | 99th percentile latency |
| `min_ns` | int | all | Minimum latency |
| `max_ns` | int | all | Maximum latency |
| `avg_ns` | int | all | Mean latency |
| `samples` | int | all | Number of measurements |
| `wal_appends_per_op` | float | latency | WAL append count per operation |
| `wal_syncs_per_op` | float | latency | WAL fsync count per operation |
| `threads` | int | concurrency | Thread count for this measurement |
| `abort_rate_pct` | float | concurrency | Transaction abort percentage |
| `fill_level` | int | fill-level | Number of pre-existing keys |

## Cross-SDK Compatibility

Python and Node SDK benchmarks should produce files matching this schema:
- Set `sdk` to `"python"` or `"node"`
- Use the same benchmark naming conventions
- Include at minimum: `p50_ns`, `p95_ns`, `p99_ns`, `samples`
- The `bench-compare` tool can compare results across SDKs

## Comparing Results

```bash
cargo run --bin bench-compare -- results/baseline.json results/candidate.json
```

The comparison tool matches benchmarks by name and reports percentage deltas for latency and throughput.
