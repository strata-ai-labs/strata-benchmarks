// Test harness for benches/graph/ldbc.rs
//
// The ldbc module lives inside a benchmark binary with harness=false,
// so its #[cfg(test)] tests never run via `cargo test --bench graph_bfs`.
// This file re-includes the module under the standard test harness.

#[path = "../benches/graph/ldbc.rs"]
mod ldbc;
