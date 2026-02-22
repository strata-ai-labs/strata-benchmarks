//! LDBC Graphalytics dataset parser.
//!
//! Parses the standard LDBC file formats:
//! - `.v`  — one vertex ID (u64) per line
//! - `.e`  — `src dst` per line (space-separated u64 pair)
//! - `.properties` — Java properties format with graph metadata
//! - BFS reference — `vertex_id depth` per line

use std::collections::HashMap;
use std::path::Path;

/// Sentinel value for unreachable vertices in LDBC BFS output.
pub const UNREACHABLE: i64 = 9223372036854775807; // i64::MAX

/// An LDBC Graphalytics dataset (vertices + edges + metadata).
pub struct LdbcDataset {
    pub vertices: Vec<u64>,
    pub edges: Vec<(u64, u64)>,
    pub directed: bool,
    pub name: String,
    pub bfs_source: Option<u64>,
}

/// BFS reference output for validation.
pub struct BfsReference {
    pub source: u64,
    pub depths: HashMap<u64, i64>, // i64 to hold UNREACHABLE sentinel
}

impl LdbcDataset {
    /// Load an LDBC dataset from a directory.
    ///
    /// Expects files named `<name>.v`, `<name>.e`, and optionally `<name>.properties`
    /// where `<name>` is the directory's basename.
    pub fn load(dir: &Path) -> Result<Self, String> {
        let name = dir
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| "invalid dataset directory name".to_string())?
            .to_string();

        let v_path = dir.join(format!("{}.v", name));
        let e_path = dir.join(format!("{}.e", name));
        let props_path = dir.join(format!("{}.properties", name));

        // Parse vertices
        let v_content = std::fs::read_to_string(&v_path)
            .map_err(|e| format!("failed to read {}: {}", v_path.display(), e))?;
        let vertices: Vec<u64> = v_content
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| {
                l.trim()
                    .parse::<u64>()
                    .map_err(|e| format!("bad vertex id '{}': {}", l.trim(), e))
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Parse edges
        let e_content = std::fs::read_to_string(&e_path)
            .map_err(|e| format!("failed to read {}: {}", e_path.display(), e))?;
        let edges: Vec<(u64, u64)> = e_content
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| {
                let parts: Vec<&str> = l.trim().split_whitespace().collect();
                if parts.len() != 2 {
                    return Err(format!("bad edge line: '{}'", l.trim()));
                }
                let src = parts[0]
                    .parse::<u64>()
                    .map_err(|e| format!("bad edge src '{}': {}", parts[0], e))?;
                let dst = parts[1]
                    .parse::<u64>()
                    .map_err(|e| format!("bad edge dst '{}': {}", parts[1], e))?;
                Ok((src, dst))
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Parse properties (optional)
        let mut directed = true;
        let mut bfs_source = None;
        let mut expected_vertices: Option<usize> = None;
        let mut expected_edges: Option<usize> = None;

        if props_path.exists() {
            let props_content = std::fs::read_to_string(&props_path)
                .map_err(|e| format!("failed to read {}: {}", props_path.display(), e))?;

            for line in props_content.lines() {
                let line = line.trim();
                if line.is_empty() || line.starts_with('#') {
                    continue;
                }
                if let Some((key, value)) = line.split_once('=') {
                    let key = key.trim();
                    let value = value.trim();
                    match key {
                        "graph.directed" => directed = value == "true",
                        "meta.vertices" => expected_vertices = value.parse().ok(),
                        "meta.edges" => expected_edges = value.parse().ok(),
                        "algorithms.bfs.source-vertex" => bfs_source = value.parse().ok(),
                        _ => {}
                    }
                }
            }
        }

        // Validate counts if properties file provided them
        if let Some(ev) = expected_vertices {
            if vertices.len() != ev {
                return Err(format!(
                    "vertex count mismatch: file has {}, properties says {}",
                    vertices.len(),
                    ev
                ));
            }
        }
        if let Some(ee) = expected_edges {
            if edges.len() != ee {
                return Err(format!(
                    "edge count mismatch: file has {}, properties says {}",
                    edges.len(),
                    ee
                ));
            }
        }

        Ok(LdbcDataset {
            vertices,
            edges,
            directed,
            name,
            bfs_source,
        })
    }
}

impl BfsReference {
    /// Load a BFS reference output file.
    ///
    /// Format: `vertex_id depth` per line, space-separated.
    /// The source vertex is inferred as the one with depth 0.
    pub fn load(path: &Path) -> Result<Self, String> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("failed to read {}: {}", path.display(), e))?;

        let mut depths = HashMap::new();
        let mut source = None;

        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() != 2 {
                return Err(format!("bad BFS reference line: '{}'", line));
            }
            let vid = parts[0]
                .parse::<u64>()
                .map_err(|e| format!("bad vertex id '{}': {}", parts[0], e))?;
            let depth = parts[1]
                .parse::<i64>()
                .map_err(|e| format!("bad depth '{}': {}", parts[1], e))?;

            if depth == 0 {
                source = Some(vid);
            }
            depths.insert(vid, depth);
        }

        let source = source.ok_or_else(|| "no source vertex (depth=0) in BFS reference".to_string())?;

        Ok(BfsReference { source, depths })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn example_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data/graph/example-directed")
    }

    #[test]
    fn load_example_dataset() {
        let ds = LdbcDataset::load(&example_dir()).unwrap();
        assert_eq!(ds.name, "example-directed");
        assert_eq!(ds.vertices.len(), 10);
        assert_eq!(ds.edges.len(), 17);
        assert!(ds.directed);
        assert_eq!(ds.bfs_source, Some(1));
    }

    #[test]
    fn load_bfs_reference() {
        let path = example_dir().join("example-directed-BFS");
        let bfs = BfsReference::load(&path).unwrap();
        assert_eq!(bfs.source, 1);
        assert_eq!(bfs.depths.len(), 10);
        assert_eq!(bfs.depths[&1], 0);
        assert_eq!(bfs.depths[&2], 1);
        assert_eq!(bfs.depths[&3], 1);
        assert_eq!(bfs.depths[&4], 2);
    }

    #[test]
    fn no_unreachable_in_example() {
        let path = example_dir().join("example-directed-BFS");
        let bfs = BfsReference::load(&path).unwrap();
        for (_vid, &depth) in &bfs.depths {
            assert_ne!(depth, UNREACHABLE, "example dataset should have no unreachable vertices");
        }
    }
}
