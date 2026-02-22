//! LDBC Graphalytics dataset parser.
//!
//! Parses the standard LDBC file formats:
//! - `.v`  — one vertex ID (u64) per line
//! - `.e`  — `src dst` per line (space-separated u64 pair)
//! - `.properties` — Java properties format with graph metadata
//! - BFS reference — `vertex_id depth` per line

use std::collections::{HashMap, VecDeque};
use std::path::Path;

use petgraph::graph::{NodeIndex, UnGraph};

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

    /// Build a petgraph undirected graph from this dataset.
    ///
    /// Returns the graph and a mapping from LDBC vertex ID to petgraph NodeIndex.
    pub fn to_petgraph(&self) -> (UnGraph<(), ()>, HashMap<u64, NodeIndex>) {
        let mut graph = UnGraph::new_undirected();
        let mut id_map: HashMap<u64, NodeIndex> = HashMap::with_capacity(self.vertices.len());

        for &vid in &self.vertices {
            let idx = graph.add_node(());
            id_map.insert(vid, idx);
        }

        for &(src, dst) in &self.edges {
            if let (Some(&si), Some(&di)) = (id_map.get(&src), id_map.get(&dst)) {
                graph.add_edge(si, di, ());
            }
        }

        (graph, id_map)
    }
}

/// Run BFS on a petgraph graph using a manual VecDeque-based traversal.
///
/// Returns a map from NodeIndex to BFS depth (0 for source).
pub fn petgraph_bfs(graph: &UnGraph<(), ()>, source: NodeIndex) -> HashMap<NodeIndex, usize> {
    let mut depths: HashMap<NodeIndex, usize> = HashMap::with_capacity(graph.node_count());
    let mut queue = VecDeque::new();

    depths.insert(source, 0);
    queue.push_back(source);

    while let Some(node) = queue.pop_front() {
        let d = depths[&node];
        for neighbor in graph.neighbors(node) {
            if !depths.contains_key(&neighbor) {
                depths.insert(neighbor, d + 1);
                queue.push_back(neighbor);
            }
        }
    }

    depths
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

    fn example_dataset() -> LdbcDataset {
        LdbcDataset::load(&example_dir()).unwrap()
    }

    // -----------------------------------------------------------------------
    // Dataset loading tests
    // -----------------------------------------------------------------------

    #[test]
    fn load_example_dataset() {
        let ds = example_dataset();
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

    // -----------------------------------------------------------------------
    // to_petgraph tests
    // -----------------------------------------------------------------------

    #[test]
    fn to_petgraph_node_count() {
        let ds = example_dataset();
        let (graph, id_map) = ds.to_petgraph();
        assert_eq!(graph.node_count(), 10);
        assert_eq!(id_map.len(), 10);
    }

    #[test]
    fn to_petgraph_edge_count() {
        let ds = example_dataset();
        let (graph, _) = ds.to_petgraph();
        // Each directed edge from the .e file becomes one undirected petgraph edge.
        // Pairs like (1,2) and (2,1) create two parallel undirected edges.
        assert_eq!(graph.edge_count(), 17);
    }

    #[test]
    fn to_petgraph_id_map_covers_all_vertices() {
        let ds = example_dataset();
        let (_, id_map) = ds.to_petgraph();
        for &vid in &ds.vertices {
            assert!(
                id_map.contains_key(&vid),
                "vertex {} missing from id_map",
                vid
            );
        }
    }

    #[test]
    fn to_petgraph_edges_are_traversable() {
        let ds = example_dataset();
        let (graph, id_map) = ds.to_petgraph();

        // Verify that for each original edge (src, dst), src and dst are
        // neighbors in the petgraph (undirected, so both directions).
        for &(src, dst) in &ds.edges {
            let si = id_map[&src];
            let di = id_map[&dst];
            let neighbors: Vec<_> = graph.neighbors(si).collect();
            assert!(
                neighbors.contains(&di),
                "edge ({}, {}): dst not in neighbors of src",
                src, dst
            );
        }
    }

    // -----------------------------------------------------------------------
    // petgraph_bfs tests
    // -----------------------------------------------------------------------

    #[test]
    fn petgraph_bfs_reaches_all_vertices() {
        let ds = example_dataset();
        let (graph, id_map) = ds.to_petgraph();
        let source = id_map[&1];
        let depths = petgraph_bfs(&graph, source);
        // All 10 vertices are reachable from vertex 1 in the undirected view
        assert_eq!(
            depths.len(),
            10,
            "BFS should reach all 10 vertices, reached {}",
            depths.len()
        );
    }

    #[test]
    fn petgraph_bfs_source_has_depth_zero() {
        let ds = example_dataset();
        let (graph, id_map) = ds.to_petgraph();
        let source = id_map[&1];
        let depths = petgraph_bfs(&graph, source);
        assert_eq!(depths[&source], 0);
    }

    #[test]
    fn petgraph_bfs_depths_match_ldbc_reference() {
        let ds = example_dataset();
        let (graph, id_map) = ds.to_petgraph();
        let source = id_map[&1];
        let depths = petgraph_bfs(&graph, source);

        // Expected depths from the LDBC reference file (BFS from vertex 1,
        // treating edges as undirected):
        let expected: HashMap<u64, usize> = [
            (1, 0),
            (2, 1),
            (3, 1),
            (4, 2),
            (5, 3),
            (6, 3),
            (7, 4),
            (8, 4),
            (9, 5),
            (10, 5),
        ]
        .into_iter()
        .collect();

        for (&vid, &expected_depth) in &expected {
            let idx = id_map[&vid];
            let actual = depths.get(&idx).copied();
            assert_eq!(
                actual,
                Some(expected_depth),
                "vertex {}: expected depth {}, got {:?}",
                vid,
                expected_depth,
                actual
            );
        }
    }

    #[test]
    fn petgraph_bfs_isolated_vertex() {
        // Build a graph with 3 nodes but only an edge between 0 and 1.
        // BFS from node 2 should only reach node 2.
        let mut graph = UnGraph::new_undirected();
        let n0 = graph.add_node(());
        let n1 = graph.add_node(());
        let n2 = graph.add_node(());
        graph.add_edge(n0, n1, ());

        let depths = petgraph_bfs(&graph, n2);
        assert_eq!(depths.len(), 1, "isolated source should only reach itself");
        assert_eq!(depths[&n2], 0);
    }

    #[test]
    fn petgraph_bfs_disconnected_components() {
        // Two disconnected pairs: (0,1) and (2,3)
        let mut graph = UnGraph::new_undirected();
        let n0 = graph.add_node(());
        let n1 = graph.add_node(());
        let n2 = graph.add_node(());
        let n3 = graph.add_node(());
        graph.add_edge(n0, n1, ());
        graph.add_edge(n2, n3, ());

        // BFS from n0 should reach n0 and n1, but NOT n2 or n3
        let depths = petgraph_bfs(&graph, n0);
        assert_eq!(depths.len(), 2);
        assert_eq!(depths[&n0], 0);
        assert_eq!(depths[&n1], 1);
        assert!(!depths.contains_key(&n2));
        assert!(!depths.contains_key(&n3));
    }

    #[test]
    fn petgraph_bfs_single_node() {
        let mut graph = UnGraph::new_undirected();
        let n0 = graph.add_node(());

        let depths = petgraph_bfs(&graph, n0);
        assert_eq!(depths.len(), 1);
        assert_eq!(depths[&n0], 0);
    }

    #[test]
    fn petgraph_bfs_linear_chain() {
        // 0 -- 1 -- 2 -- 3 -- 4
        let mut graph = UnGraph::new_undirected();
        let nodes: Vec<_> = (0..5).map(|_| graph.add_node(())).collect();
        for i in 0..4 {
            graph.add_edge(nodes[i], nodes[i + 1], ());
        }

        let depths = petgraph_bfs(&graph, nodes[0]);
        assert_eq!(depths.len(), 5);
        for (i, node) in nodes.iter().enumerate() {
            assert_eq!(depths[node], i, "node {} should be at depth {}", i, i);
        }
    }
}
