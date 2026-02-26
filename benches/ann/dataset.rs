//! Synthetic ANN dataset generation, brute-force ground truth, and recall computation.
//!
//! Uses a Gaussian Mixture Model to create clustered vectors that produce
//! realistic search difficulty (unlike uniform random vectors which are
//! nearly orthogonal in high dimensions).

// ---------------------------------------------------------------------------
// Fast LCG RNG (same as ycsb/workloads.rs)
// ---------------------------------------------------------------------------

pub struct FastRng {
    state: u64,
}

impl FastRng {
    pub fn new(seed: u64) -> Self {
        Self {
            state: seed ^ 0x5DEECE66D,
        }
    }

    #[inline]
    pub fn next_u64(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.state
    }

    #[inline]
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    #[inline]
    pub fn next_usize(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }

    /// Box-Muller transform: generate a standard normal sample from two uniform samples.
    #[inline]
    fn next_gaussian(&mut self) -> f64 {
        let u1 = self.next_f64().max(1e-10); // avoid log(0)
        let u2 = self.next_f64();
        (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()
    }
}

// ---------------------------------------------------------------------------
// Dataset types
// ---------------------------------------------------------------------------

#[allow(dead_code)]
pub struct AnnDataset {
    pub train_keys: Vec<String>,
    pub train_vectors: Vec<Vec<f32>>,
    pub query_vectors: Vec<Vec<f32>>,
    pub dim: usize,
}

#[allow(dead_code)]
pub struct GroundTruth {
    /// For each query, the indices into `train_vectors` of the k nearest neighbors.
    pub neighbors: Vec<Vec<usize>>,
    pub k: usize,
}

// ---------------------------------------------------------------------------
// Data generation (Gaussian Mixture Model)
// ---------------------------------------------------------------------------

fn l2_normalize(v: &mut [f32]) {
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 1e-10 {
        for x in v.iter_mut() {
            *x /= norm;
        }
    }
}

pub fn generate_dataset(n_train: usize, n_queries: usize, dim: usize, seed: u64) -> AnnDataset {
    let mut rng = FastRng::new(seed);
    let n_clusters = 10;
    let noise_std = 0.1;

    // Generate cluster centroids: uniform in [-1, 1], then L2-normalize
    let mut centroids: Vec<Vec<f32>> = Vec::with_capacity(n_clusters);
    for _ in 0..n_clusters {
        let mut c: Vec<f32> = (0..dim).map(|_| (rng.next_f64() * 2.0 - 1.0) as f32).collect();
        l2_normalize(&mut c);
        centroids.push(c);
    }

    // Generate training vectors
    let mut train_keys = Vec::with_capacity(n_train);
    let mut train_vectors = Vec::with_capacity(n_train);
    for i in 0..n_train {
        let cluster = rng.next_usize(n_clusters);
        let mut v: Vec<f32> = (0..dim)
            .map(|d| centroids[cluster][d] + (rng.next_gaussian() * noise_std) as f32)
            .collect();
        l2_normalize(&mut v);
        train_keys.push(format!("vec_{}", i));
        train_vectors.push(v);
    }

    // Generate query vectors (same distribution, separate from training)
    let mut query_vectors = Vec::with_capacity(n_queries);
    for _ in 0..n_queries {
        let cluster = rng.next_usize(n_clusters);
        let mut v: Vec<f32> = (0..dim)
            .map(|d| centroids[cluster][d] + (rng.next_gaussian() * noise_std) as f32)
            .collect();
        l2_normalize(&mut v);
        query_vectors.push(v);
    }

    AnnDataset {
        train_keys,
        train_vectors,
        query_vectors,
        dim,
    }
}

// ---------------------------------------------------------------------------
// Ground truth (brute-force)
// ---------------------------------------------------------------------------

fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

pub fn compute_ground_truth(dataset: &AnnDataset, k: usize) -> GroundTruth {
    let mut neighbors = Vec::with_capacity(dataset.query_vectors.len());

    for query in &dataset.query_vectors {
        // Cosine similarity = dot product for L2-normalized vectors
        let mut scores: Vec<(usize, f32)> = dataset
            .train_vectors
            .iter()
            .enumerate()
            .map(|(i, v)| (i, dot_product(query, v)))
            .collect();

        // Sort descending by score
        scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        let top_k: Vec<usize> = scores.iter().take(k).map(|(i, _)| *i).collect();
        neighbors.push(top_k);
    }

    GroundTruth { neighbors, k }
}

// ---------------------------------------------------------------------------
// Recall computation
// ---------------------------------------------------------------------------

/// Compute recall@k: fraction of true top-k neighbors found by ANN results.
/// `ann_results` is per-query list of keys returned by vector_search.
pub fn compute_recall(
    ann_results: &[Vec<String>],
    ground_truth: &GroundTruth,
    dataset: &AnnDataset,
) -> f64 {
    let mut total_recall = 0.0;
    let n = ann_results.len().min(ground_truth.neighbors.len());

    for i in 0..n {
        let k = ground_truth.neighbors[i].len();
        if k == 0 {
            continue;
        }

        // Convert ground truth indices to keys
        let gt_keys: Vec<&str> = ground_truth.neighbors[i]
            .iter()
            .map(|&idx| dataset.train_keys[idx].as_str())
            .collect();

        // Count how many ANN results are in the ground truth
        let hits = ann_results[i]
            .iter()
            .filter(|key| gt_keys.contains(&key.as_str()))
            .count();

        total_recall += hits as f64 / k as f64;
    }

    if n > 0 {
        total_recall / n as f64
    } else {
        0.0
    }
}
