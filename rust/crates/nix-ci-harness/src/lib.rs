//! L3.5 harness: shared types used by the mock_worker and driver
//! binaries. Not a public API — this crate is orchestration glue,
//! not a reusable library.

use std::time::Duration;

use nix_ci_core::types::IngestDrvRequest;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use serde::{Deserialize, Serialize};

pub const DEFAULT_SYSTEM: &str = "x86_64-linux";

/// Fixed-capacity latency sample collector. Samples are ms (f64).
/// Matches the tests/scale_xl.rs pattern but is shareable via Arc
/// across worker tasks (uses parking_lot::Mutex).
#[derive(Default)]
pub struct LatSamples {
    pub samples: parking_lot::Mutex<Vec<f64>>,
}

impl LatSamples {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&self, d: Duration) {
        self.samples.lock().push(d.as_secs_f64() * 1000.0);
    }

    /// Drain and summarize. Returns `None` if no samples were recorded.
    pub fn summary(&self) -> Option<LatSummary> {
        let mut vals = std::mem::take(&mut *self.samples.lock());
        if vals.is_empty() {
            return None;
        }
        vals.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let n = vals.len();
        let pct = |q: f64| vals[(((n - 1) as f64) * q).round() as usize];
        Some(LatSummary {
            count: n as u64,
            p50_ms: pct(0.50),
            p95_ms: pct(0.95),
            p99_ms: pct(0.99),
            max_ms: *vals.last().unwrap(),
            mean_ms: vals.iter().sum::<f64>() / n as f64,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatSummary {
    pub count: u64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
    pub mean_ms: f64,
}

/// Aggregate over multiple LatSummaries (e.g., rolling them up from
/// N containers). Approximates percentiles by weight-averaging — not
/// mathematically correct for tail, but "good enough" for harness
/// reporting where the authoritative tail comes from the single
/// coordinator /metrics endpoint.
impl LatSummary {
    pub fn merge(summaries: &[LatSummary]) -> Option<LatSummary> {
        if summaries.is_empty() {
            return None;
        }
        let count: u64 = summaries.iter().map(|s| s.count).sum();
        if count == 0 {
            return None;
        }
        let weight = |s: &LatSummary| s.count as f64 / count as f64;
        let p50 = summaries.iter().map(|s| s.p50_ms * weight(s)).sum();
        let p95 = summaries.iter().map(|s| s.p95_ms * weight(s)).sum();
        let p99 = summaries.iter().map(|s| s.p99_ms * weight(s)).sum();
        let mean = summaries.iter().map(|s| s.mean_ms * weight(s)).sum();
        let max = summaries.iter().map(|s| s.max_ms).fold(0.0_f64, f64::max);
        Some(LatSummary {
            count,
            p50_ms: p50,
            p95_ms: p95,
            p99_ms: p99,
            max_ms: max,
            mean_ms: mean,
        })
    }
}

/// DAG shape: `layers × width`, each drv in layer L depends on
/// `fan_in` random drvs from layer L-1. Layer 0 is leaves. The last
/// layer's drvs are marked `is_root=true`.
///
/// Returns drvs in ingest-safe order (deps before dependents).
pub fn layered_dag(layers: usize, width: usize, fan_in: usize, seed: u64) -> Vec<IngestDrvRequest> {
    assert!(layers >= 1);
    assert!(width >= 1);
    let mut rng = StdRng::seed_from_u64(seed);
    let mut out = Vec::with_capacity(layers * width);
    let mut prev_layer: Vec<String> = Vec::new();
    for l in 0..layers {
        let mut this_layer = Vec::with_capacity(width);
        for w in 0..width {
            // drv_hash must be a *.drv basename; coordinator parses
            // hash prefix out of filename via drv_hash_from_path.
            let hash = format!("{:013x}{:019x}", l as u64, w as u64);
            let name = format!("l{l}w{w}");
            let drv_path = format!("/nix/store/{hash}-{name}.drv");

            let deps: Vec<String> = if l == 0 {
                Vec::new()
            } else {
                let mut v: Vec<String> = prev_layer
                    .choose_multiple(&mut rng, fan_in.min(prev_layer.len()))
                    .cloned()
                    .collect();
                v.sort();
                v.dedup();
                v
            };

            let is_root = l == layers - 1;
            out.push(IngestDrvRequest {
                drv_path: drv_path.clone(),
                drv_name: name,
                system: DEFAULT_SYSTEM.into(),
                required_features: Vec::new(),
                input_drvs: deps,
                is_root,
                attr: None,
            });
            this_layer.push(drv_path);
        }
        prev_layer = this_layer;
    }
    out
}

// ─── Reports ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerReport {
    pub container_id: String,
    pub mode: String,
    pub worker_count: u32,
    pub wall_secs: f64,
    pub claims_success: u64,
    pub claims_empty: u64,
    pub completes_ok: u64,
    pub completes_err: u64,
    pub claim_lat: Option<LatSummary>,
    pub complete_lat: Option<LatSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriverReport {
    pub scenario: String,
    pub job_id: Option<String>,
    pub total_drvs: u64,
    pub wall_secs: f64,
    pub final_status: Option<String>,
    pub final_counts: Option<serde_json::Value>,
    pub coord_metrics: serde_json::Value,
    pub assertions: Vec<AssertionResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssertionResult {
    pub name: String,
    pub passed: bool,
    pub detail: String,
}

// ─── Coordinator /metrics scrape ───────────────────────────────────

/// Fetch /metrics and return a map of metric-line → value. Labels
/// are folded into the map key as part of the line (e.g.,
/// `nix_ci_builds_completed_total{outcome="success"}`).
pub async fn scrape_metrics(base_url: &str) -> anyhow::Result<std::collections::HashMap<String, f64>> {
    let body = reqwest::get(format!("{base_url}/metrics"))
        .await?
        .error_for_status()?
        .text()
        .await?;
    let mut out = std::collections::HashMap::new();
    for line in body.lines() {
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        let Some((head, value_str)) = line.rsplit_once(' ') else {
            continue;
        };
        let Ok(v) = value_str.parse::<f64>() else {
            continue;
        };
        out.insert(head.to_string(), v);
    }
    Ok(out)
}

/// Look up a metric with optional label constraints. Returns the
/// first match. Label ordering in the OpenMetrics output is stable
/// but we don't rely on it — we match on substring-presence.
pub fn metric_value(
    map: &std::collections::HashMap<String, f64>,
    name: &str,
    want_labels: &[(&str, &str)],
) -> Option<f64> {
    for (k, v) in map {
        let (mname, labels) = match k.find('{') {
            Some(i) => {
                let end = k.rfind('}')?;
                (&k[..i], Some(&k[i + 1..end]))
            }
            None => (k.as_str(), None),
        };
        if mname != name {
            continue;
        }
        if want_labels.is_empty() {
            return Some(*v);
        }
        let labels = labels.unwrap_or("");
        let all_present = want_labels.iter().all(|(lk, lv)| {
            let needle = format!("{lk}=\"{lv}\"");
            labels.contains(&needle)
        });
        if all_present {
            return Some(*v);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layered_dag_structure() {
        let drvs = layered_dag(3, 5, 2, 42);
        assert_eq!(drvs.len(), 15);
        // Layer 0: no deps
        for d in &drvs[0..5] {
            assert!(d.input_drvs.is_empty(), "layer 0 leaf has no deps");
            assert!(!d.is_root);
        }
        // Layer 2: all marked root
        for d in &drvs[10..15] {
            assert!(d.is_root);
            assert!(!d.input_drvs.is_empty());
        }
        // All deps resolve to previously-ingested drvs (acyclic + ordered)
        let mut seen = std::collections::HashSet::new();
        for d in &drvs {
            for dep in &d.input_drvs {
                assert!(seen.contains(dep), "dep {dep} referenced before ingest");
            }
            seen.insert(d.drv_path.clone());
        }
    }

    #[test]
    fn metric_value_matches_labels() {
        let mut map = std::collections::HashMap::new();
        map.insert("foo{a=\"1\",b=\"2\"}".to_string(), 42.0);
        map.insert("bar".to_string(), 7.0);
        assert_eq!(metric_value(&map, "bar", &[]), Some(7.0));
        assert_eq!(metric_value(&map, "foo", &[("a", "1")]), Some(42.0));
        assert_eq!(metric_value(&map, "foo", &[("b", "2")]), Some(42.0));
        assert_eq!(metric_value(&map, "foo", &[("a", "9")]), None);
    }

    #[test]
    fn lat_summary_merge_weighted() {
        let a = LatSummary {
            count: 100,
            p50_ms: 10.0,
            p95_ms: 20.0,
            p99_ms: 30.0,
            max_ms: 40.0,
            mean_ms: 12.0,
        };
        let b = LatSummary {
            count: 100,
            p50_ms: 20.0,
            p95_ms: 40.0,
            p99_ms: 60.0,
            max_ms: 80.0,
            mean_ms: 24.0,
        };
        let merged = LatSummary::merge(&[a, b]).unwrap();
        assert_eq!(merged.count, 200);
        assert!((merged.p50_ms - 15.0).abs() < 0.001);
        assert!((merged.max_ms - 80.0).abs() < 0.001);
    }
}
