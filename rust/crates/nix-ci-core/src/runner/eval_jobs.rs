//! Thin wrapper around `nix-eval-jobs`.
//!
//! Returns a `KillHandle` alongside the event receiver so the
//! orchestrator can `start_kill()` on shutdown — without it, a
//! SIGTERM'd `nix-ci run` would leave the evaluator child running
//! to completion before anything else could exit.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;

use crate::error::{Error, Result};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EvalLine {
    /// Full drv path, e.g. `/nix/store/…-hello-2.12.1.drv`.
    #[serde(default)]
    pub drv_path: Option<String>,
    /// Attribute name.
    #[serde(default)]
    pub attr: Option<String>,
    /// Name portion, e.g. `hello-2.12.1`.
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
    /// Cache-status hint (Cached | Local | NotBuilt) — nix-eval-jobs
    /// 2.x populates `cacheStatus` as a string.
    #[serde(default)]
    pub cache_status: Option<serde_json::Value>,
    /// Boolean legacy equivalent (`isCached`) — older nix-eval-jobs only.
    #[serde(default)]
    pub is_cached: Option<bool>,
    /// Flattened set of drv paths in this attr's closure that aren't
    /// substitutable from any configured cache (from
    /// `--check-cache-status`). Empty on older nix-eval-jobs.
    #[serde(default)]
    pub needed_builds: Vec<String>,
}

pub enum EvalMode {
    Flake { path: String, attrs: Vec<String> },
    Expr(String),
}

/// Handle the orchestrator can use to terminate a running `Child`. The
/// `Child` itself lives inside the tokio task driving it to completion;
/// `KillHandle` only exposes the lifecycle operations the caller needs.
#[derive(Clone)]
pub struct KillHandle {
    inner: Arc<Mutex<Option<Child>>>,
}

impl KillHandle {
    /// Request termination. Idempotent: subsequent calls are no-ops.
    pub async fn kill(&self) {
        let mut guard = self.inner.lock().await;
        if let Some(child) = guard.as_mut() {
            let _ = child.start_kill();
        }
    }
}

pub struct Spawned {
    pub rx: mpsc::Receiver<EvalLine>,
    pub handle: JoinHandle<Result<()>>,
    pub kill: KillHandle,
}

/// `eval_stderr_file`: when `Some`, nix-eval-jobs stderr is redirected
/// to this file instead of inheriting the process stderr. Keeps the
/// runner's OutputRenderer output clean; noisy eval diagnostics land
/// in a file CCI can pick up as a build artifact.
pub fn spawn(
    mode: EvalMode,
    workers: u32,
    eval_stderr_file: Option<&std::path::Path>,
) -> Result<Spawned> {
    let mut cmd = Command::new("nix-eval-jobs");
    cmd.arg("--workers")
        .arg(workers.to_string())
        .arg("--show-input-drvs")
        .arg("--check-cache-status");

    match &mode {
        EvalMode::Flake { path, attrs } => {
            cmd.arg("--flake").arg(path);
            for a in attrs {
                cmd.arg(a);
            }
        }
        EvalMode::Expr(e) => {
            cmd.arg("--expr").arg(e);
        }
    }
    let stderr = match eval_stderr_file {
        Some(path) => {
            let file = std::fs::File::create(path)
                .map_err(|e| Error::Internal(format!("create eval stderr file: {e}")))?;
            std::process::Stdio::from(file)
        }
        None => std::process::Stdio::inherit(),
    };
    cmd.kill_on_drop(true)
        .stdout(std::process::Stdio::piped())
        .stderr(stderr);

    let mut child = cmd
        .spawn()
        .map_err(|e| Error::Internal(format!("spawn nix-eval-jobs: {e}")))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| Error::Internal("nix-eval-jobs: no stdout".into()))?;

    let child_slot = Arc::new(Mutex::new(Some(child)));
    let kill = KillHandle {
        inner: child_slot.clone(),
    };

    let (tx, rx) = mpsc::channel::<EvalLine>(64);
    let driver_slot = child_slot;
    let handle: JoinHandle<Result<()>> = tokio::spawn(async move {
        let mut reader = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = reader.next_line().await {
            match serde_json::from_str::<EvalLine>(&line) {
                Ok(parsed) => {
                    if tx.send(parsed).await.is_err() {
                        // Receiver dropped. Kill the child — there's no
                        // one to read its output anymore and it'll
                        // eventually block on a full stdout pipe.
                        if let Some(c) = driver_slot.lock().await.as_mut() {
                            let _ = c.start_kill();
                        }
                        break;
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, line = %line, "nix-eval-jobs: bad line");
                }
            }
        }
        drop(tx);
        let status = {
            let mut guard = driver_slot.lock().await;
            match guard.take() {
                Some(mut c) => c.wait().await?,
                None => return Ok(()),
            }
        };
        if !status.success() {
            // Don't treat a killed-by-parent shutdown as an error.
            #[cfg(unix)]
            {
                use std::os::unix::process::ExitStatusExt;
                if status.signal() == Some(9) || status.signal() == Some(15) {
                    return Ok(());
                }
            }
            return Err(Error::Internal(format!(
                "nix-eval-jobs exited with {status}"
            )));
        }
        Ok(())
    });

    Ok(Spawned { rx, handle, kill })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_full_eval_line() {
        // nix-eval-jobs 2.x shape with all fields.
        let json = r#"{"drvPath":"/nix/store/abc-hello.drv","attr":"hello","name":"hello-2.12","cacheStatus":"notBuilt","neededBuilds":["/nix/store/abc-hello.drv","/nix/store/def-dep.drv"]}"#;
        let line: EvalLine = serde_json::from_str(json).unwrap();
        assert_eq!(line.drv_path.as_deref(), Some("/nix/store/abc-hello.drv"));
        assert_eq!(line.attr.as_deref(), Some("hello"));
        assert_eq!(line.name.as_deref(), Some("hello-2.12"));
        assert_eq!(line.needed_builds.len(), 2);
        assert!(line.error.is_none());
    }

    #[test]
    fn parse_eval_line_with_error_attr() {
        // Per-attr eval error: no drv_path, just attr + error.
        let json = r#"{"attr":"broken","error":"undefined variable"}"#;
        let line: EvalLine = serde_json::from_str(json).unwrap();
        assert_eq!(line.attr.as_deref(), Some("broken"));
        assert_eq!(line.error.as_deref(), Some("undefined variable"));
        assert!(line.drv_path.is_none());
    }

    #[test]
    fn parse_eval_line_with_legacy_is_cached_boolean() {
        // Older nix-eval-jobs populates `isCached` instead of cacheStatus.
        let json = r#"{"drvPath":"/nix/store/x-legacy.drv","attr":"x","isCached":true}"#;
        let line: EvalLine = serde_json::from_str(json).unwrap();
        assert_eq!(line.is_cached, Some(true));
        assert!(line.cache_status.is_none());
    }

    #[test]
    fn parse_eval_line_with_missing_neededbuilds_defaults_empty() {
        // `neededBuilds` is absent in older releases — must default
        // to empty rather than fail to parse.
        let json = r#"{"drvPath":"/nix/store/y.drv","attr":"y"}"#;
        let line: EvalLine = serde_json::from_str(json).unwrap();
        assert!(line.needed_builds.is_empty());
    }

    #[test]
    fn parse_eval_line_cachestatus_as_string_cached() {
        let json = r#"{"drvPath":"/nix/store/z.drv","attr":"z","cacheStatus":"cached"}"#;
        let line: EvalLine = serde_json::from_str(json).unwrap();
        match line.cache_status {
            Some(serde_json::Value::String(s)) => assert_eq!(s, "cached"),
            other => panic!("expected cached string, got {other:?}"),
        }
    }

    #[test]
    fn parse_eval_line_rejects_malformed_json() {
        let bad = "not json";
        assert!(serde_json::from_str::<EvalLine>(bad).is_err());
    }

    #[tokio::test]
    async fn kill_handle_is_idempotent_when_child_missing() {
        // Simulate post-exit state: inner slot holds None.
        let k = KillHandle {
            inner: Arc::new(Mutex::new(None)),
        };
        // First call is a no-op (no child).
        k.kill().await;
        // Second call must also not panic / deadlock.
        k.kill().await;
    }
}
