//! Local fallback when the coordinator is unreachable at startup.
//! Runs `nix-eval-jobs ... | nix build ^*` in-process: we evaluate,
//! then realize every drv. Loses dedup but keeps CCI green.

use crate::error::{Error, Result};
use crate::runner::eval_jobs::{self, EvalMode};

/// Max drv paths per `nix build` invocation. Avoids `E2BIG` on
/// nixpkgs-scale closures (where a single expr can yield thousands of
/// top-level drvs and each argv entry is ~160 bytes — `argv` limits
/// are commonly 128 KiB on Linux, much less on some macOS configs).
const NIX_BUILD_CHUNK: usize = 256;

pub async fn run_local(mode: EvalMode, workers: u32) -> Result<()> {
    let eval_jobs::Spawned {
        mut rx,
        handle: eval_handle,
        kill: _kill,
    } = eval_jobs::spawn(mode, workers)?;
    let mut drv_paths: Vec<String> = Vec::new();
    while let Some(line) = rx.recv().await {
        if let Some(err) = &line.error {
            return Err(Error::Internal(format!("eval error: {err}")));
        }
        if let Some(p) = line.drv_path {
            drv_paths.push(p);
        }
    }
    eval_handle
        .await
        .map_err(|e| Error::Internal(e.to_string()))??;

    tracing::info!(
        drvs = drv_paths.len(),
        "coordinator unreachable: falling back to local realise"
    );

    if drv_paths.is_empty() {
        return Ok(());
    }

    // Chunk to avoid argv overflow. Build each chunk independently
    // with --keep-going so one chunk's failure doesn't suppress
    // subsequent chunks. First non-zero exit code is the reported
    // failure; all chunks run regardless.
    let mut first_failure_code: Option<i32> = None;
    for chunk in drv_paths.chunks(NIX_BUILD_CHUNK) {
        let mut cmd = tokio::process::Command::new("nix");
        cmd.arg("build")
            .arg("--no-link")
            .arg("--keep-going")
            .arg("--print-build-logs");
        for p in chunk {
            cmd.arg(format!("{p}^*"));
        }
        let status = cmd
            .status()
            .await
            .map_err(|e| Error::Internal(format!("spawn nix build: {e}")))?;
        if !status.success() && first_failure_code.is_none() {
            first_failure_code = Some(status.code().unwrap_or(1));
        }
    }
    if let Some(code) = first_failure_code {
        return Err(Error::Subprocess {
            tool: "nix build".into(),
            code,
        });
    }
    Ok(())
}
