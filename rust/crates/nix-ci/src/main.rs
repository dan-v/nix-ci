use std::sync::Arc;

use clap::{Parser, Subcommand};
use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::config::{RunnerConfig, ServerConfig};
use nix_ci_core::runner::eval_jobs::EvalMode;
use nix_ci_core::runner::worker::{self, ClaimMode, WorkerConfig};
use nix_ci_core::runner::{self, RunArgs};
use nix_ci_core::{observability, server};
use tokio::sync::watch;

#[derive(Debug, Parser)]
#[command(name = "nix-ci", version, about = "Distributed Nix build coordinator")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Run the drop-in entrypoint: evaluate + build against the coordinator.
    Run(RunCmd),
    /// Run the coordinator server.
    Server(ServerCmd),
    /// Run a persistent fleet worker. Claims runnable drvs from any
    /// live job (FIFO oldest-first) and runs forever until SIGTERM.
    /// Use this on shared runners that host capacity for many jobs;
    /// each runner slot runs one `nix-ci worker`.
    Worker(WorkerCmd),
    /// Print the coordinator's admin snapshot.
    Status {
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        url: String,
    },
    /// Show the failure / success block for a single job, given its
    /// UUID or its caller-supplied external_ref. Same format the
    /// `nix-ci run` output footer points at.
    Show {
        #[arg(
            long,
            env = "NIX_CI_COORDINATOR",
            default_value = "http://127.0.0.1:8080"
        )]
        coordinator: String,
        /// Job UUID or external_ref (CCI build ID, PR slug, etc.).
        id_or_ref: String,
    },
    /// List recent jobs in a given status (default: failed). Cursor-
    /// paginates newest-first.
    List(ListCmd),
    /// Fetch the build log for a specific drv on a specific job. By
    /// default returns the most recent attempt; --all-attempts dumps
    /// every stored attempt with separators. Logs only exist for
    /// failed (or transient-retried) builds — successful builds do
    /// not upload.
    Logs(LogsCmd),
    /// List in-flight claims (worker observability). Sorted longest-
    /// running first — what's actually stuck shows at the top.
    Claims {
        #[arg(
            long,
            env = "NIX_CI_COORDINATOR",
            default_value = "http://127.0.0.1:8080"
        )]
        coordinator: String,
    },
}

#[derive(Debug, Parser)]
struct LogsCmd {
    #[arg(
        long,
        env = "NIX_CI_COORDINATOR",
        default_value = "http://127.0.0.1:8080"
    )]
    coordinator: String,
    /// Job UUID or external_ref. Resolved via the same path as
    /// `nix-ci show`.
    job: String,
    /// drv_hash (basename of the .drv path). Required for the default
    /// "show me the log" mode; omit to list every drv with stored
    /// logs (uses the job's failure list as the source of truth).
    #[arg(long)]
    drv: Option<String>,
    /// Specific claim_id to fetch (overrides drv-based resolution).
    #[arg(long)]
    claim: Option<String>,
    /// Print every stored attempt instead of just the most recent.
    #[arg(long)]
    all_attempts: bool,
}

#[derive(Debug, Parser)]
struct ListCmd {
    #[arg(
        long,
        env = "NIX_CI_COORDINATOR",
        default_value = "http://127.0.0.1:8080"
    )]
    coordinator: String,
    /// Status filter. Defaults to "failed" (the common operator query:
    /// "what broke recently?"). Common values: failed, done, cancelled.
    #[arg(long)]
    status: Option<String>,
    /// Only consider jobs that finished within this lookback window
    /// (e.g. "1h", "24h", "7d"). Defaults to all.
    #[arg(long)]
    since: Option<String>,
    #[arg(long, default_value_t = 50)]
    limit: u32,
}

#[derive(Debug, Parser)]
struct RunCmd {
    /// Flake path (or "." for cwd). Mutually exclusive with --expr.
    #[arg(long, conflicts_with = "expr")]
    flake: Option<String>,
    /// Nix expression. Mutually exclusive with --flake.
    #[arg(long)]
    expr: Option<String>,
    /// Coordinator URL.
    #[arg(
        long,
        env = "NIX_CI_COORDINATOR",
        default_value = "http://127.0.0.1:8080"
    )]
    coordinator: String,
    /// Max concurrent local builds.
    #[arg(long, env = "NIX_CI_MAX_PARALLEL")]
    max_parallel: Option<u32>,
    /// Worker system string.
    #[arg(long, env = "NIX_CI_SYSTEM")]
    system: Option<String>,
    /// Supported features (comma-separated).
    #[arg(long, env = "NIX_CI_FEATURES")]
    features: Option<String>,
    /// Number of nix-eval-jobs workers.
    #[arg(long, default_value_t = 4)]
    eval_workers: u32,
    /// Dry run: don't actually build, just report success.
    #[arg(long)]
    dry_run: bool,
    /// External reference (e.g. CCI build ID) stored with the job for ops.
    #[arg(long)]
    external_ref: Option<String>,
    /// Verbose output: emit per-drv started/built lines instead of
    /// the periodic-progress + immediate-failures default. Useful when
    /// debugging a specific build, noisy at scale.
    #[arg(short, long)]
    verbose: bool,
    /// Write failure logs and eval stderr to this directory for CI
    /// artifact collection. CCI picks them up automatically.
    /// Creates `build_logs/<drv_name>.log` for each failed drv and
    /// `eval.stderr` with nix-eval-jobs diagnostic output.
    #[arg(long)]
    artifacts_dir: Option<std::path::PathBuf>,
    /// Attributes to evaluate (positional). Required with --flake.
    attrs: Vec<String>,
}

#[derive(Debug, Parser)]
struct WorkerCmd {
    /// Coordinator URL.
    #[arg(
        long,
        env = "NIX_CI_COORDINATOR",
        default_value = "http://127.0.0.1:8080"
    )]
    coordinator: String,
    /// Max concurrent local builds. Defaults to 1, which is the safe
    /// choice for shared runners with multiple slots.
    #[arg(long, env = "NIX_CI_MAX_PARALLEL", default_value_t = 1)]
    max_parallel: u32,
    /// Worker system string (e.g. x86_64-linux). Defaults to runner
    /// config default.
    #[arg(long, env = "NIX_CI_SYSTEM")]
    system: Option<String>,
    /// Supported features (comma-separated).
    #[arg(long, env = "NIX_CI_FEATURES")]
    features: Option<String>,
    /// Dry run: don't actually invoke `nix build`, just report success.
    #[arg(long)]
    dry_run: bool,
}

#[derive(Debug, Parser)]
struct ServerCmd {
    /// JSON config file. All knobs in `ServerConfig` are exposed
    /// via this file. Optional — without it, defaults are used.
    /// Override order: defaults < this file < env vars < CLI flags.
    #[arg(long)]
    config: Option<std::path::PathBuf>,
    /// Dump the merged effective config and exit (defaults overlaid
    /// with the JSON file overlaid with env vars / CLI flags). Useful
    /// to confirm "what's actually live?" before starting the server.
    #[arg(long)]
    print_config: bool,
    /// Validate the merged config and exit. Returns 0 on success, 1
    /// with a diagnostic on failure. Suitable for CI hooks ahead of
    /// rolling out a config change.
    #[arg(long)]
    validate: bool,
    /// Database URL. Required ultimately; can be supplied here, via
    /// `DATABASE_URL`, or as a `database_url` field in the config
    /// file (in CLI > env > file priority).
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,
    /// Listen address. Defaults to whatever the config file specifies
    /// (or `127.0.0.1:8080` if no file).
    #[arg(long)]
    listen: Option<String>,
    #[arg(long)]
    lock_key: Option<i64>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    // Service-name picked per subcommand so OTLP traces from the same
    // host (operator dev box: `run` then `status`) split into honest
    // service buckets in Jaeger/Tempo.
    let service_name = match &cli.cmd {
        Cmd::Server(_) => observability::service::COORDINATOR,
        Cmd::Worker(_) => observability::service::WORKER,
        _ => observability::service::RUNNER,
    };
    observability::init_tracing(service_name);
    // Install AFTER tracing so panic records go through the real
    // subscriber. Hook logs a structured record + bumps
    // `nix_ci_process_panics_total` before chaining to the default
    // hook (which aborts on panic). An operator alert on
    // `process_panics_total > 0` catches any coordinator / worker
    // panic in production.
    observability::install_panic_hook();

    match cli.cmd {
        Cmd::Run(cmd) => run_cmd(cmd).await,
        Cmd::Server(cmd) => server_cmd(cmd).await,
        Cmd::Worker(cmd) => worker_cmd(cmd).await,
        Cmd::Status { url } => status_cmd(&url).await,
        Cmd::Show {
            coordinator,
            id_or_ref,
        } => show_cmd(&coordinator, &id_or_ref).await,
        Cmd::List(cmd) => list_cmd(cmd).await,
        Cmd::Logs(cmd) => logs_cmd(cmd).await,
        Cmd::Claims { coordinator } => claims_cmd(&coordinator).await,
    }
}

async fn logs_cmd(cmd: LogsCmd) -> anyhow::Result<()> {
    let client = nix_ci_core::client::CoordinatorClient::new(&cmd.coordinator);
    // claim_id wins over drv: caller is asking for one specific
    // attempt and we already know the (job, claim) tuple.
    if let Some(claim_str) = cmd.claim.as_deref() {
        let claim_id = parse_claim_id(claim_str)?;
        let job_id = resolve_job_id(&client, &cmd.job).await?;
        let log = client.fetch_log(job_id, claim_id).await?;
        print!("{log}");
        return Ok(());
    }

    // No claim_id given: list attempts for the (job, drv) tuple, then
    // dump either the most recent or all of them.
    let drv = cmd
        .drv
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("--drv or --claim is required"))?;
    let job_id = resolve_job_id(&client, &cmd.job).await?;
    let drv_hash = nix_ci_core::types::DrvHash::new(drv.to_string());
    let listing = client.list_drv_logs(job_id, &drv_hash).await?;
    if listing.attempts.is_empty() {
        eprintln!("(no stored logs for drv {drv} on job {})", job_id);
        return Ok(());
    }
    let attempts = if cmd.all_attempts {
        listing.attempts.iter().collect::<Vec<_>>()
    } else {
        // attempts[] is already newest-first.
        vec![&listing.attempts[0]]
    };
    for (i, a) in attempts.iter().enumerate() {
        if cmd.all_attempts {
            println!(
                "==== attempt {} (claim {}, exit {:?}, {}{}) ====",
                a.attempt,
                a.claim_id,
                a.exit_code,
                if a.success { "success" } else { "failed" },
                if a.truncated { ", truncated" } else { "" }
            );
        }
        let log = client.fetch_log(job_id, a.claim_id).await?;
        print!("{log}");
        if !log.ends_with('\n') {
            println!();
        }
        if cmd.all_attempts && i + 1 < attempts.len() {
            println!();
        }
    }
    Ok(())
}

async fn resolve_job_id(
    client: &nix_ci_core::client::CoordinatorClient,
    s: &str,
) -> anyhow::Result<nix_ci_core::types::JobId> {
    if let Ok(uuid) = uuid::Uuid::parse_str(s) {
        return Ok(nix_ci_core::types::JobId(uuid));
    }
    // Fall back to external_ref → status snapshot → id field.
    let snap = client.show_job(s).await?;
    Ok(snap.id)
}

fn parse_claim_id(s: &str) -> anyhow::Result<nix_ci_core::types::ClaimId> {
    let uuid = uuid::Uuid::parse_str(s).map_err(|e| anyhow::anyhow!("bad --claim {s:?}: {e}"))?;
    Ok(nix_ci_core::types::ClaimId(uuid))
}

async fn claims_cmd(coordinator: &str) -> anyhow::Result<()> {
    let client = nix_ci_core::client::CoordinatorClient::new(coordinator);
    let resp = client.list_claims().await?;
    if resp.claims.is_empty() {
        println!("(no in-flight claims)");
        return Ok(());
    }
    println!(
        "{:<20} {:<10} {:<40} {:<10} deadline",
        "worker", "job", "drv", "elapsed"
    );
    for c in &resp.claims {
        let worker = c.worker_id.as_deref().unwrap_or("-");
        let job_short: String = c.job_id.0.to_string().chars().take(8).collect();
        let drv = c.drv_hash.as_str();
        let drv_short = if drv.len() > 40 {
            format!("{}…", &drv[..39])
        } else {
            drv.to_string()
        };
        let elapsed = format_elapsed(c.elapsed_ms);
        let deadline = c.deadline.to_rfc3339();
        println!(
            "{:<20} {:<10} {:<40} {:<10} {}",
            truncate(worker, 20),
            job_short,
            drv_short,
            elapsed,
            deadline
        );
    }
    Ok(())
}

fn format_elapsed(ms: u64) -> String {
    let s = ms / 1000;
    if s < 60 {
        format!("{s}s")
    } else if s < 3600 {
        format!("{}m{}s", s / 60, s % 60)
    } else {
        format!("{}h{}m", s / 3600, (s % 3600) / 60)
    }
}

async fn show_cmd(coordinator: &str, id_or_ref: &str) -> anyhow::Result<()> {
    let client = nix_ci_core::client::CoordinatorClient::new(coordinator);
    let snap = client.show_job(id_or_ref).await?;
    println!("job:    {}", snap.id);
    println!("status: {:?}", snap.status);
    println!("sealed: {}", snap.sealed);
    println!(
        "counts: total={} done={} failed={} pending={} building={}",
        snap.counts.total,
        snap.counts.done,
        snap.counts.failed,
        snap.counts.pending,
        snap.counts.building
    );
    if let Some(err) = &snap.eval_error {
        println!("eval_error: {err}");
    }
    if snap.failures.is_empty() {
        println!("failures: (none)");
    } else {
        let originating: Vec<_> = snap
            .failures
            .iter()
            .filter(|f| {
                !matches!(
                    f.error_category,
                    nix_ci_core::types::ErrorCategory::PropagatedFailure
                )
            })
            .collect();
        let propagated: Vec<_> = snap
            .failures
            .iter()
            .filter(|f| {
                matches!(
                    f.error_category,
                    nix_ci_core::types::ErrorCategory::PropagatedFailure
                )
            })
            .collect();
        println!("failures:");
        for f in &originating {
            println!("  {}", f.drv_name);
            if let Some(msg) = &f.error_message {
                println!("    reason: {msg}");
            }
        }
        if !propagated.is_empty() {
            let names: Vec<&str> = propagated
                .iter()
                .take(5)
                .map(|f| f.drv_name.as_str())
                .collect();
            let suffix = if propagated.len() > 5 {
                format!(", +{} more", propagated.len() - 5)
            } else {
                String::new()
            };
            println!(
                "  propagated ({}): {}{}",
                propagated.len(),
                names.join(", "),
                suffix
            );
        }
    }
    Ok(())
}

async fn list_cmd(cmd: ListCmd) -> anyhow::Result<()> {
    let status = cmd.status.clone().unwrap_or_else(|| "failed".to_string());
    let since = cmd
        .since
        .as_deref()
        .map(parse_duration_to_since)
        .transpose()?;
    let client = nix_ci_core::client::CoordinatorClient::new(cmd.coordinator);
    let resp = client.list_jobs(&status, since, None, cmd.limit).await?;
    if resp.jobs.is_empty() {
        println!("(no jobs match)");
        return Ok(());
    }
    println!("{:<10} {:<25} {:<22} failures", "id", "ext_ref", "done_at");
    for j in &resp.jobs {
        let id_short: String = j.id.0.to_string().chars().take(8).collect();
        let ext = j.external_ref.as_deref().unwrap_or("-");
        let done_at = j
            .done_at
            .map(|d| d.to_rfc3339())
            .unwrap_or_else(|| "-".into());
        let mut tail = j.originating_failures.join(", ");
        let extra = j.originating_failures_total as i64 - j.originating_failures.len() as i64;
        if extra > 0 {
            tail.push_str(&format!(" +{extra} more"));
        }
        if j.propagated_failures > 0 {
            tail.push_str(&format!(" (+{} propagated)", j.propagated_failures));
        }
        if tail.is_empty() {
            tail = "(no failure detail)".to_string();
        }
        println!(
            "{:<10} {:<25} {:<22} {}",
            id_short,
            truncate(ext, 25),
            done_at,
            tail
        );
    }
    if let Some(cursor) = resp.next_cursor {
        println!();
        println!(
            "(more available — re-run with --since {})",
            cursor.to_rfc3339()
        );
    }
    Ok(())
}

fn truncate(s: &str, n: usize) -> String {
    if s.len() <= n {
        s.to_string()
    } else {
        format!("{}…", &s[..n.saturating_sub(1)])
    }
}

/// Parse `1h`, `30m`, `7d` etc. and return the wall-clock time
/// `Utc::now() - dur` for use as a `since` filter.
fn parse_duration_to_since(s: &str) -> anyhow::Result<chrono::DateTime<chrono::Utc>> {
    let s = s.trim();
    if s.is_empty() {
        anyhow::bail!("empty --since");
    }
    let (num, unit) = s.split_at(s.len() - 1);
    let n: i64 = num
        .parse()
        .map_err(|e| anyhow::anyhow!("bad --since {s}: {e}"))?;
    let secs = match unit {
        "s" => n,
        "m" => n * 60,
        "h" => n * 3600,
        "d" => n * 86_400,
        _ => anyhow::bail!("bad --since unit {unit:?}; expected s/m/h/d"),
    };
    Ok(chrono::Utc::now() - chrono::Duration::seconds(secs))
}

async fn worker_cmd(cmd: WorkerCmd) -> anyhow::Result<()> {
    let runner_defaults = RunnerConfig::default();
    let system = cmd.system.unwrap_or(runner_defaults.system);
    let supported_features = cmd
        .features
        .map(|f| {
            f.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or(runner_defaults.supported_features);

    let client = Arc::new(CoordinatorClient::new(cmd.coordinator));
    let cfg = WorkerConfig {
        mode: ClaimMode::Fleet,
        system,
        supported_features,
        max_parallel: cmd.max_parallel,
        dry_run: cmd.dry_run,
        worker_id: Some(worker::default_worker_id()),
        tuning: nix_ci_core::runner::worker::WorkerTuning::default(),
    };

    // Wire SIGTERM / Ctrl-C to the worker's shutdown watch.
    let (sd_tx, sd_rx) = watch::channel(false);
    let signal_task = tokio::spawn(async move {
        wait_for_signal().await;
        let _ = sd_tx.send(true);
    });

    tracing::info!("nix-ci worker: starting fleet loop");
    let result = worker::run(client, cfg, sd_rx).await;
    signal_task.abort();
    result.map_err(|e| anyhow::anyhow!("nix-ci worker: {e}"))
}

/// Block on Ctrl-C or SIGTERM, whichever fires first.
async fn wait_for_signal() {
    use tokio::signal;
    let ctrl_c = async {
        let _ = signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut s) = signal::unix::signal(signal::unix::SignalKind::terminate()) {
            s.recv().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
}

async fn run_cmd(cmd: RunCmd) -> anyhow::Result<()> {
    let mode = match (cmd.flake.clone(), cmd.expr.clone()) {
        (Some(flake), None) => EvalMode::Flake {
            path: flake,
            attrs: cmd.attrs.clone(),
        },
        (None, Some(expr)) => EvalMode::Expr(expr),
        _ => {
            anyhow::bail!("exactly one of --flake or --expr must be specified");
        }
    };

    let mut cfg = RunnerConfig {
        coordinator_url: cmd.coordinator,
        eval_workers: cmd.eval_workers,
        dry_run: cmd.dry_run,
        verbose: cmd.verbose,
        ..RunnerConfig::default()
    };
    if let Some(n) = cmd.max_parallel {
        cfg.max_parallel = n;
    }
    if let Some(s) = cmd.system {
        cfg.system = s;
    }
    if let Some(f) = cmd.features {
        cfg.supported_features = f
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
    }

    let outcome = runner::run(RunArgs {
        mode,
        cfg,
        external_ref: cmd.external_ref,
        artifacts_dir: cmd.artifacts_dir,
    })
    .await?;

    match outcome.status {
        nix_ci_core::types::JobStatus::Done => Ok(()),
        nix_ci_core::types::JobStatus::Failed => {
            anyhow::bail!("nix-ci run: build failed")
        }
        nix_ci_core::types::JobStatus::Cancelled => {
            anyhow::bail!("nix-ci run: cancelled")
        }
        other => anyhow::bail!("nix-ci run: unexpected terminal status {:?}", other),
    }
}

async fn server_cmd(cmd: ServerCmd) -> anyhow::Result<()> {
    // Layering: defaults < JSON file < env vars < CLI flags.
    // Built bottom-up: load file (or defaults), then overlay any
    // CLI/env overrides that came in via clap.
    let mut cfg = if let Some(path) = cmd.config.as_deref() {
        ServerConfig::load_json(path).map_err(|e| anyhow::anyhow!("config load failed:\n{e}"))?
    } else {
        ServerConfig::default()
    };

    if let Some(url) = cmd.database_url {
        cfg.database_url = url;
    }
    if let Some(listen_str) = cmd.listen.as_deref() {
        cfg.listen = listen_str
            .parse::<std::net::SocketAddr>()
            .map_err(|e| anyhow::anyhow!("bad --listen: {e}"))?;
    }
    if let Some(k) = cmd.lock_key {
        cfg.lock_key = k;
    }
    // Load bearer tokens from file paths supplied via env (typically
    // systemd's `LoadCredential`). Must run after the JSON+CLI overlay
    // so an explicit file override wins; redaction in --print-config
    // then hides the plaintext.
    cfg.apply_bearer_files();

    if cmd.print_config {
        // No validation here — operator wants to see exactly what
        // they have, including invalid configs they're debugging.
        println!("{}", cfg.to_json_pretty());
        return Ok(());
    }

    cfg.validate().map_err(|e| {
        anyhow::anyhow!("ServerConfig validation failed (run with --validate to see all):\n{e}")
    })?;

    if cmd.validate {
        // Validation succeeded; print a confirmation and exit cleanly.
        eprintln!("config OK");
        return Ok(());
    }

    server::run(cfg).await?;
    Ok(())
}

async fn status_cmd(url: &str) -> anyhow::Result<()> {
    let client = nix_ci_core::client::CoordinatorClient::new(url);
    let snap = client.admin_snapshot().await?;
    println!("{:#?}", snap);
    Ok(())
}
