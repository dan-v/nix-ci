use clap::{Parser, Subcommand};
use nix_ci_core::config::{RunnerConfig, ServerConfig, NIX_CI_COORDINATOR_LOCK_KEY};
use nix_ci_core::runner::eval_jobs::EvalMode;
use nix_ci_core::runner::{self, RunArgs};
use nix_ci_core::{observability, server};

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
    /// Print the coordinator's admin snapshot.
    Status {
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        url: String,
    },
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
    /// Attributes to evaluate (positional). Required with --flake.
    attrs: Vec<String>,
}

#[derive(Debug, Parser)]
struct ServerCmd {
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,
    #[arg(long, default_value = "127.0.0.1:8080")]
    listen: String,
    #[arg(long, default_value_t = NIX_CI_COORDINATOR_LOCK_KEY)]
    lock_key: i64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    observability::init_tracing();

    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Run(cmd) => run_cmd(cmd).await,
        Cmd::Server(cmd) => server_cmd(cmd).await,
        Cmd::Status { url } => status_cmd(&url).await,
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
    let listen = cmd
        .listen
        .parse::<std::net::SocketAddr>()
        .map_err(|e| anyhow::anyhow!("bad --listen: {e}"))?;
    let cfg = ServerConfig {
        database_url: cmd.database_url,
        listen,
        lock_key: cmd.lock_key,
        ..ServerConfig::default()
    };
    server::run(cfg).await?;
    Ok(())
}

async fn status_cmd(url: &str) -> anyhow::Result<()> {
    let client = nix_ci_core::client::CoordinatorClient::new(url);
    let snap = client.admin_snapshot().await?;
    println!("{:#?}", snap);
    Ok(())
}
