//! Tracing + metrics wiring. Small enough to live in one module.

pub mod metrics;

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Install a global tracing subscriber. Honors `RUST_LOG`; defaults to
/// `info` when unset. Uses JSON output if `NIX_CI_LOG_JSON=1` is set.
pub fn init_tracing() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let json = std::env::var("NIX_CI_LOG_JSON")
        .map(|v| v == "1" || v == "true")
        .unwrap_or(false);

    let registry = tracing_subscriber::registry().with(env_filter);
    if json {
        registry
            .with(tracing_subscriber::fmt::layer().json())
            .init();
    } else {
        registry
            .with(tracing_subscriber::fmt::layer().compact())
            .init();
    }
}
