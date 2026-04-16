//! `nix-ci run`: streaming submitter + self-regulating worker + event
//! printer. Drives `nix-eval-jobs` → batch-ingest → coordinator; a
//! coordinator outage fails the run loudly rather than silently
//! degrading to local-only builds (which would hide outages from ops).

pub mod artifacts;
pub mod drv_parser;
pub mod drv_walk;
pub mod eval_jobs;
pub mod orchestrator;
pub mod output;
pub mod sse;
pub mod submitter;
pub mod worker;

pub use orchestrator::{run, RunArgs, RunOutcome};
