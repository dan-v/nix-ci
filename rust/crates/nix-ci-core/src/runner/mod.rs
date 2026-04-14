//! `nix-ci run`: streaming submitter + self-regulating worker + event
//! printer. Falls back to local `nix-eval-jobs | nix-store --realise`
//! if the coordinator is unreachable at startup.

pub mod drv_parser;
pub mod drv_walk;
pub mod eval_jobs;
pub mod fallback;
pub mod orchestrator;
pub mod sse;
pub mod submitter;
pub mod worker;

pub use orchestrator::{run, RunArgs, RunOutcome};
