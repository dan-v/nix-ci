//! nix-ci core: dispatcher, server, client, runner, durability.
//!
//! One crate so the binary can link any combination. The module tree
//! mirrors v2 DESIGN section 12.

pub mod client;
pub mod config;
pub mod dispatch;
pub mod durable;
pub mod error;
pub mod observability;
pub mod runner;
pub mod server;
pub mod types;

pub use error::{Error, Result};
pub use types::*;
