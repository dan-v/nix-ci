//! HTTP client for talking to a nix-ci coordinator.

pub mod http;

pub use http::{BuildLogUploadMeta, CoordinatorClient};
