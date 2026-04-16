//! Tracing + metrics wiring. Small enough to live in one module.

pub mod metrics;

use opentelemetry::global;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::TracerProvider as SdkTracerProvider;
use opentelemetry_sdk::Resource;
use opentelemetry_semantic_conventions::resource::SERVICE_NAME;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

/// Service name used for OTel resource attributes when nothing more
/// specific is supplied. Override with `OTEL_SERVICE_NAME` env var.
pub const DEFAULT_SERVICE_NAME: &str = "nix-ci";

/// Install a global tracing subscriber. Honors `RUST_LOG`; defaults to
/// `info` when unset. Uses JSON output if `NIX_CI_LOG_JSON=1` is set.
///
/// If `OTEL_EXPORTER_OTLP_ENDPOINT` is set in the environment, also
/// installs an OTLP layer that exports spans via HTTP/protobuf to the
/// configured collector. Other standard OTel env vars (`OTEL_SERVICE_NAME`,
/// `OTEL_RESOURCE_ATTRIBUTES`, `OTEL_TRACES_SAMPLER`, etc.) are honored
/// by the SDK. When the env var is absent, behavior is identical to
/// pre-OTLP — no exporter is created, no traces are emitted, no
/// runtime overhead beyond what `tracing` already costs.
///
/// `service_name_default` is the per-binary default (e.g.
/// `"nix-ci-coordinator"` for the server) used when the operator
/// hasn't set `OTEL_SERVICE_NAME` themselves.
pub fn init_tracing(service_name_default: &str) {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let json = std::env::var("NIX_CI_LOG_JSON")
        .map(|v| v == "1" || v == "true")
        .unwrap_or(false);

    let otel_layer = build_otel_layer(service_name_default);

    let registry = tracing_subscriber::registry().with(env_filter);
    // Boxing each layer keeps the type signatures of the conditional
    // chains tractable — without it the registry's type explodes
    // combinatorially across (json/compact) × (otel/no-otel).
    if json {
        let r = registry.with(tracing_subscriber::fmt::layer().json().boxed());
        if let Some(layer) = otel_layer {
            r.with(layer).init();
        } else {
            r.init();
        }
    } else {
        let r = registry.with(tracing_subscriber::fmt::layer().compact().boxed());
        if let Some(layer) = otel_layer {
            r.with(layer).init();
        } else {
            r.init();
        }
    }
}

/// Build the `tracing-opentelemetry` layer if OTLP export is configured
/// in the environment. Returns `None` when the operator hasn't set up
/// a collector — in that case the layer is omitted entirely so there's
/// zero runtime overhead.
///
/// Uses HTTP/protobuf transport (not gRPC) so we can reuse the
/// `reqwest` dependency already in the tree instead of pulling in
/// the tonic stack.
fn build_otel_layer<S>(
    service_name_default: &str,
) -> Option<tracing_opentelemetry::OpenTelemetryLayer<S, opentelemetry_sdk::trace::Tracer>>
where
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
{
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok()?;
    if endpoint.trim().is_empty() {
        return None;
    }
    let service_name = std::env::var("OTEL_SERVICE_NAME")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| service_name_default.to_string());

    let exporter = match opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(endpoint.clone())
        .build()
    {
        Ok(e) => e,
        Err(e) => {
            // OTel build failures shouldn't take down the binary.
            eprintln!("nix-ci: OTLP exporter init failed ({e}); tracing disabled");
            return None;
        }
    };

    let resource = Resource::new(vec![opentelemetry::KeyValue::new(
        SERVICE_NAME,
        service_name,
    )]);

    let provider = SdkTracerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
        .build();

    let tracer = provider.tracer(DEFAULT_SERVICE_NAME);

    // Install as the global provider so anything that uses the
    // bare `opentelemetry::global::tracer(...)` API also works
    // (workers may add such tracers later for non-tracing-bridged
    // spans).
    global::set_tracer_provider(provider);
    // Make W3C `traceparent` the default propagator so cross-process
    // links work without extra wiring.
    global::set_text_map_propagator(opentelemetry_sdk::propagation::TraceContextPropagator::new());

    Some(tracing_opentelemetry::layer().with_tracer(tracer))
}

/// Service-name conventions for the in-tree binaries. Operators can
/// override per-process via `OTEL_SERVICE_NAME`.
pub mod service {
    pub const COORDINATOR: &str = "nix-ci-coordinator";
    pub const RUNNER: &str = "nix-ci-runner";
    pub const WORKER: &str = "nix-ci-worker";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_otel_layer_returns_none_when_endpoint_unset() {
        // SAFETY: env-var reads are process-global. We only assert the
        // negative case (no endpoint = no layer), which is robust to
        // concurrent test interference: even if another test sets the
        // env var briefly, the worst case is a spurious Some which
        // we'd notice via test flakiness.
        // SAFETY: setting env vars in tests is unsafe in 2024 edition;
        // using set_var here in a single-threaded test path.
        unsafe {
            std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
        }
        let layer = build_otel_layer::<tracing_subscriber::Registry>("test");
        assert!(layer.is_none(), "no endpoint env var → no layer");
    }

    #[test]
    fn build_otel_layer_returns_none_when_endpoint_empty_or_whitespace() {
        // SAFETY: see above.
        unsafe {
            std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", "   ");
        }
        let layer = build_otel_layer::<tracing_subscriber::Registry>("test");
        assert!(layer.is_none(), "whitespace-only endpoint must not init");
        unsafe {
            std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
        }
    }
}
