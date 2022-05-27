/// OpenTelemetry KeyVal for Processor Tags
pub use opentelemetry::KeyValue;
use opentelemetry::{global, sdk::propagation::TraceContextPropagator};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Registry};

/// Parse KeyValues from structopt's cmdline arguments
pub fn parse_key_value(source: &str) -> Result<KeyValue, String> {
    match source.split_once('=') {
        None => Err("Each element must be in the format: 'Key=Value'".to_string()),
        Some((key, value)) => Ok(KeyValue::new(key.to_string(), value.to_string())),
    }
}

/// Get Default Processor Tags
/// ## Example:
/// let _ = default_tracing_tags(git_version!(args = ["--abbrev=12", "--always"]),
/// env!("CARGO_PKG_VERSION"));
pub fn default_tracing_tags(git_commit: &str, cargo_version: &str) -> Vec<KeyValue> {
    vec![
        KeyValue::new("git.commit", git_commit.to_string()),
        KeyValue::new("crate.version", cargo_version.to_string()),
    ]
}

/// Name of the OTEL_BSP_MAX_EXPORT_BATCH_SIZE variable
pub const OTEL_BSP_MAX_EXPORT_BATCH_SIZE_NAME: &str = "OTEL_BSP_MAX_EXPORT_BATCH_SIZE";
/// The value of OTEL_BSP_MAX_EXPORT_BATCH_SIZE to be used with JAEGER
pub const OTEL_BSP_MAX_EXPORT_BATCH_SIZE_JAEGER: &str = "64";
/// Set the OTEL variables for a jaeger configuration
pub fn set_jaeger_env() {
    // if not set, default it to our jaeger value
    if std::env::var(OTEL_BSP_MAX_EXPORT_BATCH_SIZE_NAME).is_err() {
        std::env::set_var(
            OTEL_BSP_MAX_EXPORT_BATCH_SIZE_NAME,
            OTEL_BSP_MAX_EXPORT_BATCH_SIZE_JAEGER,
        );
    }
}

fn rust_log_add_quiet_defaults(
    current: tracing_subscriber::EnvFilter,
) -> tracing_subscriber::EnvFilter {
    let main = match current.to_string().as_str() {
        "debug" => "debug",
        "trace" => "trace",
        _ => return current,
    };
    let logs = format!("{},{}", main, super::constants::RUST_LOG_QUIET_DEFAULTS);
    tracing_subscriber::EnvFilter::try_new(logs).unwrap()
}

/// Initialise tracing and optionally opentelemetry.
/// Tracing will have a stdout subscriber with pretty formatting.
pub fn init_tracing(service_name: &str, tracing_tags: Vec<KeyValue>, jaeger: Option<String>) {
    init_tracing_level(service_name, tracing_tags, jaeger, None);
}

/// Initialise tracing and optionally opentelemetry.
/// Tracing will have a stdout subscriber with pretty formatting.
pub fn init_tracing_level(
    service_name: &str,
    mut tracing_tags: Vec<KeyValue>,
    jaeger: Option<String>,
    level: Option<&str>,
) {
    let level = level.unwrap_or("info");
    let filter = rust_log_add_quiet_defaults(
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(level)),
    );

    let subscriber = Registry::default()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().pretty());

    match jaeger {
        Some(jaeger) => {
            tracing_tags.append(&mut default_tracing_tags(
                super::raw_version_str(),
                env!("CARGO_PKG_VERSION"),
            ));
            tracing_tags.dedup();
            println!("Using the following tracing tags: {:?}", tracing_tags);

            set_jaeger_env();

            global::set_text_map_propagator(TraceContextPropagator::new());
            let tracer = opentelemetry_jaeger::new_pipeline()
                .with_agent_endpoint(jaeger)
                .with_service_name(service_name)
                .with_tags(tracing_tags)
                .install_batch(opentelemetry::runtime::TokioCurrentThread)
                .expect("Should be able to initialise the exporter");
            let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
            subscriber.with(telemetry).init();
        }
        None => subscriber.init(),
    };
}
