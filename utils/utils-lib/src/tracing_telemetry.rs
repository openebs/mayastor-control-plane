/// OpenTelemetry KeyVal for Processor Tags
pub use opentelemetry::{global, trace, Context, KeyValue};

use event_publisher::event_handler::EventHandle;
use opentelemetry::sdk::{propagation::TraceContextPropagator, Resource};
use tracing::Level;
use tracing_subscriber::{
    filter, fmt::writer::MakeWriterExt, layer::SubscriberExt, util::SubscriberInitExt, Layer,
    Registry,
};

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

/// Initialise tracing and optionally opentelemetry and eventing.
/// Tracing will have a stdout subscriber with pretty formatting.
pub fn init_tracing_with_eventing(
    service_name: &str,
    tracing_tags: Vec<KeyValue>,
    jaeger: Option<String>,
    events_url: Option<url::Url>,
) {
    init_tracing_ext(
        service_name,
        tracing_tags,
        jaeger,
        FmtLayer::Stdout,
        events_url,
    );
}

/// Initialise tracing and optionally opentelemetry.
/// Tracing will have a stdout subscriber with pretty formatting.
pub fn init_tracing(service_name: &str, tracing_tags: Vec<KeyValue>, jaeger: Option<String>) {
    init_tracing_ext(service_name, tracing_tags, jaeger, FmtLayer::Stdout, None);
}

/// Fmt Layer for console output.
pub enum FmtLayer {
    /// Output traces to stdout.
    Stdout,
    /// Output traces to stderr.
    Stderr,
    /// Don't output traces to console.
    None,
}

/// Initialise tracing and optionally opentelemetry.
/// Tracing will have a stdout subscriber with pretty formatting.
pub fn init_tracing_ext<T: std::net::ToSocketAddrs>(
    service_name: &str,
    mut tracing_tags: Vec<KeyValue>,
    jaeger: Option<T>,
    fmt_layer: FmtLayer,
    events_url: Option<url::Url>,
) {
    const EVENT_BUS: &str = "mbus-events-target";
    let (stdout, stderr) = match fmt_layer {
        FmtLayer::Stdout => (
            Some(
                tracing_subscriber::fmt::layer()
                    .with_writer(std::io::stdout.with_filter(|meta| meta.target() != EVENT_BUS))
                    .pretty(),
            ),
            None,
        ),
        FmtLayer::Stderr => (
            None,
            Some(
                tracing_subscriber::fmt::layer()
                    .with_writer(std::io::stderr.with_filter(|meta| meta.target() != EVENT_BUS))
                    .pretty(),
            ),
        ),
        FmtLayer::None => (None, None),
    };

    // Get the optional eventing layer.
    let events_layer = match events_url {
        Some(url) => {
            let target = filter::Targets::new().with_target(EVENT_BUS, Level::INFO);
            Some(EventHandle::init(url.to_string(), service_name).with_filter(target))
        }
        None => None,
    };

    let subscriber = Registry::default()
        .with(tracing_filter::rust_log_filter())
        .with(stdout)
        .with(stderr)
        .with(events_layer);

    match jaeger {
        Some(jaeger) => {
            tracing_tags.append(&mut default_tracing_tags(
                super::raw_version_str(),
                env!("CARGO_PKG_VERSION"),
            ));
            let tracing_tags =
                tracing_tags
                    .into_iter()
                    .fold(Vec::<KeyValue>::new(), |mut acc, kv| {
                        if !acc.iter().any(|acc| acc.key == kv.key) {
                            acc.push(kv);
                        }
                        acc
                    });
            set_jaeger_env();

            global::set_text_map_propagator(TraceContextPropagator::new());
            let tracer = opentelemetry_jaeger::new_agent_pipeline()
                .with_endpoint(jaeger)
                .with_service_name(service_name)
                .with_trace_config(
                    opentelemetry::sdk::trace::Config::default()
                        .with_resource(Resource::new(tracing_tags)),
                )
                .install_batch(opentelemetry::runtime::TokioCurrentThread)
                .expect("Should be able to initialise the exporter");
            let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
            subscriber.with(telemetry).init();
        }
        None => subscriber.init(),
    };
}

/// Tracing telemetry style.
pub enum FmtStyle {
    /// Compact style.
    Compact,
    /// Pretty Style
    Pretty,
}

/// Tracing telemetry builder.
pub struct TracingTelemetry {
    writer: FmtLayer,
    style: FmtStyle,
    colours: bool,
}

impl TracingTelemetry {
    /// Tracing telemetry default builder.
    pub fn builder() -> Self {
        Self {
            writer: FmtLayer::Stdout,
            style: FmtStyle::Pretty,
            colours: true,
        }
    }
    /// Specify writer stream.
    pub fn with_writer(self, writer: FmtLayer) -> TracingTelemetry {
        TracingTelemetry { writer, ..self }
    }
    /// Specify style.
    pub fn with_style(self, style: FmtStyle) -> TracingTelemetry {
        TracingTelemetry { style, ..self }
    }
    /// Specify whether colour is needed or not.
    pub fn with_colours(self, colours: bool) -> TracingTelemetry {
        TracingTelemetry { colours, ..self }
    }
    /// Initialize the telemetry instance.
    pub fn init(self) {
        match (self.writer, self.style) {
            (FmtLayer::Stderr, FmtStyle::Compact) => {
                let stderr = tracing_subscriber::fmt::layer()
                    .with_writer(std::io::stderr)
                    .compact()
                    .with_ansi(self.colours);
                let subscriber = Registry::default()
                    .with(tracing_filter::rust_log_filter())
                    .with(stderr);
                subscriber.init();
            }
            (FmtLayer::Stdout, FmtStyle::Compact) => {
                let stdout = tracing_subscriber::fmt::layer()
                    .with_writer(std::io::stdout)
                    .compact()
                    .with_ansi(self.colours);
                let subscriber = Registry::default()
                    .with(tracing_filter::rust_log_filter())
                    .with(stdout);
                subscriber.init();
            }
            (FmtLayer::Stderr, FmtStyle::Pretty) => {
                let stderr = tracing_subscriber::fmt::layer()
                    .with_writer(std::io::stderr)
                    .pretty()
                    .with_ansi(self.colours);
                let subscriber = Registry::default()
                    .with(tracing_filter::rust_log_filter())
                    .with(stderr);
                subscriber.init();
            }
            (FmtLayer::Stdout, FmtStyle::Pretty) => {
                let stdout = tracing_subscriber::fmt::layer()
                    .with_writer(std::io::stdout)
                    .pretty()
                    .with_ansi(self.colours);
                let subscriber = Registry::default()
                    .with(tracing_filter::rust_log_filter())
                    .with(stdout);
                subscriber.init();
            }
            (FmtLayer::None, _) => {
                let subscriber = Registry::default().with(tracing_filter::rust_log_filter());
                subscriber.init()
            }
        };
    }
}

/// todo: force flush the traces.
pub fn flush_traces() {
    opentelemetry::global::shutdown_tracer_provider();
}
