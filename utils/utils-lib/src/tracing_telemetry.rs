/// OpenTelemetry KeyVal for Processor Tags
pub use opentelemetry::{global, trace, Context, KeyValue};

use event_publisher::event_handler::EventHandle;
use opentelemetry::sdk::{propagation::TraceContextPropagator, trace::Tracer, Resource};
use tracing::Level;
use tracing_subscriber::{filter, layer::SubscriberExt, util::SubscriberInitExt, Layer, Registry};

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

/// Fmt Layer for console output.
pub enum FmtLayer {
    /// Output traces to stdout.
    Stdout,
    /// Output traces to stderr.
    Stderr,
    /// Don't output traces to console.
    None,
}

/// Tracing telemetry style.
#[derive(Debug, Clone, Copy, strum_macros::EnumString, strum_macros::AsRefStr)]
#[strum(serialize_all = "lowercase")]
pub enum FmtStyle {
    /// Compact style.
    Compact,
    /// Pretty Style.
    Pretty,
    /// JSON Style.
    Json,
}

const EVENT_BUS: &str = "mbus-events-target";

/// Tracing telemetry builder.
pub struct TracingTelemetry {
    writer: FmtLayer,
    style: FmtStyle,
    colours: bool,
    jaeger: Option<String>,
    events_url: Option<url::Url>,
    tracing_tags: Vec<KeyValue>,
}

impl TracingTelemetry {
    /// Tracing telemetry default builder.
    pub fn builder() -> Self {
        Self {
            writer: FmtLayer::Stdout,
            style: FmtStyle::Pretty,
            colours: true,
            jaeger: None,
            events_url: None,
            tracing_tags: Vec::new(),
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

    /// Specify the jaeger endpoint, If any.
    pub fn with_jaeger(self, jaeger: Option<String>) -> TracingTelemetry {
        TracingTelemetry { jaeger, ..self }
    }

    /// Specify the events url, If any.
    pub fn with_events_url(self, events_url: Option<url::Url>) -> TracingTelemetry {
        TracingTelemetry { events_url, ..self }
    }

    /// Specify the tracing tags, If any.
    pub fn with_tracing_tags(self, tracing_tags: Vec<KeyValue>) -> TracingTelemetry {
        TracingTelemetry {
            tracing_tags,
            ..self
        }
    }

    /// Initialize the telemetry instance.
    pub fn init(self, service_name: &str) {
        let stdout = tracing_subscriber::fmt::layer()
            .with_writer(std::io::stdout)
            .with_ansi(self.colours);
        let stderr = tracing_subscriber::fmt::layer()
            .with_writer(std::io::stderr)
            .with_ansi(self.colours);
        let tracer: Option<Tracer> = self.jaeger.map(|jaeger| {
            let tracing_tags =
                self.tracing_tags
                    .into_iter()
                    .fold(Vec::<KeyValue>::new(), |mut acc, kv| {
                        if !acc.iter().any(|acc| acc.key == kv.key) {
                            acc.push(kv);
                        }
                        acc
                    });
            set_jaeger_env();
            global::set_text_map_propagator(TraceContextPropagator::new());
            opentelemetry_jaeger::new_agent_pipeline()
                .with_endpoint(jaeger)
                .with_service_name(service_name)
                .with_trace_config(
                    opentelemetry::sdk::trace::Config::default()
                        .with_resource(Resource::new(tracing_tags)),
                )
                .install_batch(opentelemetry::runtime::TokioCurrentThread)
                .expect("Should be able to initialise the exporter")
        });

        // Get the optional eventing layer.
        let events_layer = self.events_url.map(|url| {
            let target = filter::Targets::new().with_target(EVENT_BUS, Level::INFO);
            EventHandle::init(url.to_string(), service_name).with_filter(target)
        });

        let subscriber = Registry::default()
            .with(tracing_filter::rust_log_filter())
            .with(events_layer);

        match (self.writer, self.style) {
            (FmtLayer::Stderr, FmtStyle::Compact) => {
                if let Some(tracer) = tracer {
                    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
                    subscriber.with(stderr.compact()).with(telemetry).init();
                } else {
                    subscriber.with(stderr.compact()).init();
                }
            }
            (FmtLayer::Stdout, FmtStyle::Compact) => {
                if let Some(tracer) = tracer {
                    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
                    subscriber.with(stdout.compact()).with(telemetry).init();
                } else {
                    subscriber.with(stdout.compact()).init();
                }
            }
            (FmtLayer::Stderr, FmtStyle::Pretty) => {
                if let Some(tracer) = tracer {
                    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
                    subscriber.with(stderr.pretty()).with(telemetry).init();
                } else {
                    subscriber.with(stderr.pretty()).init();
                }
            }
            (FmtLayer::Stdout, FmtStyle::Pretty) => {
                if let Some(tracer) = tracer {
                    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
                    subscriber.with(stdout.pretty()).with(telemetry).init();
                } else {
                    subscriber.with(stdout.pretty()).init();
                }
            }
            (FmtLayer::Stdout, FmtStyle::Json) => {
                if let Some(tracer) = tracer {
                    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
                    subscriber.with(stdout.json()).with(telemetry).init();
                } else {
                    subscriber.with(stdout.json()).init();
                }
            }
            (FmtLayer::Stderr, FmtStyle::Json) => {
                if let Some(tracer) = tracer {
                    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
                    subscriber.with(stderr.json()).with(telemetry).init();
                } else {
                    subscriber.with(stderr.json()).init();
                }
            }
            (FmtLayer::None, _) => {
                let subscriber = Registry::default().with(tracing_filter::rust_log_filter());
                if let Some(tracer) = tracer {
                    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
                    subscriber.with(telemetry).init();
                } else {
                    subscriber.init()
                }
            }
        };
    }
}

/// todo: force flush the traces.
pub fn flush_traces() {
    opentelemetry::global::shutdown_tracer_provider();
}
