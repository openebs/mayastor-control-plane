use event_publisher::event_handler::EventHandle;
use opentelemetry::trace::TracerProvider;
pub use opentelemetry::{global, trace};
/// OpenTelemetry KeyVal for Processor Tags
pub use opentelemetry::{Context, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{propagation::TraceContextPropagator, trace::SdkTracerProvider, Resource};
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
    events_replicas: Option<usize>,
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
            events_replicas: None,
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

    /// Specify the events url and replicas, If any.
    pub fn with_events(
        self,
        events_url: Option<url::Url>,
        events_replicas: Option<usize>,
    ) -> TracingTelemetry {
        TracingTelemetry {
            events_url,
            events_replicas,
            ..self
        }
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
        let tracer = self.jaeger.map(|mut jaeger| {
            let svc_name = vec![KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_NAME,
                service_name.to_owned(),
            )];
            let tracing_tags = self.tracing_tags.into_iter().fold(svc_name, |mut acc, kv| {
                if !acc.iter().any(|acc| acc.key == kv.key) {
                    acc.push(kv);
                }
                acc
            });

            if !jaeger.starts_with("http") {
                jaeger = format!("http://{jaeger}");
            }
            // todo: init should return an error
            let jaeger = match url::Url::parse(&jaeger).ok() {
                Some(mut url) => {
                    if url.port().is_none() {
                        url.set_port(Some(4317)).ok();
                    }
                    url.to_string()
                }
                None => jaeger,
            };

            set_jaeger_env();
            global::set_text_map_propagator(TraceContextPropagator::new());
            let exporter = opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .with_endpoint(jaeger)
                .build()
                .expect("Should be able to initialise the exporter");
            SdkTracerProvider::builder()
                .with_batch_exporter(exporter)
                .with_resource(
                    Resource::builder_empty()
                        .with_attributes(tracing_tags)
                        .build(),
                )
                .build()
        });
        let tracer = tracer.map(|tracer_provider| {
            global::set_tracer_provider(tracer_provider.clone());
            TRACER_PROVIDER.get_or_init(|| tracer_provider.clone());
            tracer_provider.tracer("tracing-otel-subscriber")
        });

        // Get the optional eventing layer.
        let events_layer = self.events_url.map(|url| {
            let target = filter::Targets::new().with_target(EVENT_BUS, Level::INFO);
            EventHandle::init(url.to_string(), service_name, self.events_replicas)
                .with_filter(target)
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

/// We have to force flush the tracer provider as it lives in a global context.
/// todo: return provider on [`TracingTelemetry::init`] and let the caller manage this.
static TRACER_PROVIDER: std::sync::OnceLock<SdkTracerProvider> = std::sync::OnceLock::new();

/// Flush the traces from the provider.
pub fn flush_traces() {
    if let Some(trace_provider) = TRACER_PROVIDER.get() {
        trace_provider.shutdown().ok();
    }
}
