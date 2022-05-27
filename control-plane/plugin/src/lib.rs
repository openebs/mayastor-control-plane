#[macro_use]
extern crate prettytable;
#[macro_use]
extern crate lazy_static;

use opentelemetry::{global, sdk::propagation::TraceContextPropagator};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Registry};

pub mod operations;
pub mod resources;
pub mod rest_wrapper;

fn default_log_filter(current: tracing_subscriber::EnvFilter) -> tracing_subscriber::EnvFilter {
    let log_level = match current.to_string().as_str() {
        "debug" => "debug",
        "trace" => "trace",
        _ => return current,
    };
    let logs = format!(
        "plugin={},{}={},error",
        log_level,
        std::env!("CARGO_PKG_NAME").replace('-', "_"),
        log_level
    );
    tracing_subscriber::EnvFilter::try_new(logs).unwrap()
}

/// Initialize tracing (including opentelemetry).
pub fn init_tracing(jaeger: &Option<String>) {
    let filter = default_log_filter(
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("off")),
    );

    let subscriber = Registry::default().with(filter).with(
        tracing_subscriber::fmt::layer()
            .with_writer(std::io::stderr)
            .pretty(),
    );

    match jaeger {
        Some(jaeger) => {
            global::set_text_map_propagator(TraceContextPropagator::new());
            let git_version = option_env!("GIT_VERSION").unwrap_or_else(utils::raw_version_str);
            let tags = utils::tracing_telemetry::default_tracing_tags(
                git_version,
                env!("CARGO_PKG_VERSION"),
            );
            let tracer = opentelemetry_jaeger::new_pipeline()
                .with_agent_endpoint(jaeger)
                .with_service_name(std::env!("CARGO_PKG_NAME"))
                .with_tags(tags)
                .install_batch(opentelemetry::runtime::TokioCurrentThread)
                .expect("Should be able to initialise the exporter");
            let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
            subscriber.with(telemetry).init();
        }
        None => subscriber.init(),
    };
}
