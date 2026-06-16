use openapi::clients::tower::configuration::Configuration;
use opentelemetry::{global, trace::TracerProvider, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{propagation::TraceContextPropagator, Resource};
use std::time::Duration;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Registry};

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    let subscriber = Registry::default()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().pretty());

    let svc_name = Resource::new(vec![KeyValue::new("service.name", "example".to_owned())]);

    global::set_text_map_propagator(TraceContextPropagator::new());
    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint("http://localhost:4317"),
        )
        .with_trace_config(opentelemetry_sdk::trace::Config::default().with_resource(svc_name))
        .install_simple()
        .expect("Should be able to initialise the exporter");
    let tracer = tracer.tracer("example");
    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    subscriber.with(telemetry).init();
}

#[tokio::main]
async fn main() {
    init_tracing();
    let config = Configuration::new(
        "http://localhost:8081/".parse().unwrap(),
        Duration::from_secs(5),
        None,
        true,
        Some(1),
    )
    .unwrap();
    let client = openapi::clients::tower::ApiClient::new(config);

    {
        let span = tracing::info_span!("span example");
        let _enter = span.enter();

        match client.nodes_api().get_nodes(None).await {
            Ok(resp) => {
                println!("resp: {resp:#?}");
            }
            Err(resp) => {
                println!("resp: {resp:#?}");
            }
        }
    }

    global::shutdown_tracer_provider();
}
