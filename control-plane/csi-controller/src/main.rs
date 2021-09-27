use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Registry};

use clap::{App, Arg};

mod client;
mod controller;
mod identity;
use client::{ApiClientError, MayastorApiClient};
mod server;

const CSI_SOCKET: &str = "/var/tmp/csi.sock";

#[tokio::main(worker_threads = 2)]
pub async fn main() -> Result<(), String> {
    let args = App::new("Mayastor k8s pool operator")
        .author(clap::crate_authors!())
        .version(clap::crate_version!())
        .settings(&[
            clap::AppSettings::ColoredHelp,
            clap::AppSettings::ColorAlways,
        ])
        .arg(
            Arg::with_name("endpoint")
                .long("rest-endpoint")
                .short("-r")
                .env("ENDPOINT")
                .default_value("http://ksnode-1:30011")
                .help("an URL endpoint to the mayastor control plane"),
        )
        .arg(
            Arg::with_name("socket")
                .long("csi-socket")
                .short("-c")
                .env("CSI_SOCKET")
                .default_value(CSI_SOCKET)
                .help("CSI socket path"),
        )
        .arg(
            Arg::with_name("jaeger")
                .short("-j")
                .long("jaeger")
                .env("JAEGER_ENDPOINT")
                .help("enable open telemetry and forward to jaeger"),
        )
        .get_matches();

    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .expect("failed to init tracing filter");

    let subscriber = Registry::default()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().pretty());

    if let Some(jaeger) = args.value_of("jaeger") {
        let tracer = opentelemetry_jaeger::new_pipeline()
            .with_agent_endpoint(jaeger)
            .with_service_name("csi-controller")
            .install_batch(opentelemetry::runtime::TokioCurrentThread)
            .expect("Should be able to initialise the exporter");
        let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
        subscriber.with(telemetry).init();
    } else {
        subscriber.init();
    }

    let rest_endpoint = args
        .value_of("endpoint")
        .expect("rest endpoint must be specified");

    info!(?rest_endpoint, "Starting Mayastor CSI Controller");

    MayastorApiClient::initialize(rest_endpoint.into())
        .map_err(|e| format!("Failed to initialize API client, error = {}", e))?;

    server::CsiServer::run(
        args.value_of("socket")
            .expect("CSI socket must be specfied")
            .to_string(),
    )
    .await
}
