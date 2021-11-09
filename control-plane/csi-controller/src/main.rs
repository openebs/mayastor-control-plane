use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Registry};

use clap::{App, Arg, ArgMatches};
use opentelemetry::{global, sdk::propagation::TraceContextPropagator};

mod client;
mod config;
mod controller;
mod identity;
use client::{ApiClientError, CreateVolumeTopology, MayastorApiClient};
use config::CsiControllerConfig;

mod server;

const CSI_SOCKET: &str = "/var/tmp/csi.sock";

/// Initialize all components before starting the CSI controller.
fn initialize_controller(args: &ArgMatches) -> Result<(), String> {
    CsiControllerConfig::initialize(args);
    MayastorApiClient::initialize()
        .map_err(|e| format!("Failed to initialize API client, error = {}", e))?;
    Ok(())
}

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
        .arg(
            Arg::with_name("timeout")
                .short("-t")
                .long("rest-timeout")
                .env("REST_TIMEOUT")
                .default_value("5s"),
        )
        .get_matches();

    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .expect("failed to init tracing filter");

    let subscriber = Registry::default()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().pretty());

    if let Some(jaeger) = args.value_of("jaeger") {
        global::set_text_map_propagator(TraceContextPropagator::new());
        let tags = common_lib::opentelemetry::default_tracing_tags(
            git_version::git_version!(args = ["--abbrev=12", "--always"]),
            env!("CARGO_PKG_VERSION"),
        );
        let tracer = opentelemetry_jaeger::new_pipeline()
            .with_agent_endpoint(jaeger)
            .with_service_name("csi-controller")
            .with_tags(tags)
            .install_batch(opentelemetry::runtime::TokioCurrentThread)
            .expect("Should be able to initialise the exporter");
        let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
        subscriber.with(telemetry).init();
    } else {
        subscriber.init();
    }

    initialize_controller(&args)?;

    info!(
        "Starting Mayastor CSI Controller, REST endpoint = {}",
        CsiControllerConfig::get_config().rest_endpoint()
    );

    let mut signal_term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("Failed to register handler for SIGTERM");
    let mut signal_int = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .expect("Failed to register handler for SIGINT");

    tokio::select! {
        srv =  server::CsiServer::run(args.value_of("socket")
            .expect("CSI socket must be specified")
            .to_string()) => {
            srv?
        }
        _evt = signal_term.recv() => {
            }
        _evt = signal_int.recv() => {
            }
    }
    Ok(())
}
