use tracing::info;

use clap::{App, Arg, ArgMatches};
use opentelemetry::global;

mod client;
mod config;
mod controller;
mod identity;
use client::{ApiClientError, CreateVolumeTopology, MayastorApiClient};
use config::CsiControllerConfig;

mod server;

const CSI_SOCKET: &str = "/var/tmp/csi.sock";

/// Initialize all components before starting the CSI controller.
fn initialize_controller(args: &ArgMatches) -> anyhow::Result<()> {
    CsiControllerConfig::initialize(args)?;
    MayastorApiClient::initialize()
        .map_err(|e| anyhow::anyhow!("Failed to initialize API client, error = {}", e))?;
    Ok(())
}

#[tokio::main(worker_threads = 2)]
pub async fn main() -> anyhow::Result<()> {
    let default_io_selector = CsiControllerConfig::default_io_selector();
    let args = App::new(utils::package_description!())
        .author(clap::crate_authors!())
        .version(utils::version_info_str!())
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
        .arg(
            Arg::with_name("io-engine-selector")
                .long("io-engine-selector")
                .multiple(true)
                .number_of_values(1)
                .allow_hyphen_values(true)
                .default_value(&default_io_selector)
                .help(
                    "Adds io-engine selector labels (supports multiple values).\n\
                Example:\n --io-engine-selector key:value --io-engine-selector key2:value2",
                ),
        )
        .get_matches();

    utils::print_package_info!();

    let tags = utils::tracing_telemetry::default_tracing_tags(
        utils::raw_version_str(),
        env!("CARGO_PKG_VERSION"),
    );
    utils::tracing_telemetry::init_tracing(
        "csi-controller",
        tags,
        args.value_of("jaeger").map(|s| s.to_string()),
    );

    initialize_controller(&args)?;

    info!(
        "Starting Mayastor CSI Controller, REST endpoint = {}",
        CsiControllerConfig::get_config().rest_endpoint()
    );

    let result = server::CsiServer::run(
        args.value_of("socket")
            .expect("CSI socket must be specfied")
            .to_string(),
    )
    .await;
    global::shutdown_tracer_provider();
    result.map_err(|e| anyhow::anyhow!("e: {}", e))
}
