use tracing::info;

use clap::{App, Arg, ArgMatches};
use opentelemetry::global;
mod client;
mod config;
mod controller;
mod identity;
mod pvwatcher;
mod server;

use client::{ApiClientError, CreateVolumeTopology, IoEngineApiClient};
use config::CsiControllerConfig;

const CSI_SOCKET: &str = "/var/tmp/csi.sock";

/// Initialize all components before starting the CSI controller.
fn initialize_controller(args: &ArgMatches) -> anyhow::Result<()> {
    CsiControllerConfig::initialize(args)?;
    IoEngineApiClient::initialize()
        .map_err(|e| anyhow::anyhow!("Failed to initialize API client, error = {}", e))?;
    Ok(())
}

#[tokio::main(worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
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
                .help("a URL endpoint to the control plane's rest endpoint"),
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
            Arg::with_name("node-selector")
                .long("node-selector")
                .multiple(true)
                .number_of_values(1)
                .allow_hyphen_values(true)
                .default_value(Box::leak(csi_driver::csi_node_selector().into_boxed_str()))
                .help(
                    "The node selector label which this plugin will report as part of its topology.\n\
                    Example:\n --node-selector key=value --node-selector key2=value2",
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
        "Starting IoEngine CSI Controller, REST endpoint = {}",
        CsiControllerConfig::get_config().rest_endpoint()
    );

    // Starts PV Garbage Collector if platform type is k8s
    if common_lib::platform::current_plaform_type() == common_lib::platform::PlatformType::K8s {
        let gc_instance = pvwatcher::PvGarbageCollector::new().await?;
        tokio::spawn(async move { gc_instance.run_watcher().await });
    }

    let result = server::CsiServer::run(
        args.value_of("socket")
            .expect("CSI socket must be specified")
            .to_string(),
    )
    .await;
    global::shutdown_tracer_provider();
    result.map_err(|e| anyhow::anyhow!("e: {}", e))
}
