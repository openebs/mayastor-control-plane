use tracing::info;

use clap::{Arg, ArgMatches};
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
    let args = clap::Command::new(utils::package_description!())
        .version(utils::version_info_str!())
        .arg(
            Arg::new("endpoint")
                .long("rest-endpoint")
                .short('r')
                .env("ENDPOINT")
                .default_value("http://ksnode-1:30011")
                .help("a URL endpoint to the control plane's rest endpoint"),
        )
        .arg(
            Arg::new("socket")
                .long("csi-socket")
                .short('c')
                .env("CSI_SOCKET")
                .default_value(CSI_SOCKET)
                .help("CSI socket path"),
        )
        .arg(
            Arg::new("jaeger")
                .short('j')
                .long("jaeger")
                .env("JAEGER_ENDPOINT")
                .help("enable open telemetry and forward to jaeger"),
        )
        .arg(
            Arg::new("timeout")
                .short('t')
                .long("rest-timeout")
                .env("REST_TIMEOUT")
                .default_value("30s"),
        )
        .arg(
            Arg::new("node-selector")
                .long("node-selector")
                .action(clap::ArgAction::Append)
                .num_args(1)
                .allow_hyphen_values(true)
                .default_value(csi_driver::csi_node_selector())
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
        args.get_one::<String>("jaeger").cloned(),
    );

    initialize_controller(&args)?;

    info!(
        "Starting IoEngine CSI Controller, REST endpoint = {}",
        CsiControllerConfig::get_config().rest_endpoint()
    );

    // Starts PV Garbage Collector if platform type is k8s
    if stor_port::platform::current_plaform_type() == stor_port::platform::PlatformType::K8s {
        let gc_instance = pvwatcher::PvGarbageCollector::new().await?;
        tokio::spawn(async move { gc_instance.run_watcher().await });
    }

    let result = server::CsiServer::run(
        args.get_one::<String>("socket")
            .expect("CSI socket must be specified")
            .clone(),
    )
    .await;
    utils::tracing_telemetry::flush_traces();
    result.map_err(|error| anyhow::anyhow!("error: {}", error))
}
