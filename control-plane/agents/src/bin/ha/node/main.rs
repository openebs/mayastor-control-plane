use clap::Parser;
use grpc::{
    csi_node_nvme::nvme_operations_client::NvmeOperationsClient,
    operations::ha_node::{client::ClusterAgentClient, traits::ClusterAgentOperations},
};
use http::Uri;
use once_cell::sync::OnceCell;
use std::{
    net::{IpAddr, SocketAddr},
    time::Duration,
};
use stor_port::{
    transport_api::TimeoutOptions, types::v0::transport::cluster_agent::NodeAgentInfo,
};
use tokio::net::UnixStream;
use tonic::transport::{Channel, Endpoint};
use tower::service_fn;
use utils::{
    package_description, tracing_telemetry::KeyValue, version_info_str,
    DEFAULT_CLUSTER_AGENT_CLIENT_ADDR, DEFAULT_NODE_AGENT_SERVER_IP,
    DEFAULT_NODE_AGENT_SERVER_PORT, NVME_PATH_AGGREGATION_PERIOD, NVME_PATH_CHECK_PERIOD,
    NVME_PATH_CONNECTION_PERIOD, NVME_PATH_RETRANSMISSION_PERIOD, NVME_SUBSYS_REFRESH_PERIOD,
};
mod detector;
mod path_provider;
mod reporter;
mod server;

use detector::PathFailureDetector;
use server::NodeAgentApiServer;
use utils::tracing_telemetry::{FmtLayer, FmtStyle};

/// TODO
#[derive(Debug, Parser)]
#[structopt(name = package_description!(), version = version_info_str!())]
struct Cli {
    /// HA Cluster Agent URL or address to connect to the services.
    #[clap(long, short, default_value = DEFAULT_CLUSTER_AGENT_CLIENT_ADDR)]
    cluster_agent: Uri,

    /// Node name(spec.nodeName). This must be the same as provided in csi-node.
    #[clap(short, long)]
    node_name: String,

    /// IP address and port for the ha node-agent to listen on.
    #[clap(short, long = "grpc-endpoint")]
    deprecated_grpc_endpoint: Option<SocketAddr>,

    #[clap(long, default_value_t = DEFAULT_NODE_AGENT_SERVER_IP)]
    grpc_ip: IpAddr,

    #[clap(long, default_value_t = DEFAULT_NODE_AGENT_SERVER_PORT)]
    grpc_port: u16,

    /// Add process service tags to the traces.
    #[clap(short, long, env = "TRACING_TAGS", value_delimiter=',', value_parser = utils::tracing_telemetry::parse_key_value)]
    tracing_tags: Vec<KeyValue>,

    /// Path failure detection period.
    #[clap(short, long, env = "DETECTION_PERIOD", default_value = NVME_PATH_CHECK_PERIOD)]
    detection_period: humantime::Duration,

    /// Retransmission period for reporting failed paths in case of network issues.
    #[clap(short, long, env = "RETRANSMISSION_PERIOD", default_value = NVME_PATH_RETRANSMISSION_PERIOD)]
    retransmission_period: humantime::Duration,

    /// Period for aggregating multiple failed paths before reporting them.
    #[clap(short, long, env = "AGGREGATION_PERIOD", default_value = NVME_PATH_AGGREGATION_PERIOD)]
    aggregation_period: humantime::Duration,

    /// Connection timeout for path replacement operation.
    #[clap(short, long, env = "PATH_CONNECTION_TIMEOUT", default_value = NVME_PATH_CONNECTION_PERIOD)]
    path_connection_timeout: humantime::Duration,

    /// NVMe subsystem refresh period when monitoring its state.
    #[clap(short, long, env = "SUBSYS_REFRESH_PERIOD", default_value = NVME_SUBSYS_REFRESH_PERIOD)]
    subsys_refresh_period: humantime::Duration,

    /// Sends opentelemetry spans to the Jaeger endpoint agent.
    #[clap(long, short)]
    jaeger: Option<String>,

    /// The csi-node socket file for grpc over uds.
    #[clap(long)]
    csi_socket: std::path::PathBuf,

    /// Events message-bus endpoint url.
    #[clap(long, short)]
    events_url: Option<url::Url>,

    /// Formatting style to be used while logging.
    #[clap(default_value = FmtStyle::Pretty.as_ref(), short, long)]
    fmt_style: FmtStyle,

    /// Enable ansi colors for logs.
    #[clap(long, default_value_t = true, action = clap::ArgAction::Set)]
    ansi_colors: bool,
}

static CLUSTER_AGENT_CLIENT: OnceCell<ClusterAgentClient> = OnceCell::new();

static CSI_NODE_NVME_CLIENT: OnceCell<NvmeOperationsClient<Channel>> = OnceCell::new();

pub fn cluster_agent_client() -> &'static ClusterAgentClient {
    CLUSTER_AGENT_CLIENT
        .get()
        .expect("HA Cluster-Agent client should have been initialized")
}

pub fn csi_node_nvme_client() -> &'static NvmeOperationsClient<Channel> {
    CSI_NODE_NVME_CLIENT
        .get()
        .expect("Csi node nvme client should have been initialized")
}

impl Cli {
    fn args() -> Self {
        Cli::parse()
    }

    fn grpc_endpoint(&self) -> SocketAddr {
        #[allow(deprecated)]
        if let Some(deprecated_endpoint) = &self.deprecated_grpc_endpoint {
            *deprecated_endpoint
        } else {
            std::net::SocketAddr::new(self.grpc_ip, self.grpc_port)
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli_args = Cli::args();

    utils::print_package_info!();

    utils::tracing_telemetry::TracingTelemetry::builder()
        .with_writer(FmtLayer::Stdout)
        .with_style(cli_args.fmt_style)
        .with_colours(cli_args.ansi_colors)
        .with_jaeger(cli_args.jaeger.clone())
        .with_events_url(cli_args.events_url.clone())
        .with_tracing_tags(cli_args.tracing_tags.clone())
        .init("agent-ha-node");

    CLUSTER_AGENT_CLIENT
        .set(ClusterAgentClient::new(cli_args.cluster_agent.clone(), None).await)
        .ok()
        .expect("Expect to be initialized only once");

    CSI_NODE_NVME_CLIENT
        .set(get_nvme_connection_client(
            &cli_args.csi_socket,
            TimeoutOptions::new().with_connect_timeout(Duration::from_millis(500)),
        ))
        .expect("Expect to be initialized only once");

    if let Err(error) = cluster_agent_client()
        .register(
            &NodeAgentInfo::new(cli_args.node_name.clone(), cli_args.grpc_endpoint()),
            None,
        )
        .await
    {
        tracing::error!(
            %error,
            "Failed to register HA Node agent with Cluster HA agent"
        );
    }

    // Check whether ana is enabled or not.
    let ana_enabled: bool = utils::check_nvme_core_ana().unwrap_or_default();
    // Instantiate path failure detector along with Nvme cache object.
    let detector = PathFailureDetector::new(&cli_args, ana_enabled);

    let cache = detector.get_cache();

    // Instantiate gRPC server.
    let server = NodeAgentApiServer::new(&cli_args, cache);

    // Start gRPC server and path failure detection loop.
    tokio::select! {
        result = detector.start() => {
            tracing::info!("Path failure detector stopped");
            result?;
            Ok(())
        },
        result = server.serve() => {
            tracing::info!("gRPC server stopped");
            result?;
            Ok(())
        },
    }
}

// helper function to connect to csi-node nvme operations svc over uds.
fn get_nvme_connection_client(
    socket_path: &std::path::Path,
    timeout_options: TimeoutOptions,
) -> NvmeOperationsClient<Channel> {
    let socket_path_cp = socket_path.to_path_buf();
    let channel = Endpoint::try_from("http://[::]:50051")
        .expect("local endpoint should be valid")
        .connect_timeout(timeout_options.connect_timeout())
        .connect_with_connector_lazy(service_fn(move |_: Uri| {
            UnixStream::connect(socket_path_cp.clone())
        }));
    NvmeOperationsClient::new(channel)
}
