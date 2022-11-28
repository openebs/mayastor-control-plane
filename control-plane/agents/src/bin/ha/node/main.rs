use agents::errors::SvcError;
use common_lib::{
    transport_api::TimeoutOptions, types::v0::transport::cluster_agent::NodeAgentInfo,
};
use grpc::{
    csi_node_nvme::nvme_operations_client::NvmeOperationsClient,
    operations::ha_node::{client::ClusterAgentClient, traits::ClusterAgentOperations},
};
use http::Uri;
use once_cell::sync::OnceCell;
use opentelemetry::KeyValue;
use std::{net::SocketAddr, time::Duration};
use structopt::StructOpt;
use tokio::net::UnixStream;
use tonic::transport::{Channel, Endpoint};
use tower::service_fn;
use utils::{
    package_description, version_info_str, DEFAULT_CLUSTER_AGENT_CLIENT_ADDR,
    DEFAULT_NODE_AGENT_SERVER_ADDR, NVME_PATH_AGGREGATION_PERIOD, NVME_PATH_CHECK_PERIOD,
    NVME_PATH_RETRANSMISSION_PERIOD,
};
mod detector;
mod path_provider;
mod reporter;
mod server;

use detector::PathFailureDetector;
use server::NodeAgentApiServer;

/// TODO
#[derive(Debug, StructOpt)]
#[structopt(name = package_description!(), version = version_info_str!())]
struct Cli {
    /// HA Cluster Agent URL or address to connect to the services.
    #[structopt(long, short, default_value = DEFAULT_CLUSTER_AGENT_CLIENT_ADDR)]
    cluster_agent: Uri,

    /// Node name(spec.nodeName). This must be the same as provided in csi-node.
    #[structopt(short, long)]
    node_name: String,

    /// IP address and port for the ha node-agent to listen on.
    #[structopt(short, long, default_value = DEFAULT_NODE_AGENT_SERVER_ADDR)]
    grpc_endpoint: SocketAddr,

    /// Add process service tags to the traces.
    #[structopt(short, long, env = "TRACING_TAGS", value_delimiter=",", parse(try_from_str = utils::tracing_telemetry::parse_key_value))]
    tracing_tags: Vec<KeyValue>,

    /// Path failure detection period.
    #[structopt(short, long, env = "DETECTION_PERIOD", default_value = NVME_PATH_CHECK_PERIOD)]
    detection_period: humantime::Duration,

    /// Retransmission period for reporting failed paths in case of network issues.
    #[structopt(short, long, env = "RETRANSMISSION_PERIOD", default_value = NVME_PATH_RETRANSMISSION_PERIOD)]
    retransmission_period: humantime::Duration,

    /// Period for aggregating multiple failed paths before reporting them.
    #[structopt(short, long, env = "AGGREGATION_PERIOD", default_value = NVME_PATH_AGGREGATION_PERIOD)]
    aggregation_period: humantime::Duration,

    /// Sends opentelemetry spans to the Jaeger endpoint agent.
    #[structopt(long, short)]
    jaeger: Option<String>,

    /// The csi-node socket file for grpc over uds.
    #[structopt(long)]
    csi_socket: String,
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
        Cli::from_args()
    }
}

#[tokio::main]
async fn main() {
    let cli_args = Cli::args();

    utils::print_package_info!();

    utils::tracing_telemetry::init_tracing(
        "agent-ha-node",
        cli_args.tracing_tags.clone(),
        cli_args.jaeger.clone(),
    );

    CLUSTER_AGENT_CLIENT
        .set(ClusterAgentClient::new(cli_args.cluster_agent.clone(), None).await)
        .ok()
        .expect("Expect to be initialized only once");

    CSI_NODE_NVME_CLIENT
        .set(
            get_nvme_connection_client(
                cli_args.csi_socket.as_str(),
                TimeoutOptions::new().with_connect_timeout(Duration::from_millis(500)),
            )
            .expect("Expect to get the csi node client"),
        )
        .expect("Expect to be initialized only once");

    if let Err(error) = cluster_agent_client()
        .register(
            &NodeAgentInfo::new(cli_args.node_name.clone(), cli_args.grpc_endpoint),
            None,
        )
        .await
    {
        tracing::error!(
            %error,
            "Failed to register HA Node agent with Cluster HA agent"
        );
    }

    // Instantiate path failure detector along with Nvme cache object.
    let detector =
        PathFailureDetector::new(&cli_args).expect("Failed to initialize path failure detector");

    let cache = detector.get_cache();

    // Instantiate gRPC server.
    let server = NodeAgentApiServer::new(cli_args.grpc_endpoint, cache);

    // Start gRPC server and path failure detection loop.
    tokio::select! {
        _ = detector.start() => {
            tracing::info!("Path failure detector stopped.")
        },
        _ = server.serve() => {
            tracing::info!("gRPC server stopped.");
        },
    }
}

// helper function to connect to csi-node nvme operations svc over uds.
fn get_nvme_connection_client(
    socket_path: &str,
    timeout_options: TimeoutOptions,
) -> Result<NvmeOperationsClient<Channel>, SvcError> {
    let socket_path_cp = socket_path.to_string();
    let channel = Endpoint::try_from("http://[::]:50051")
        .map_err(|_| SvcError::InvalidArguments {})?
        .connect_timeout(timeout_options.connect_timeout())
        .connect_with_connector_lazy(service_fn(move |_: Uri| {
            UnixStream::connect(socket_path_cp.to_string())
        }));
    Ok(NvmeOperationsClient::new(channel))
}
