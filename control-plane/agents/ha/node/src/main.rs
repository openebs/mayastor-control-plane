use common_lib::types::v0::transport::cluster_agent::NodeAgentInfo;
use grpc::operations::ha_node::{client::ClusterAgentClient, traits::ClusterAgentOperations};
use http::Uri;
use once_cell::sync::OnceCell;
use opentelemetry::KeyValue;
use structopt::StructOpt;
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
pub(crate) struct Cli {
    /// HA Cluster Agent URL or address to connect to the services.
    #[structopt(long, short, default_value = DEFAULT_CLUSTER_AGENT_CLIENT_ADDR)]
    cluster_agent: Uri,

    /// Node name(spec.nodeName). This must be the same as provided in csi-node.
    #[structopt(short, long)]
    node_name: String,

    /// IP address and port for the ha node-agent to listen on.
    #[structopt(short, long, default_value = DEFAULT_NODE_AGENT_SERVER_ADDR)]
    grpc_endpoint: Uri,

    /// Add process service tags to the traces
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

    /// Trace rest requests to the Jaeger endpoint agent.
    #[structopt(long, short)]
    jaeger: Option<String>,
}

static CLUSTER_AGENT_CLIENT: OnceCell<ClusterAgentClient> = OnceCell::new();

pub fn cluster_agent_client() -> &'static ClusterAgentClient {
    CLUSTER_AGENT_CLIENT
        .get()
        .expect("HA Cluster-Agent client should have been initialized")
}

impl Cli {
    fn args() -> Self {
        Cli::from_args()
    }
}

#[tokio::main]
async fn main() {
    let cli_args = Cli::args();

    println!("** START **");
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

    if let Err(e) = cluster_agent_client()
        .register(&NodeAgentInfo::new(
            cli_args.node_name.clone(),
            cli_args
                .grpc_endpoint
                .authority()
                .unwrap()
                .to_string()
                .parse()
                .unwrap(),
        ))
        .await
    {
        tracing::error!(
            "Failed to register HA Node agent in Cluster HA agent: {:?}",
            e
        );
    }

    // Instantiate path failure detector along with Nvme cache object.
    let detector =
        PathFailureDetector::new(&cli_args).expect("Failed to initialize path failure detector");

    let cache = detector.get_cache();

    // Instantiate gRPC server.
    let server = NodeAgentApiServer::new(cli_args.grpc_endpoint.clone(), cache);

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
