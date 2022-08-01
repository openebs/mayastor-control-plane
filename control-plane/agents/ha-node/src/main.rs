use grpc::operations::ha_node::{client::ClusterAgentClient, traits::ClusterAgentOperations};
use http::Uri;
use once_cell::sync::OnceCell;
use structopt::StructOpt;
use utils::{
    package_description, version_info_str, DEFAULT_CLUSTER_AGENT_SERVER_ADDR,
    DEFAULT_NODE_AGENT_SERVER_ADDR,
};

#[derive(Debug, StructOpt)]
#[structopt(name = package_description!(), version = version_info_str!())]
struct Cli {
    /// HA Cluster Agent URL or address to connect to the services.
    #[structopt(long, short, default_value = DEFAULT_CLUSTER_AGENT_SERVER_ADDR)]
    cluster_agent: Uri,

    /// Node name(spec.nodeName). This must be the same as provided in csi-node
    #[structopt(short, long)]
    node_name: String,

    /// IP address and port for the ha node-agent to listen on
    #[structopt(short, long, default_value = DEFAULT_NODE_AGENT_SERVER_ADDR)]
    grpc_endpoint: Uri,
}

static CLUSTER_AGENT_CLIENT: OnceCell<ClusterAgentClient> = OnceCell::new();

fn cluster_agent_client() -> &'static ClusterAgentClient {
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
    let cli = Cli::args();

    CLUSTER_AGENT_CLIENT
        .set(ClusterAgentClient::new(cli.cluster_agent, None).await)
        .ok()
        .expect("Expect to be initialized only once");

    cluster_agent_client()
        .register(cli.node_name.clone(), cli.grpc_endpoint)
        .await
        .unwrap();
}
