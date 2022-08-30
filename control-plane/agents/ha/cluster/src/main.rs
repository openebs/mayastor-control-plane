use std::net::SocketAddr;

use structopt::StructOpt;
use utils::{package_description, version_info_str, DEFAULT_CLUSTER_AGENT_SERVER_ADDR};

mod server;

#[derive(Debug, StructOpt)]
#[structopt(name = package_description!(), version = version_info_str!())]
struct Cli {
    /// IP address and port for the cluster-agent to listen on
    #[structopt(long, short, default_value = DEFAULT_CLUSTER_AGENT_SERVER_ADDR)]
    grpc_endpoint: SocketAddr,
}

impl Cli {
    fn args() -> Self {
        Cli::from_args()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::args();

    server::ClusterAgent::new(cli.grpc_endpoint)
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("Error running server: {e}"))
}
