use opentelemetry::KeyValue;
use std::net::SocketAddr;

use structopt::StructOpt;
use utils::{package_description, version_info_str, DEFAULT_CLUSTER_AGENT_SERVER_ADDR};

mod server;

#[derive(Debug, StructOpt)]
#[structopt(name = package_description!(), version = version_info_str!())]
struct Cli {
    /// IP address and port for the cluster-agent to listen on.
    #[structopt(long, short, default_value = DEFAULT_CLUSTER_AGENT_SERVER_ADDR)]
    grpc_endpoint: SocketAddr,

    /// Sends opentelemetry spans to the Jaeger endpoint agent.
    #[structopt(long, short)]
    jaeger: Option<String>,

    /// Add process service tags to the traces.
    #[structopt(short, long, env = "TRACING_TAGS", value_delimiter=",", parse(try_from_str = utils::tracing_telemetry::parse_key_value))]
    tracing_tags: Vec<KeyValue>,
}

impl Cli {
    fn args() -> Self {
        Cli::from_args()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    utils::print_package_info!();

    let cli = Cli::args();

    utils::tracing_telemetry::init_tracing(
        "agent-ha-cluster",
        cli.tracing_tags.clone(),
        cli.jaeger.clone(),
    );

    server::ClusterAgent::new(cli.grpc_endpoint)
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("Error running server: {e}"))
}
