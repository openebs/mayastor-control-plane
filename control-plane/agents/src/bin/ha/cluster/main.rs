use grpc::client::CoreClient;
use http::Uri;
use once_cell::sync::OnceCell;
use opentelemetry::{global, KeyValue};
use std::net::SocketAddr;
use structopt::StructOpt;
use tracing::info;
use utils::{
    package_description, version_info_str, DEFAULT_CLUSTER_AGENT_SERVER_ADDR,
    DEFAULT_GRPC_CLIENT_ADDR,
};
mod etcd;
mod nodes;
mod server;
mod switchover;
mod volume;

#[derive(Debug, StructOpt)]
#[structopt(name = package_description!(), version = version_info_str!())]
struct Cli {
    /// IP address and port for the cluster-agent to listen on.
    #[structopt(long, short, default_value = DEFAULT_CLUSTER_AGENT_SERVER_ADDR)]
    grpc_endpoint: SocketAddr,

    /// The Persistent Store URL to connect to.
    #[structopt(long, short, default_value = "http://localhost:2379")]
    store: Uri,

    /// Timeout for store operation.
    #[structopt(long, default_value = utils::STORE_OP_TIMEOUT)]
    store_timeout: humantime::Duration,

    /// Core gRPC server URL or address.
    #[structopt(long, short, default_value = DEFAULT_GRPC_CLIENT_ADDR)]
    core_grpc: Uri,

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

/// Once cell static variable to store the grpc client and initialize once at startup.
pub static CORE_CLIENT: OnceCell<CoreClient> = OnceCell::new();

/// Get Core gRPC Client
pub(crate) fn core_grpc<'a>() -> &'a CoreClient {
    CORE_CLIENT
        .get()
        .expect("gRPC Core Client should have been initialised")
}

fn initialize_tracing(args: &Cli) {
    utils::tracing_telemetry::init_tracing(
        "agent-ha-cluster",
        args.tracing_tags.clone(),
        args.jaeger.clone(),
    )
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    utils::print_package_info!();

    let cli = Cli::args();

    initialize_tracing(&cli);

    // Initialise the core client to be used in rest
    CORE_CLIENT
        .set(CoreClient::new(cli.core_grpc, None).await)
        .ok()
        .expect("Expect to be initialised only once");

    let store = etcd::EtcdStore::new(cli.store, cli.store_timeout.into()).await?;
    let node_list = nodes::NodeList::new();

    // Node list has ref counted list internally.
    let mover = volume::VolumeMover::new(store.clone(), node_list.clone());

    let entries = store.fetch_incomplete_requests().await?;
    mover.send_switchover_req(entries).await?;

    info!("starting cluster-agent server");
    server::ClusterAgent::new(cli.grpc_endpoint, node_list, mover)
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("Error running server: {e}"))?;

    global::shutdown_tracer_provider();
    Ok(())
}
