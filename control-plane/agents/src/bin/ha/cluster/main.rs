use clap::Parser;
use grpc::client::CoreClient;
use http::Uri;
use once_cell::sync::OnceCell;
use std::net::SocketAddr;
use tracing::info;
use utils::{
    package_description,
    tracing_telemetry::{FmtLayer, FmtStyle, KeyValue},
    version_info_str, DEFAULT_CLUSTER_AGENT_SERVER_ADDR, DEFAULT_GRPC_CLIENT_ADDR,
};

mod etcd;
mod nodes;
mod server;
mod switchover;
mod volume;

#[derive(Debug, Parser)]
#[structopt(name = package_description!(), version = version_info_str!())]
struct Cli {
    /// IP address and port for the cluster-agent to listen on.
    #[clap(long, short, default_value = DEFAULT_CLUSTER_AGENT_SERVER_ADDR)]
    grpc_endpoint: SocketAddr,

    /// The Persistent Store URL to connect to.
    #[clap(long, short, default_value = "http://localhost:2379")]
    store: Uri,

    /// Timeout for store operation.
    #[clap(long, default_value = utils::STORE_OP_TIMEOUT)]
    store_timeout: humantime::Duration,

    /// Core gRPC server URL or address.
    #[clap(long, short, default_value = DEFAULT_GRPC_CLIENT_ADDR)]
    core_grpc: Uri,

    /// Sends opentelemetry spans to the Jaeger endpoint agent.
    #[clap(long, short)]
    jaeger: Option<String>,

    /// Add process service tags to the traces.
    #[clap(short, long, env = "TRACING_TAGS", value_delimiter=',', value_parser = utils::tracing_telemetry::parse_key_value)]
    tracing_tags: Vec<KeyValue>,

    /// If set, configures the fast requeue period to this duration.
    #[clap(long)]
    fast_requeue: Option<humantime::Duration>,

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

impl Cli {
    fn args() -> Self {
        Cli::parse()
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
    utils::tracing_telemetry::TracingTelemetry::builder()
        .with_writer(FmtLayer::Stdout)
        .with_style(args.fmt_style)
        .with_colours(args.ansi_colors)
        .with_jaeger(args.jaeger.clone())
        .with_events_url(args.events_url.clone())
        .with_tracing_tags(args.tracing_tags.clone())
        .init("agent-ha-cluster");
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    utils::print_package_info!();
    let cli = Cli::args();
    println!("Using options: {cli:?}");
    initialize_tracing(&cli);

    // Initialise the core client to be used in rest
    CORE_CLIENT
        .set(CoreClient::new(cli.core_grpc, None).await)
        .ok()
        .expect("Expect to be initialised only once");

    let store = etcd::EtcdStore::new(cli.store, cli.store_timeout.into()).await?;
    let node_list = nodes::NodeList::new();

    let entries = store.fetch_incomplete_requests().await?;

    // Node list has ref counted list internally.
    let mover = volume::VolumeMover::new(store, cli.fast_requeue, node_list.clone());
    mover.send_switchover_req(entries).await?;

    info!("Starting cluster-agent server");
    let result = server::ClusterAgent::new(cli.grpc_endpoint, node_list, mover)
        .run()
        .await;
    utils::tracing_telemetry::flush_traces();
    result?;

    Ok(())
}
