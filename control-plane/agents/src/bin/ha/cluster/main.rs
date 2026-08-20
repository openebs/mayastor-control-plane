use clap::Parser;
use grpc::client::CoreClient;
use http::Uri;
use once_cell::sync::OnceCell;
use std::{net::SocketAddr, path::PathBuf};
use tracing::info;
use utils::{
    package_description,
    tracing_telemetry::{FmtLayer, FmtStyle, KeyValue},
    version_info_string, DEFAULT_CLUSTER_AGENT_SERVER_ADDR, DEFAULT_GRPC_CLIENT_ADDR,
};

mod etcd;
mod nodes;
mod server;
mod switchover;
mod volume;

#[derive(Debug, Parser)]
#[structopt(name = package_description!(), version = version_info_string!())]
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
    /// Path to the CA bundle used to verify Core and HA Node Agent gRPC servers and authenticate
    /// HA Cluster Agent gRPC clients.
    #[clap(long = "grpc-tls-ca-file")]
    grpc_tls_ca_file: Option<PathBuf>,
    /// Path to the TLS certificate used by the HA Cluster Agent gRPC server and client.
    #[clap(long = "grpc-tls-cert-file", requires = "grpc_tls_key_file")]
    grpc_tls_cert_file: Option<PathBuf>,
    /// Path to the TLS private key used by the HA Cluster Agent gRPC server and client.
    #[clap(long = "grpc-tls-key-file", requires = "grpc_tls_cert_file")]
    grpc_tls_key_file: Option<PathBuf>,
    /// Auto-generate an ephemeral self-signed certificate for the HA Cluster Agent gRPC server.
    #[clap(long = "grpc-auto-tls", conflicts_with_all = ["grpc_tls_ca_file", "grpc_tls_cert_file", "grpc_tls_key_file"])]
    grpc_auto_tls: bool,

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

    /// Replication factor for the events jetstream.
    #[clap(long)]
    events_replicas: Option<usize>,

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

    fn grpc_tls(&self) -> anyhow::Result<Option<grpc::tls::TlsConfig>> {
        if self.grpc_tls_ca_file.is_some() && self.grpc_tls_cert_file.is_none() {
            anyhow::bail!(
                "a TLS-enabled HA Cluster Agent requires both gRPC TLS certificate and private key files"
            );
        }
        let tls = grpc::tls::TlsConfig::new(
            self.grpc_tls_ca_file.clone(),
            self.grpc_tls_cert_file.clone(),
            self.grpc_tls_key_file.clone(),
        )?;
        Ok(tls.enabled().then_some(tls))
    }
}

/// Once cell static variable to store the grpc client and initialize once at startup.
pub static CORE_CLIENT: OnceCell<CoreClient> = OnceCell::new();
pub static GRPC_TLS: OnceCell<Option<grpc::tls::TlsConfig>> = OnceCell::new();
pub static GRPC_AUTO_TLS: OnceCell<bool> = OnceCell::new();

/// Get Core gRPC Client
pub(crate) fn core_grpc<'a>() -> &'a CoreClient {
    CORE_CLIENT
        .get()
        .expect("gRPC Core Client should have been initialised")
}

pub(crate) fn grpc_tls() -> Option<grpc::tls::TlsConfig> {
    GRPC_TLS.get().cloned().flatten()
}

/// Whether the HA Node Agent gRPC servers are dialled over TLS (file-backed or auto-TLS).
pub(crate) fn grpc_tls_enabled() -> bool {
    grpc_tls().is_some() || GRPC_AUTO_TLS.get().copied().unwrap_or(false)
}

fn initialize_tracing(args: &Cli) {
    utils::tracing_telemetry::TracingTelemetry::builder()
        .with_writer(FmtLayer::Stdout)
        .with_style(args.fmt_style)
        .with_colours(args.ansi_colors)
        .with_jaeger(args.jaeger.clone())
        .with_events(args.events_url.clone(), args.events_replicas)
        .with_tracing_tags(args.tracing_tags.clone())
        .init("agent-ha-cluster");
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    utils::init_rustls_crypto_provider();
    utils::print_package_info!();
    let cli = Cli::args();
    println!("Using options: {cli:?}");
    initialize_tracing(&cli);

    let grpc_tls = cli.grpc_tls()?;
    let grpc_auto_tls = cli.grpc_auto_tls;
    if grpc_tls.is_some() && cli.core_grpc.scheme_str() != Some("https") {
        anyhow::bail!("a TLS-enabled Core gRPC client requires an https:// URL");
    }
    let core_client = match grpc_tls.clone() {
        Some(tls) => CoreClient::new_with_tls(cli.core_grpc.clone(), None, tls).await?,
        None => CoreClient::new(cli.core_grpc.clone(), None).await,
    };
    CORE_CLIENT
        .set(core_client)
        .ok()
        .expect("Expect to be initialised only once");
    GRPC_TLS
        .set(grpc_tls.clone())
        .expect("Expect to be initialised only once");
    GRPC_AUTO_TLS
        .set(grpc_auto_tls)
        .expect("Expect to be initialised only once");

    let store = etcd::EtcdStore::new(cli.store, cli.store_timeout.into()).await?;
    let node_list = nodes::NodeList::new();

    let entries = store.fetch_incomplete_requests().await?;

    // Node list has ref counted list internally.
    let mover = volume::VolumeMover::new(store, cli.fast_requeue, node_list.clone());
    mover.send_switchover_req(entries).await?;

    info!("Starting cluster-agent server");
    let result =
        server::ClusterAgent::new(cli.grpc_endpoint, grpc_tls, grpc_auto_tls, node_list, mover)
            .run()
            .await;
    utils::tracing_telemetry::flush_traces();
    result?;

    Ok(())
}
