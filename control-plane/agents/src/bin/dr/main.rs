mod service;
mod store;

use agents::Service;
use clap::Parser;
use grpc::{client::CoreClient, operations::dr::server::DrServer};
use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use tracing::info;
use utils::{
    package_description,
    tracing_telemetry::{FmtLayer, FmtStyle, KeyValue},
    version_info_string, DEFAULT_DR_AGENT_SERVER_ADDR, DEFAULT_GRPC_CLIENT_ADDR,
};

#[derive(Debug, Parser)]
#[structopt(name = package_description!(), version = version_info_string!())]
struct Cli {
    /// IP address and port for the dr-agent to listen on.
    #[clap(long, short, default_value = DEFAULT_DR_AGENT_SERVER_ADDR)]
    grpc_endpoint: SocketAddr,

    /// Core gRPC server URL or address, which answers whether a volume exists.
    #[clap(long, short = 'z', default_value = DEFAULT_GRPC_CLIENT_ADDR)]
    core_grpc: http::Uri,

    /// Path to the CA bundle used to verify the Core gRPC server and authenticate dr-agent
    /// gRPC clients.
    #[clap(long = "grpc-tls-ca-file")]
    grpc_tls_ca_file: Option<PathBuf>,
    /// Path to the TLS certificate used by the dr-agent gRPC server and client.
    #[clap(long = "grpc-tls-cert-file", requires = "grpc_tls_key_file")]
    grpc_tls_cert_file: Option<PathBuf>,
    /// Path to the TLS private key used by the dr-agent gRPC server and client.
    #[clap(long = "grpc-tls-key-file", requires = "grpc_tls_cert_file")]
    grpc_tls_key_file: Option<PathBuf>,
    /// Auto-generate an ephemeral self-signed certificate for the dr-agent gRPC server.
    #[clap(long = "grpc-auto-tls", conflicts_with_all = ["grpc_tls_ca_file", "grpc_tls_cert_file", "grpc_tls_key_file"])]
    grpc_auto_tls: bool,

    /// The persistent store endpoint (supports the http/https schema).
    #[clap(long, short, default_value = "http://localhost:2379")]
    store: http::Uri,

    /// The timeout for store operations.
    #[clap(long, default_value = utils::STORE_OP_TIMEOUT)]
    store_timeout: humantime::Duration,

    /// Sends opentelemetry spans to the Jaeger endpoint agent.
    #[clap(long, short)]
    jaeger: Option<String>,

    /// Add process service tags to the traces.
    #[clap(short, long, env = "TRACING_TAGS", value_delimiter=',', value_parser = utils::tracing_telemetry::parse_key_value)]
    tracing_tags: Vec<KeyValue>,

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

    /// Crypto options.
    #[clap(flatten)]
    crypto: utils::CryptoArgs,
}

impl Cli {
    fn grpc_tls(&self) -> anyhow::Result<Option<grpc::tls::TlsConfig>> {
        if self.grpc_tls_ca_file.is_some() && self.grpc_tls_cert_file.is_none() {
            anyhow::bail!(
                "a TLS-enabled dr agent requires both gRPC TLS certificate and private key files"
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

fn initialize_tracing(args: &Cli) {
    utils::tracing_telemetry::TracingTelemetry::builder()
        .with_writer(FmtLayer::Stdout)
        .with_style(args.fmt_style)
        .with_colours(args.ansi_colors)
        .with_jaeger(args.jaeger.clone())
        .with_events(args.events_url.clone(), args.events_replicas)
        .with_tracing_tags(args.tracing_tags.clone())
        .init("agent-dr");
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    utils::print_package_info!();
    let cli = Cli::parse();
    cli.crypto.init()?;
    println!("Using options: {cli:?}");
    initialize_tracing(&cli);

    // The cluster's own identity, which a pair records and which is not the caller's to
    // choose. Core keys its persistent store on the same value.
    stor_port::platform::init_cluster_info_or_panic().await;

    let grpc_tls = cli.grpc_tls()?;
    if grpc_tls.is_some() && cli.core_grpc.scheme_str() != Some("https") {
        anyhow::bail!("a TLS-enabled Core gRPC client requires an https:// URL");
    }
    let core = match grpc_tls.clone() {
        Some(tls) => CoreClient::new_with_tls(cli.core_grpc.clone(), None, tls).await?,
        None => CoreClient::new(cli.core_grpc.clone(), None).await,
    };
    let dr = service::new(&cli.store, cli.store_timeout.into(), core).await?;
    let service = Service::builder().with_service(DrServer::new(Arc::new(dr)).into_grpc_server());

    info!("Starting dr-agent server");
    let result = match (cli.grpc_auto_tls, grpc_tls) {
        (true, None) => service.run_auto_tls_err(cli.grpc_endpoint, false).await,
        (true, Some(_)) => unreachable!("clap prevents combining gRPC TLS files and auto TLS"),
        (false, Some(tls)) => service.run_tls_err(cli.grpc_endpoint, tls, false).await,
        (false, None) => service.run_err(cli.grpc_endpoint).await,
    };
    utils::tracing_telemetry::flush_traces();

    Ok(result?)
}
