mod service;

use crate::service::DrAgentSvc;
use agents::Service;
use clap::Parser;
use grpc::operations::dr::server::DrServer;
use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use tracing::info;
use utils::{
    package_description,
    tracing_telemetry::{FmtLayer, FmtStyle, KeyValue},
    version_info_string, DEFAULT_DR_AGENT_SERVER_ADDR,
};

#[derive(Debug, Parser)]
#[structopt(name = package_description!(), version = version_info_string!())]
struct Cli {
    /// IP address and port for the dr-agent to listen on.
    #[clap(long, short, default_value = DEFAULT_DR_AGENT_SERVER_ADDR)]
    grpc_endpoint: SocketAddr,

    /// Path to the CA bundle used to authenticate dr-agent gRPC clients.
    #[clap(long = "grpc-tls-ca-file")]
    grpc_tls_ca_file: Option<PathBuf>,
    /// Path to the TLS certificate used by the dr-agent gRPC server.
    #[clap(long = "grpc-tls-cert-file", requires = "grpc_tls_key_file")]
    grpc_tls_cert_file: Option<PathBuf>,
    /// Path to the TLS private key used by the dr-agent gRPC server.
    #[clap(long = "grpc-tls-key-file", requires = "grpc_tls_cert_file")]
    grpc_tls_key_file: Option<PathBuf>,
    /// Auto-generate an ephemeral self-signed certificate for the dr-agent gRPC server.
    #[clap(long = "grpc-auto-tls", conflicts_with_all = ["grpc_tls_ca_file", "grpc_tls_cert_file", "grpc_tls_key_file"])]
    grpc_auto_tls: bool,

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

    let grpc_tls = cli.grpc_tls()?;
    let service = Service::builder()
        .with_service(DrServer::new(Arc::new(DrAgentSvc::new())).into_grpc_server());

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
