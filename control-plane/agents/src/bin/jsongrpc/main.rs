mod service;

use crate::service::JsonGrpcSvc;
use agents::Service;
use clap::Parser;
use grpc::{client::CoreClient, operations::jsongrpc::server::JsonGrpcServer};
use http::Uri;
use once_cell::sync::OnceCell;
use std::{net::SocketAddr, sync::Arc};
use tracing::{error, info};
use utils::{DEFAULT_GRPC_CLIENT_ADDR, DEFAULT_JSON_GRPC_SERVER_ADDR};

#[derive(Debug, Parser)]
#[structopt(name = utils::package_description!(), version = utils::version_info_string!())]
struct CliArgs {
    /// The json grpc server URL or address to connect to the its services.
    #[clap(long, short = 'J', default_value = DEFAULT_JSON_GRPC_SERVER_ADDR)]
    json_grpc_server_addr: SocketAddr,

    /// The CORE gRPC client URL or address to connect to the core services.
    #[clap(long, short = 'z', default_value = DEFAULT_GRPC_CLIENT_ADDR)]
    core_grpc: Uri,

    /// Path to the TLS server certificate chain for the JsonGrpc gRPC server.
    #[clap(long = "grpc-tls-cert-file", requires = "grpc_tls_key_file")]
    grpc_tls_cert_file: Option<std::path::PathBuf>,
    /// Path to the TLS server private key for the JsonGrpc gRPC server.
    #[clap(long = "grpc-tls-key-file", requires = "grpc_tls_cert_file")]
    grpc_tls_key_file: Option<std::path::PathBuf>,
    /// Path to the CA bundle used to authenticate JsonGrpc gRPC clients.
    #[clap(long = "grpc-tls-ca-file")]
    grpc_tls_ca_file: Option<std::path::PathBuf>,
    /// Auto-generate an ephemeral self-signed certificate for the JsonGrpc gRPC server.
    #[clap(long = "grpc-auto-tls", conflicts_with_all = ["grpc_tls_cert_file", "grpc_tls_key_file", "grpc_tls_ca_file"])]
    grpc_auto_tls: bool,
}

impl CliArgs {
    fn grpc_tls(&self) -> anyhow::Result<Option<grpc::tls::TlsConfig>> {
        match (&self.grpc_tls_cert_file, &self.grpc_tls_key_file) {
            (None, None) => Ok(None),
            (Some(certificate), Some(private_key)) => grpc::tls::TlsConfig::new(
                self.grpc_tls_ca_file.clone(),
                Some(certificate.clone()),
                Some(private_key.clone()),
            )
            .map(Some),
            _ => unreachable!("clap requires the gRPC TLS certificate and private key together"),
        }
    }
}

pub(crate) static CORE_CLIENT: OnceCell<CoreClient> = OnceCell::new();

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    utils::init_rustls_crypto_provider();
    let cli_args = CliArgs::parse();
    utils::print_package_info!();
    utils::tracing_telemetry::TracingTelemetry::builder().init("agent-jsongrpc");
    info!("Using options: {:?}", &cli_args);

    let grpc_addr = &cli_args.core_grpc;
    // Initialise the core client to be used in rest
    CORE_CLIENT
        .set(CoreClient::new(grpc_addr.clone(), None).await)
        .ok()
        .expect("Expect to be initialised only once");

    server(cli_args).await
}

async fn server(cli_args: CliArgs) -> anyhow::Result<()> {
    let grpc_addr = cli_args.json_grpc_server_addr;
    let tls = cli_args.grpc_tls()?;
    let json_grpc_service = JsonGrpcServer::new(Arc::new(JsonGrpcSvc::new())).into_grpc_server();

    let service = Service::builder().with_service(json_grpc_service);

    let tonic_thread = tokio::spawn(async move {
        match (cli_args.grpc_auto_tls, tls) {
            (true, None) => service.run_auto_tls_err(grpc_addr, false).await,
            (true, Some(_)) => {
                unreachable!("clap prevents combining gRPC TLS files and auto TLS")
            }
            (false, Some(tls)) => service.run_tls_err(grpc_addr, tls, false).await,
            (false, None) => service.run_err(grpc_addr).await,
        }
    });

    let result = tonic_thread.await;
    match &result {
        Err(error) => error!("Failed to wait for thread: {error:?}"),
        Ok(Err(error)) => error!(?error, "Error running service thread"),
        _ => {}
    }
    Ok(result??)
}
