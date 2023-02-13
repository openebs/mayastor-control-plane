mod service;

use crate::service::JsonGrpcSvc;
use agents::{Service, ServiceError};
use clap::Parser;
use grpc::{client::CoreClient, operations::jsongrpc::server::JsonGrpcServer};
use http::Uri;
use once_cell::sync::OnceCell;
use std::{net::SocketAddr, sync::Arc};
use tracing::{error, info};
use utils::{DEFAULT_GRPC_CLIENT_ADDR, DEFAULT_JSON_GRPC_SERVER_ADDR};

#[derive(Debug, Parser)]
#[structopt(name = utils::package_description!(), version = utils::version_info_str!())]
struct CliArgs {
    /// The json grpc server URL or address to connect to the its services.
    #[clap(long, short = 'J', default_value = DEFAULT_JSON_GRPC_SERVER_ADDR)]
    json_grpc_server_addr: SocketAddr,

    /// The CORE gRPC client URL or address to connect to the core services.
    #[clap(long, short = 'z', default_value = DEFAULT_GRPC_CLIENT_ADDR)]
    core_grpc: Uri,
}

pub(crate) static CORE_CLIENT: OnceCell<CoreClient> = OnceCell::new();

#[tokio::main]
async fn main() {
    let cli_args = CliArgs::parse();
    utils::print_package_info!();
    info!("Using options: {:?}", &cli_args);

    let grpc_addr = &cli_args.core_grpc;
    // Initialise the core client to be used in rest
    CORE_CLIENT
        .set(CoreClient::new(grpc_addr.clone(), None).await)
        .ok()
        .expect("Expect to be initialised only once");

    server(cli_args).await;
}

async fn server(cli_args: CliArgs) {
    let grpc_addr = cli_args.json_grpc_server_addr;
    let json_grpc_service = JsonGrpcServer::new(Arc::new(JsonGrpcSvc::new())).into_grpc_server();

    let tonic_router = tonic::transport::Server::builder().add_service(json_grpc_service);

    let tonic_thread = tokio::spawn(async move {
        tonic_router
            .serve_with_shutdown(grpc_addr, Service::shutdown_signal())
            .await
            .map_err(|source| ServiceError::GrpcServer { source })
    });

    match tonic_thread.await {
        Err(error) => error!("Failed to wait for thread: {:?}", error),
        Ok(Err(error)) => {
            error!(error=?error, "Error running service thread");
        }
        _ => {}
    }
}
