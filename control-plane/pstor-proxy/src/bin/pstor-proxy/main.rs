use crate::{
    error::SvcError,
    frontend_node::{
        server::{FrontendNodeRegistrationServer, FrontendNodeServer},
        service::Service,
    },
    registry::Registry,
};
use clap::Parser;
use std::net::SocketAddr;
use utils::{version_info_str, DEFAULT_GRPC_SERVER_ADDR};

mod error;
pub(crate) mod frontend_node;
pub(crate) mod registry;
pub(crate) mod resource_map;

/// The Cli arguments for this binary.
#[derive(Debug, Parser)]
#[structopt(name = utils::package_description!(), version = version_info_str!())]
pub(crate) struct CliArgs {
    /// The Persistent Store URLs to connect to.
    /// (supports the http/https schema)
    #[clap(long, short, default_value = "http://localhost:2379")]
    pub(crate) store: String,

    /// The timeout for store operations.
    #[clap(long, default_value = utils::STORE_OP_TIMEOUT)]
    pub(crate) store_timeout: humantime::Duration,

    /// The lease lock ttl for the persistent store after which we'll lose the exclusive access.
    #[clap(long, default_value = utils::STORE_LEASE_LOCK_TTL)]
    pub(crate) store_lease_ttl: humantime::Duration,

    // The GRPC Server URLs to connect to.
    /// (supports the http/https schema)
    #[clap(long, short, default_value = DEFAULT_GRPC_SERVER_ADDR)]
    pub(crate) grpc_server_addr: SocketAddr,
}
impl CliArgs {
    fn args() -> Self {
        CliArgs::parse()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli_args = CliArgs::args();
    utils::print_package_info!();

    let _ = server(cli_args).await;

    Ok(())
}

async fn server(cli_args: CliArgs) -> anyhow::Result<()> {
    let registry = Registry::new(
        cli_args.store.clone(),
        cli_args.store_timeout.into(),
        cli_args.store_lease_ttl.into(),
    )
    .await?;

    let frontend_node_registration_service =
        FrontendNodeRegistrationServer::new(Service::new(registry.clone())).into_grpc_server();
    let frontend_node_service =
        FrontendNodeServer::new(Service::new(registry.clone())).into_grpc_server();
    tonic::transport::Server::builder()
        .add_service(frontend_node_registration_service)
        .add_service(frontend_node_service)
        .serve_with_shutdown(cli_args.grpc_server_addr, shutdown_signal())
        .await
        .map_err(|error| SvcError::GrpcServer { source: error })?;

    registry.stop().await;
    Ok(())
}

/// Waits until the process receives a shutdown: either TERM or INT.
pub async fn shutdown_signal() {
    shutdown::Shutdown::wait().await;
}
