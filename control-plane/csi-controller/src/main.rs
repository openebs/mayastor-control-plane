use env_logger::{Builder, Env};
use git_version::git_version;
use structopt::StructOpt;

mod client;
pub mod controller;
pub mod identity;
pub use client::{ApiClientError, MayastorApiClient};
mod server;

#[macro_use]
extern crate log;
extern crate env_logger;

const CSI_SOCKET: &str = "/var/tmp/csi.sock";

#[derive(Debug, Clone, StructOpt)]
#[structopt(
    name = "Mayastor CSI Controller",
    about = "test",
    version = git_version!(args = ["--tags", "--abbrev=12"], fallback="unkown"),
    setting(structopt::clap::AppSettings::ColoredHelp)
)]

struct CsiControllerCliArgs {
    #[structopt(short = "r", long = "rest-endpoint")]
    pub rest_endpoint: String,
    #[structopt(short="c", long="csi-socket", default_value=CSI_SOCKET)]
    pub csi_socket: String,
    #[structopt(short="l", long="log-level", default_value="info", possible_values=&["info", "debug", "trace"])]
    pub log_level: String,
}

fn setup_logger(log_level: String) {
    let filter_expr = format!("{}={}", module_path!(), log_level);
    let mut builder = Builder::from_env(Env::default().default_filter_or(filter_expr));
    builder.init();
}

#[tokio::main]
pub async fn main() -> Result<(), String> {
    let args = CsiControllerCliArgs::from_args();
    trace!("{:?}", args);

    setup_logger(args.log_level.to_string());

    info!(
        "Starting Mayastor CSI Controller, Control Plane REST API endpoint is {}",
        args.rest_endpoint
    );

    MayastorApiClient::initialize(args.rest_endpoint.to_string())
        .map_err(|e| format!("Failed to initialize API client, error = {}", e))?;
    server::CsiServer::run(args.csi_socket.to_string()).await
}
