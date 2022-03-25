#[macro_use]
extern crate prettytable;
mod collect;
mod operations;

use collect::{error::Error, rest_wrapper::rest_wrapper_client};
use openapi::tower::client::Url;
use operations::{Operations, Resource};
use std::path::PathBuf;
use structopt::StructOpt;

/// Supportability tool collects state & log information of mayastor services runningin
/// in the system
#[derive(Debug, Clone, StructOpt)]
#[structopt(name = "mayastor-cli", about = "Supportability tool for Mayastor")]
struct CliArgs {
    /// The rest endpoint, parsed from KUBECONFIG, if left empty
    #[structopt(global = true, short, long)]
    rest: Option<Url>,

    /// Specifies the timeout value to interact with other modules of system
    #[structopt(global = true, long, short, default_value = "10s")]
    timeout: humantime::Duration,

    /// Period states to collect all logs from last specified duration
    #[structopt(global = true, long, short, default_value = "24h")]
    since: humantime::Duration,

    /// Endpoint of LOKI service, if left empty then logs will be collected from Kube-apiserver
    #[structopt(global = true, short, long)]
    loki_endpoint: Option<String>,

    /// Output directory path to store archive file
    #[structopt(global = true, long, short = "o", default_value = "./")]
    output_directory_path: String,

    /// Kubernetes namespace of mayastor service, defaults to mayastor
    #[structopt(global = true, long, short = "n", default_value = "mayastor")]
    namespace: String,

    /// Path to kubeconfig file
    #[structopt(parse(from_os_str), long, short = "k")]
    kube_config_path: Option<PathBuf>,

    /// Supported subcommand options
    #[structopt(subcommand)]
    operations: Operations,
}

impl CliArgs {
    fn args() -> Self {
        CliArgs::from_args()
    }
}

#[tokio::main]
async fn main() {
    let args = CliArgs::args();
    let ret = execute(args).await;
    if let Err(_e) = ret {
        std::process::exit(1);
    }
}

async fn execute(cli_args: CliArgs) -> Result<(), Error> {
    // Initialise the REST client.
    let rest_client = rest_wrapper_client::RestClient::new(
        cli_args.clone().rest,
        std::time::Duration::new(cli_args.timeout.as_secs(), 0),
    )
    .expect("Failed to initialise REST client");

    // TODO: Move code inside options to some generic function
    // Perform the operations based on user choosen subcommands
    match &cli_args.clone().operations {
        Operations::Dump(resource) => {
            execute_resource_dump(cli_args, resource.clone(), rest_client).await
        }
    }
}

async fn execute_resource_dump(
    cli_args: CliArgs,
    resource: Resource,
    rest_client: &'static rest_wrapper_client::RestClient,
) -> Result<(), Error> {
    match resource {
        Resource::System => {
            let mut system_dumper = collect::system_dump::SystemDumper::get_or_panic_system_dumper(
                rest_client,
                cli_args.output_directory_path,
                cli_args.namespace,
                cli_args.loki_endpoint,
                cli_args.since,
                cli_args.kube_config_path,
                cli_args.timeout,
            )
            .await;
            if let Err(e) = system_dumper.dump_system().await {
                println!("Failed to dump system state, error: {:?}", e);
                return Err(e);
            }
        }
    }
    Ok(())
}
