#[macro_use]
extern crate prettytable;
mod collect;
mod operations;

use collect::{
    common::DumpConfig,
    error::Error,
    resource_dump::ResourceDumper,
    resources::{
        node::NodeClientWrapper, pool::PoolClientWrapper, traits::Topologer,
        volume::VolumeClientWrapper, Resourcer,
    },
    rest_wrapper::rest_wrapper_client,
};
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

    /// Endpoint of LOKI service, if left empty then it will try to parse endpoint
    /// from Loki service(K8s service resource), if the tool is unable to parse
    /// from service then logs will be collected using Kube-apiserver
    #[structopt(global = true, short, long)]
    loki_endpoint: Option<String>,

    /// Endpoint of ETCD service, if left empty then will be parsed from the internal service name
    #[structopt(global = true, short, long)]
    etcd_endpoint: Option<String>,

    /// Output directory path to store archive file
    #[structopt(global = true, long, short = "o", default_value = "./")]
    output_directory_path: String,

    /// Kubernetes namespace of mayastor service, defaults to mayastor
    #[structopt(global = true, long, short = "n", default_value = "mayastor")]
    namespace: String,

    /// Path to kubeconfig file
    #[structopt(parse(from_os_str), global = true, long, short = "k")]
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
    let topologer: Box<dyn Topologer>;
    let mut config = DumpConfig {
        rest_client,
        output_directory: cli_args.output_directory_path,
        namespace: cli_args.namespace,
        loki_uri: cli_args.loki_endpoint,
        etcd_uri: cli_args.etcd_endpoint,
        since: cli_args.since,
        kube_config_path: cli_args.kube_config_path,
        timeout: cli_args.timeout,
        topologer: None,
    };
    match resource {
        Resource::System => {
            let mut system_dumper =
                collect::system_dump::SystemDumper::get_or_panic_system_dumper(config).await;
            if let Err(e) = system_dumper.dump_system().await {
                println!("Failed to dump system state, error: {:?}", e);
                return Err(e);
            }
        }
        Resource::Volumes => {
            let volume_client = VolumeClientWrapper::new(rest_client);
            topologer = volume_client.get_topologer(None).await?;
            config.topologer = Some(topologer);
            let mut dumper = ResourceDumper::get_or_panic_resource_dumper(config).await;
            if let Err(e) = dumper.dump_info().await {
                println!("Failed to dump volumes information, Error: {:?}", e);
                return Err(e);
            }
        }
        Resource::Volume { id } => {
            let volume_client = VolumeClientWrapper::new(rest_client);
            topologer = volume_client.get_topologer(Some(id)).await?;
            config.topologer = Some(topologer);
            let mut dumper = ResourceDumper::get_or_panic_resource_dumper(config).await;
            if let Err(e) = dumper.dump_info().await {
                println!("Failed to dump volume {} information, Error: {:?}", id, e);
                return Err(e);
            }
        }
        Resource::Pools => {
            let pool_client = PoolClientWrapper::new(rest_client);
            topologer = pool_client.get_topologer(None).await?;
            config.topologer = Some(topologer);
            let mut dumper = ResourceDumper::get_or_panic_resource_dumper(config).await;
            if let Err(e) = dumper.dump_info().await {
                println!("Failed to dump pools information, Error: {:?}", e);
                return Err(e);
            }
        }
        Resource::Pool { id } => {
            let pool_client = PoolClientWrapper::new(rest_client);
            topologer = pool_client.get_topologer(Some(id.to_string())).await?;
            config.topologer = Some(topologer);
            let mut dumper = ResourceDumper::get_or_panic_resource_dumper(config).await;
            if let Err(e) = dumper.dump_info().await {
                println!("Failed to dump pool {} information, Error: {:?}", id, e);
                return Err(e);
            }
        }
        Resource::Nodes => {
            let node_client = NodeClientWrapper { rest_client };
            topologer = node_client.get_topologer(None).await?;
            config.topologer = Some(topologer);
            let mut dumper = ResourceDumper::get_or_panic_resource_dumper(config).await;
            if let Err(e) = dumper.dump_info().await {
                println!("Failed to dump nodes information, Error: {:?}", e);
                return Err(e);
            }
        }
        Resource::Node { id } => {
            let node_client = NodeClientWrapper { rest_client };
            topologer = node_client.get_topologer(Some(id.to_string())).await?;
            config.topologer = Some(topologer);
            let mut dumper = ResourceDumper::get_or_panic_resource_dumper(config).await;
            if let Err(e) = dumper.dump_info().await {
                println!("Failed to dump node {} information, Error: {:?}", id, e);
                return Err(e);
            }
        }
    };
    Ok(())
}
