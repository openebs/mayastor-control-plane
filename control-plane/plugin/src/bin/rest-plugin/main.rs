use clap::Parser;
use openapi::tower::client::Url;
use opentelemetry::global;
use plugin::{
    operations::{Cordoning, Get, GetBlockDevices, List, Operations, ReplicaTopology, Scale},
    resources::{blockdevice, node, pool, volume, CordonResources, GetResources, ScaleResources},
    rest_wrapper::RestClient,
};
use std::env;

#[derive(clap::Parser, Debug)]
#[clap(name = utils::package_description!(), version = utils::version_info_str!())]
struct CliArgs {
    /// The rest endpoint to connect to.
    #[clap(global = true, long, short, default_value = "http://localhost:8081")]
    rest: Url,

    /// The operation to be performed.
    #[clap(subcommand)]
    operations: Operations,

    /// The Output, viz yaml, json.
    #[clap(global = true, default_value = plugin::resources::utils::OutputFormat::None.as_ref(), short, long)]
    output: plugin::resources::utils::OutputFormat,

    /// Trace rest requests to the Jaeger endpoint agent.
    #[clap(global = true, long, short)]
    jaeger: Option<String>,

    /// Timeout for the REST operations.
    #[clap(long, short, default_value = "10s")]
    timeout: humantime::Duration,
}
impl CliArgs {
    fn args() -> Self {
        CliArgs::parse()
    }
}

#[tokio::main]
async fn main() {
    plugin::init_tracing(CliArgs::args().jaeger.as_ref());

    execute(CliArgs::args()).await;

    global::shutdown_tracer_provider();
}

async fn execute(cli_args: CliArgs) {
    // Initialise the REST client.
    if let Err(e) = RestClient::init(cli_args.rest.clone(), *cli_args.timeout) {
        println!("Failed to initialise the REST client. Error {}", e);
    }

    // Perform the operations based on the subcommand, with proper output format.
    match &cli_args.operations {
        Operations::Get(resource) => match resource {
            GetResources::Volumes => volume::Volumes::list(&cli_args.output).await,
            GetResources::Volume { id } => volume::Volume::get(id, &cli_args.output).await,
            GetResources::VolumeReplicaTopology { id } => {
                volume::Volume::topology(id, &cli_args.output).await
            }
            GetResources::Pools => pool::Pools::list(&cli_args.output).await,
            GetResources::Pool { id } => pool::Pool::get(id, &cli_args.output).await,
            GetResources::Nodes => node::Nodes::list(&cli_args.output).await,
            GetResources::Node(args) => {
                if args.show_cordon_labels() {
                    node::Node::get_labels(&args.node_id(), &cli_args.output).await
                } else {
                    node::Node::get(&args.node_id(), &cli_args.output).await
                }
            }
            GetResources::BlockDevices(bdargs) => {
                blockdevice::BlockDevice::get_blockdevices(
                    &bdargs.node_id(),
                    &bdargs.all(),
                    &cli_args.output,
                )
                .await
            }
        },
        Operations::Scale(resource) => match resource {
            ScaleResources::Volume { id, replica_count } => {
                volume::Volume::scale(id, *replica_count, &cli_args.output).await
            }
        },
        Operations::Cordon(resource) => match resource {
            CordonResources::Node { id, label } => {
                node::Node::cordon(id, label, &cli_args.output).await
            }
        },
        Operations::Uncordon(resource) => match resource {
            CordonResources::Node { id, label } => {
                node::Node::uncordon(id, label, &cli_args.output).await
            }
        },
    };
}
