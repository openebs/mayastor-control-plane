use anyhow::Result;
use clap::Parser;
use openapi::tower::client::Url;
use opentelemetry::global;
use plugin::{
    operations::{Cordoning, Drain, Get, GetBlockDevices, List, ReplicaTopology, Scale},
    resources::{
        blockdevice, cordon, drain, node, pool, volume, CordonResources, DrainResources,
        GetCordonArgs, GetDrainArgs, GetResources, ScaleResources,
    },
    rest_wrapper::RestClient,
};
use std::{env, path::PathBuf};

mod resources;
use resources::Operations;

#[derive(Parser, Debug)]
#[clap(name = utils::package_description!(), version = utils::version_info_str!())]
struct CliArgs {
    /// The rest endpoint to connect to.
    #[clap(global = true, long, short)]
    rest: Option<Url>,

    /// Path to kubeconfig file.
    #[clap(parse(from_os_str), global = true, long, short = 'k')]
    kube_config_path: Option<PathBuf>,

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

    /// Kubernetes namespace of mayastor service, defaults to mayastor
    #[clap(global = true, long, short = 'n', default_value = "mayastor")]
    namespace: String,
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
    if let Err(e) = init_rest(&cli_args).await {
        println!("Failed to initialise the REST client. Error {}", e);
        std::process::exit(1);
    }

    // Perform the operations based on the subcommand, with proper output format.
    let fut = async move {
        match cli_args.operations {
            Operations::Get(resource) => match resource {
                GetResources::Cordon(get_cordon_resource) => match get_cordon_resource {
                    GetCordonArgs::Node { id: node_id } => {
                        cordon::NodeCordon::get(&node_id, &cli_args.output).await
                    }
                    GetCordonArgs::Nodes => cordon::NodeCordons::list(&cli_args.output).await,
                },
                GetResources::Drain(get_drain_resource) => match get_drain_resource {
                    GetDrainArgs::Node { id: node_id } => {
                        drain::NodeDrain::get(&node_id, &cli_args.output).await
                    }
                    GetDrainArgs::Nodes => drain::NodeDrains::list(&cli_args.output).await,
                },
                GetResources::Volumes => volume::Volumes::list(&cli_args.output).await,
                GetResources::Volume { id } => volume::Volume::get(&id, &cli_args.output).await,
                GetResources::VolumeReplicaTopology { id } => {
                    volume::Volume::topology(&id, &cli_args.output).await
                }
                GetResources::Pools => pool::Pools::list(&cli_args.output).await,
                GetResources::Pool { id } => pool::Pool::get(&id, &cli_args.output).await,
                GetResources::Nodes => node::Nodes::list(&cli_args.output).await,
                GetResources::Node(args) => {
                    node::Node::get(&args.node_id(), &cli_args.output).await
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
            Operations::Drain(resource) => match resource {
                DrainResources::Node(drain_node_args) => {
                    node::Node::drain(
                        &drain_node_args.node_id(),
                        drain_node_args.label(),
                        drain_node_args.drain_timeout(),
                        &cli_args.output,
                    )
                    .await
                }
            },
            Operations::Scale(resource) => match resource {
                ScaleResources::Volume { id, replica_count } => {
                    volume::Volume::scale(&id, replica_count, &cli_args.output).await
                }
            },
            Operations::Cordon(resource) => match resource {
                CordonResources::Node { id, label } => {
                    node::Node::cordon(&id, &label, &cli_args.output).await
                }
            },
            Operations::Uncordon(resource) => match resource {
                CordonResources::Node { id, label } => {
                    node::Node::uncordon(&id, &label, &cli_args.output).await
                }
            },
            Operations::Dump(resources) => {
                let _ignore = resources
                    .dump(cli_args.kube_config_path)
                    .await
                    .map_err(|_e| {
                        println!("Partially collected dump information !!");
                        std::process::exit(1);
                    });
                println!("Completed collection of dump !!");
            }
        };
    };

    tokio::select! {
        _shutdown = shutdown::Shutdown::wait_sig() => {},
        _done = fut => {}
    }
}

/// Initialise the REST client.
async fn init_rest(args: &CliArgs) -> Result<()> {
    // Use the supplied URL if there is one otherwise obtain one from the kubeconfig file.
    match args.rest.clone() {
        Some(url) => RestClient::init(url, *args.timeout),
        None => {
            let config = kube_proxy::ConfigBuilder::default_api_rest()
                .with_kube_config(args.kube_config_path.clone())
                .with_timeout(*args.timeout)
                .with_target_mod(|t| t.with_namespace(&args.namespace))
                .build()
                .await?;
            RestClient::init_with_config(config)?;
            Ok(())
        }
    }
}
