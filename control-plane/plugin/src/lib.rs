#[macro_use]
extern crate prettytable;
#[macro_use]
extern crate lazy_static;

use crate::{
    operations::{
        Cordoning, Drain, Get, GetBlockDevices, GetSnapshots, List, ListExt, Operations,
        PluginResult, RebuildHistory, ReplicaTopology, Scale,
    },
    resources::{
        blockdevice, cordon, drain, node, pool, snapshot, volume, CordonResources, DrainResources,
        GetCordonArgs, GetDrainArgs, GetResources, ScaleResources,
    },
};

pub mod operations;
pub mod resources;
pub mod rest_wrapper;

/// Flush traces on `Drop`.
pub struct TracingFlusher {}
impl Drop for TracingFlusher {
    fn drop(&mut self) {
        utils::tracing_telemetry::flush_traces();
    }
}

#[derive(clap::Parser, Debug)]
#[clap(name = utils::package_description!(), version = utils::version_info_str!())]
pub struct RestCliArgs {
    /// The operation to be performed.
    #[clap(subcommand)]
    pub operation: Operations,

    /// The Output, viz yaml, json.
    #[clap(global = true, default_value = resources::utils::OutputFormat::None.as_ref(), short, long)]
    pub output: resources::utils::OutputFormat,

    /// Trace rest requests to the Jaeger endpoint agent.
    #[clap(global = true, long, short)]
    pub jaeger: Option<String>,

    /// Timeout for the REST operations.
    #[clap(long, short, default_value = "10s")]
    pub timeout: humantime::Duration,
}

impl RestCliArgs {
    /// Execute the operation specified in `Self::operations`, with proper output format.
    pub async fn execute(&self) -> PluginResult {
        match &self.operation {
            Operations::Drain(resource) => match resource {
                DrainResources::Node(drain_node_args) => {
                    node::Node::drain(
                        &drain_node_args.node_id(),
                        drain_node_args.label(),
                        drain_node_args.drain_timeout(),
                        &self.output,
                    )
                    .await
                }
            },
            Operations::Get(resource) => match resource {
                GetResources::Cordon(get_cordon_resource) => match get_cordon_resource {
                    GetCordonArgs::Node { id: node_id } => {
                        cordon::NodeCordon::get(node_id, &self.output).await
                    }
                    GetCordonArgs::Nodes => cordon::NodeCordons::list(&self.output).await,
                },
                GetResources::Drain(get_drain_resource) => match get_drain_resource {
                    GetDrainArgs::Node { id: node_id } => {
                        drain::NodeDrain::get(node_id, &self.output).await
                    }
                    GetDrainArgs::Nodes => drain::NodeDrains::list(&self.output).await,
                },
                GetResources::Volumes(vol_args) => {
                    volume::Volumes::list(&self.output, vol_args).await
                }
                GetResources::Volume { id } => volume::Volume::get(id, &self.output).await,
                GetResources::RebuildHistory { id } => {
                    volume::Volume::rebuild_history(id, &self.output).await
                }
                GetResources::VolumeReplicaTopologies(vol_args) => {
                    volume::Volume::topologies(&self.output, vol_args).await
                }
                GetResources::VolumeReplicaTopology { id } => {
                    volume::Volume::topology(id, &self.output).await
                }
                GetResources::Pools => pool::Pools::list(&self.output).await,
                GetResources::Pool { id } => pool::Pool::get(id, &self.output).await,
                GetResources::Nodes => node::Nodes::list(&self.output).await,
                GetResources::Node(args) => node::Node::get(&args.node_id(), &self.output).await,
                GetResources::BlockDevices(bdargs) => {
                    blockdevice::BlockDevice::get_blockdevices(
                        &bdargs.node_id(),
                        &bdargs.all(),
                        &self.output,
                    )
                    .await
                }
                GetResources::VolumeSnapshots(snapargs) => {
                    snapshot::VolumeSnapshots::get_snapshots(
                        &snapargs.volume(),
                        &snapargs.snapshot(),
                        &self.output,
                    )
                    .await
                }
            },
            Operations::Scale(resource) => match resource {
                ScaleResources::Volume { id, replica_count } => {
                    volume::Volume::scale(id, *replica_count, &self.output).await
                }
            },
            Operations::Cordon(resource) => match resource {
                CordonResources::Node { id, label } => {
                    node::Node::cordon(id, label, &self.output).await
                }
            },
            Operations::Uncordon(resource) => match resource {
                CordonResources::Node { id, label } => {
                    node::Node::uncordon(id, label, &self.output).await
                }
            },
        }
    }
    /// Initialize tracing (including opentelemetry).
    pub fn init_tracing(&self) -> TracingFlusher {
        let git_version = option_env!("GIT_VERSION").unwrap_or_else(utils::raw_version_str);
        let tags =
            utils::tracing_telemetry::default_tracing_tags(git_version, env!("CARGO_PKG_VERSION"));

        let fmt_layer = match std::env::var("RUST_LOG") {
            Ok(_) => utils::tracing_telemetry::FmtLayer::Stderr,
            Err(_) => utils::tracing_telemetry::FmtLayer::None,
        };

        utils::tracing_telemetry::init_tracing_ext(
            env!("CARGO_PKG_NAME"),
            tags,
            self.jaeger.as_ref(),
            fmt_layer,
            None,
        );

        TracingFlusher {}
    }
}
