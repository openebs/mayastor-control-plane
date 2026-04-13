use crate::resources::{
    blockdevice::BlockDeviceArgs,
    node::{DrainNodeArgs, GetNodeArgs, GetNodesArgs},
    pool::{GetPoolArgs, GetPoolsArgs},
    snapshot::VolumeSnapshotArgs,
    volume::{VolumeTopologiesArgs, VolumeTopologyArgs, VolumesArgs},
};

pub mod blockdevice;
pub mod cordon;
pub mod drain;
pub mod error;
pub mod impact;
pub mod node;
pub mod pool;
pub mod snapshot;
pub mod utils;
pub mod volume;

pub use error::Error;

pub type VolumeId = openapi::apis::Uuid;
pub type SnapshotId = openapi::apis::Uuid;
pub type ReplicaCount = u8;
pub type PoolId = String;
pub type NodeId = String;

/// The types of resources that support the 'get' operation.
#[derive(clap::Subcommand, Debug)]
pub enum GetResources {
    /// get cordon
    #[clap(subcommand)]
    Cordon(GetCordonArgs),
    /// get drain
    #[clap(subcommand)]
    Drain(GetDrainArgs),
    /// Get all volumes.
    Volumes(VolumesArgs),
    /// Get volume with the given ID.
    Volume { id: VolumeId },
    /// Get Rebuild history for the volume with the given ID.
    RebuildHistory { id: VolumeId },
    /// Get the replica topology for all volumes.
    VolumeReplicaTopologies(VolumeTopologiesArgs),
    /// Get the replica topology for the volume with the given ID.
    VolumeReplicaTopology {
        id: VolumeId,
        #[clap(flatten)]
        args: VolumeTopologyArgs,
    },
    /// Get volume snapshots based on input args.
    VolumeSnapshots(VolumeSnapshotArgs),

    /// Get volume snapshot topology based on input args.
    VolumeSnapshotTopology(VolumeSnapshotArgs),
    /// Get all pools.
    Pools(GetPoolsArgs),
    /// Get pool with the given ID.
    Pool(GetPoolArgs),
    /// Get all nodes.
    Nodes(GetNodesArgs),
    /// Get node with the given ID.
    Node(GetNodeArgs),
    /// Get BlockDevices present on the Node. Lists usable devices by default.
    /// Currently, disks having blobstore pools not created by control-plane are also shown as
    /// usable.
    BlockDevices(BlockDeviceArgs),
}

/// The types of resources that support the 'scale' operation.
#[derive(clap::Subcommand, Debug)]
pub enum ScaleResources {
    /// Scale volume.
    Volume {
        /// ID of the volume.
        id: VolumeId,
        /// Replica count of the volume.
        replica_count: ReplicaCount,
    },
}

/// The types of resources that support the 'expand' operation.
#[derive(clap::Subcommand, Debug)]
pub enum ExpandResources {
    /// Expand a Pool to cover the entire span of the disk.
    /// Please refer maxExpansion parameter in the docs which controls maxExpandableSize of the pool.
    Pool {
        /// ID of the pool.
        id: PoolId,
    },
}

/// The types of resources that support the 'SetProperty' operation.
#[derive(clap::Subcommand, Debug)]
pub enum SetPropertyResources {
    /// Set volume property.
    Volume {
        /// ID of the volume.
        id: VolumeId,
        /// Volume properties.
        #[clap(subcommand)]
        properties: SetVolumeProperties,
    },
}
/// Various kinds of settable volume properties.
#[derive(clap::Subcommand, Debug, Clone)]
pub enum SetVolumeProperties {
    /// Max snapshot limit per volume.
    MaxSnapshots { max_snapshots: u32 },
    #[clap(
        about = "Encryption required for volume.\n\x1b[31m\x1b[1mCAUTION:\x1b[0m Use carefully during for example non-encrypted to encrypted pool migration. Refer to documentation for more details."
    )]
    Encryption {
        #[clap(action = clap::ArgAction::Set)]
        enabled: bool,
    },
}

/// The types of resources that support the 'Clear' operation.
#[derive(clap::Subcommand, Debug)]
pub enum ClearErrors {
    /// Clear errors from a pool.
    Pool {
        /// ID of the pool.
        id: PoolId,
        #[clap(flatten)]
        options: Option<pool::ClearErrorsRequest>,
    },
}

/// The types of resources that support cordoning.
#[derive(clap::Subcommand, Debug)]
pub enum CordonResources {
    /// Cordon the node with the given ID by applying the cordon label to that node.
    Node { id: NodeId, label: String },
    /// Cordon the pool with the given ID by applying the cordon constraints to that pool{n}
    /// By default only replicas and restores are constrained. Otherwise, you may individually
    /// select specific constraints.
    Pool {
        /// Id of the pool to cordon.
        id: PoolId,
        #[clap(flatten)]
        resources: Option<pool::CordonReq>,
    },
}

/// The types of resources that support uncordoning.
#[derive(clap::Subcommand, Debug)]
pub enum UnCordonResources {
    /// Removes the cordon label from the node.
    /// When the node has no more cordon labels, it is effectively uncordoned.
    Node { id: NodeId, label: String },
    /// Removes the cordon constraints from the pool.
    /// When the pool has no more cordon constraints, it is effectively uncordoned{n}
    /// By default all constraints are removed. Otherwise, you may individually select specific constraints' removal.
    Pool {
        /// Id of the pool to uncordon.
        id: PoolId,
        #[clap(flatten)]
        resources: Option<pool::UncordonReq>,
    },
}

/// The types of resources that support the 'get cordon' operation.
#[derive(clap::Subcommand, Debug)]
pub enum GetCordonArgs {
    /// Get the cordon for the node with the given ID.
    Node { id: NodeId },
    /// Get all nodes which are cordoned.
    Nodes,
    /// Get the cordon for the pool with the given ID.
    Pool { id: PoolId },
    /// Get all pools which are cordoned.
    Pools,
}

/// The types of resources that support the 'drain' operation.
#[derive(clap::Subcommand, Debug)]
pub enum DrainResources {
    /// Drain node with the given ID.
    Node(DrainNodeArgs),
}

/// The types of resources that support the 'label' operation.
#[derive(clap::Subcommand, Debug)]
pub enum LabelResources {
    /// Adds or removes a label to or from the specified node.
    Node {
        /// The id of the node to label/unlabel.
        id: NodeId,
        /// The label to be added or removed from the node.
        /// To add a label, please use the following format:
        /// ${key}=${value}
        /// To remove a label, please use the following format:
        /// ${key}-
        /// A label key and value must begin with a letter or number, and may contain letters,
        /// numbers, hyphens, dots, and underscores, up to 63 characters each.
        /// The key may contain a single slash.
        label: String,
        /// Allow labels to be overwritten, otherwise reject label updates that overwrite existing
        /// labels.
        #[clap(long)]
        overwrite: bool,
    },
    /// Adds or removes a label to or from the specified pool.
    Pool {
        /// The id of the pool to label/unlabel.
        id: PoolId,
        /// The label to be added or removed from the pool.
        /// To add a label, please use the following format:
        /// ${key}=${value}
        /// To remove a label, please use the following format:
        /// ${key}-
        /// A label key and value must begin with a letter or number, and may contain letters,
        /// numbers, hyphens, dots, and underscores, up to 63 characters each.
        /// The key may contain a single slash.
        label: String,
        /// Allow labels to be overwritten, otherwise reject label updates that overwrite existing
        /// labels.
        #[clap(long)]
        overwrite: bool,
    },
}

#[derive(clap::Subcommand, Debug)]
pub enum GetDrainArgs {
    /// Get the drain for the node with the given ID.
    Node {
        /// The id of the node to get the drain labels from.
        id: NodeId,
    },
    Nodes,
}

/// Delete resources.
#[derive(Debug, clap::Args)]
pub struct DeleteArgs {
    /// Ignore error if resource is not found.
    #[clap(long, short, global = true)]
    pub ignore_not_found: bool,

    /// Automatically confirm and assume yes for all prompts.
    #[clap(long, short, global = true)]
    pub yes: bool,

    #[clap(subcommand)]
    pub resource: DeleteResources,
}

/// The type of resources which support the delete operation.
#[derive(clap::Subcommand, Debug)]
pub enum DeleteResources {
    /// Deletes the specified pool resource.
    Pool(pool::DeletePoolArgs),
    /// Deletes the specified node and all its resources (purge).
    Node(node::DeleteNodeArgs),
    /// Deletes the specified volume resource.
    Volume {
        /// The id of the volume to delete.
        id: VolumeId,
    },
    /// Deletes the specified volume snapshot resource.
    VolumeSnapshot(snapshot::DelVolumeSnapshotArgs),
}

/// Prompt the user with the given question, and collect a y/n answer.
/// # NOTE: A newline must be added to ensure it's not a mistake.
pub fn prompt(question: &str, help: &str) -> Result<bool, Error> {
    inquire::Confirm::new(question)
        .with_default(false)
        .with_help_message(help)
        .prompt()
        .map_err(|source| Error::Dialogue { source })
}

/// Confirm the operation by prompting user with the question, and specify whether the answer has been given
/// automatically as part of cli args.
/// In case of negative answer, return an error.
pub fn confirm(question: &str, help: &str, yes: bool) -> Result<(), Error> {
    if yes {
        return Ok(());
    }
    if prompt(question, help)? {
        Ok(())
    } else {
        Err(Error::DialogueAbort {})
    }
}

/// Tabular Output Tests.
#[cfg(test)]
mod tests;
