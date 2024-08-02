use crate::resources::{
    blockdevice::BlockDeviceArgs,
    node::{DrainNodeArgs, GetNodeArgs, GetNodesArgs},
    pool::GetPoolsArgs,
    snapshot::VolumeSnapshotArgs,
    volume::VolumesArgs,
};

pub mod blockdevice;
pub mod cordon;
pub mod drain;
pub mod error;
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
    VolumeReplicaTopologies(VolumesArgs),
    /// Get the replica topology for the volume with the given ID.
    VolumeReplicaTopology { id: VolumeId },
    /// Get volume snapshots based on input args.
    VolumeSnapshots(VolumeSnapshotArgs),

    /// Get volume snapshot topology based on input args.
    VolumeSnapshotTopology(VolumeSnapshotArgs),
    /// Get all pools.
    Pools(GetPoolsArgs),
    /// Get pool with the given ID.
    Pool { id: PoolId },
    /// Get all nodes.
    Nodes(GetNodesArgs),
    /// Get node with the given ID.
    Node(GetNodeArgs),
    /// Get BlockDevices present on the Node. Lists usable devices by default.
    /// Currently disks having blobstore pools not created by control-plane are also shown as
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
}

/// The types of resources that support cordoning.
#[derive(clap::Subcommand, Debug)]
pub enum CordonResources {
    /// Cordon the node with the given ID by applying the cordon label to that node.
    Node { id: NodeId, label: String },
}

/// The types of resources that support uncordoning.
#[derive(clap::Subcommand, Debug)]
pub enum UnCordonResources {
    /// Removes the cordon label from the node.
    /// When the node has no more cordon labels, it is effectively uncordoned.
    Node { id: NodeId, label: String },
}

/// The types of resources that support the 'get cordon' operation.
#[derive(clap::Subcommand, Debug)]
pub enum GetCordonArgs {
    /// Get the cordon for the node with the given ID.
    Node {
        id: NodeId,
    },
    Nodes,
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

/// Tabular Output Tests.
#[cfg(test)]
mod tests;
