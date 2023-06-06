use crate::resources::{
    blockdevice::BlockDeviceArgs,
    node::{DrainNodeArgs, GetNodeArgs},
    snapshot::VolumeSnapshotArgs,
};

pub mod blockdevice;
pub mod cordon;
pub mod drain;
pub mod node;
pub mod pool;
pub mod snapshot;
pub mod utils;
pub mod volume;

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
    Volumes,
    /// Get volume with the given ID.
    Volume { id: VolumeId },
    /// Get the replica topology for all volumes.
    VolumeReplicaTopologies,
    /// Get the replica topology for the volume with the given ID.
    VolumeReplicaTopology { id: VolumeId },
    /// Get volume snapshots based on input args.
    VolumeSnapshots(VolumeSnapshotArgs),
    /// Get all pools.
    Pools,
    /// Get pool with the given ID.
    Pool { id: PoolId },
    /// Get all nodes.
    Nodes,
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

/// The types of resources that support cordoning.
#[derive(clap::Subcommand, Debug)]
pub enum CordonResources {
    /// Cordon the node with the given ID by applying the cordon label to that node.
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

#[derive(clap::Subcommand, Debug)]
pub enum GetDrainArgs {
    /// Get the drain for the node with the given ID.
    Node {
        id: NodeId,
    },
    Nodes,
}

/// Tabular Output Tests
#[cfg(test)]
mod tests;
