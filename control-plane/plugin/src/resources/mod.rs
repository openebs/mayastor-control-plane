use crate::resources::{
    blockdevice::BlockDeviceArgs,
    node::{DrainNodeArgs, GetNodeArgs, GetNodesArgs},
};

pub mod blockdevice;
pub mod node;
pub mod pool;
pub mod utils;
pub mod volume;

pub type VolumeId = openapi::apis::Uuid;
pub type ReplicaCount = u8;
pub type PoolId = String;
pub type NodeId = String;

/// The types of resources that support the 'get' operation.
#[derive(clap::Subcommand, Debug)]
pub enum GetResources {
    /// Get all volumes.
    Volumes,
    /// Get volume with the given ID.
    Volume { id: VolumeId },
    /// Get the replica topology for the volume with the given ID
    VolumeReplicaTopology { id: VolumeId },
    /// Get all pools.
    Pools,
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

/// The types of resources that support the 'drain' operation.
#[derive(clap::Subcommand, Debug)]
pub enum DrainResources {
    /// Drain node with the given ID.
    Node(DrainNodeArgs),
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

/// Tabular Output Tests
#[cfg(test)]
mod tests;
