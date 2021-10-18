pub mod node;
pub mod pool;
pub mod utils;
pub mod volume;

use structopt::StructOpt;

pub(crate) type VolumeId = openapi::apis::Uuid;
pub(crate) type ReplicaCount = u8;
pub(crate) type PoolId = String;
pub(crate) type NodeId = String;

/// The types of resources that support the 'get' operation.
#[derive(StructOpt, Debug)]
pub(crate) enum GetResources {
    /// Get all volumes.
    Volumes,
    /// Get volume with the given ID.
    Volume { id: VolumeId },
    /// Get the replica toplogy for the volume with the given ID
    VolumeReplicaTopology { id: VolumeId },
    /// Get all pools.
    Pools,
    /// Get pool with the given ID.
    Pool { id: PoolId },
    /// Get all nodes.
    Nodes,
    /// Get node with the given ID.
    Node { id: NodeId },
}

/// The types of resources that support the 'scale' operation.
#[derive(StructOpt, Debug)]
pub(crate) enum ScaleResources {
    /// Scale volume.
    Volume {
        /// ID of the volume.
        id: VolumeId,
        /// Replica count of the volume.
        replica_count: ReplicaCount,
    },
}

/// Tabular Output Tests
#[cfg(test)]
mod tests;
