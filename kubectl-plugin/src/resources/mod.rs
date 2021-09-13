pub mod pool;
pub mod utils;
pub mod volume;

use structopt::StructOpt;

pub(crate) type VolumeId = String;
pub(crate) type ReplicaCount = u8;
pub(crate) type PoolId = String;

/// The types of resources that support the 'list' operation.
#[derive(StructOpt, Debug)]
pub(crate) enum GetResources {
    /// Get all volumes.
    Volumes,
    /// Get volume with the given ID.
    Volume { id: VolumeId },
    /// Get all pools.
    Pools,
    /// Get pool with the given ID.
    Pool { id: PoolId },
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
