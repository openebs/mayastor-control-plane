mod pool;
mod volume;

use crate::operations::{Get, List, Scale};
use structopt::StructOpt;

/// Trait to create a concrete instance of a resource.
pub trait ResourceFactory {
    type R;
    fn instance(&self) -> Self::R;
}

/// The types of resources that support the 'list' operation.
#[derive(StructOpt, Debug)]
pub(crate) enum ListResources {
    Volumes,
    Pools,
}

impl ResourceFactory for &ListResources {
    type R = Box<dyn List>;

    fn instance(&self) -> Self::R {
        match self {
            ListResources::Volumes => Box::new(volume::Volumes {}),
            ListResources::Pools => Box::new(pool::Pools {}),
        }
    }
}

pub(crate) type VolumeId = String;
pub(crate) type ReplicaCount = u8;
pub(crate) type PoolId = String;

/// The types of resources that support the 'get' operation.
#[derive(StructOpt, Debug)]
pub(crate) enum GetResources {
    Volume { id: VolumeId },
    Pool { id: PoolId },
}

impl ResourceFactory for &GetResources {
    type R = Box<dyn Get>;

    fn instance(&self) -> Self::R {
        match self {
            GetResources::Volume { id } => Box::new(volume::Volume::new(id, None)),
            GetResources::Pool { id } => Box::new(pool::Pool::new(id)),
        }
    }
}

/// The types of resources that support the 'scale' operation.
#[derive(StructOpt, Debug)]
pub(crate) enum ScaleResources {
    Volume {
        id: VolumeId,
        replica_count: ReplicaCount,
    },
}

impl ResourceFactory for &ScaleResources {
    type R = Box<dyn Scale>;

    fn instance(&self) -> Self::R {
        match self {
            ScaleResources::Volume { id, replica_count } => {
                Box::new(volume::Volume::new(id, Some(*replica_count)))
            }
        }
    }
}
