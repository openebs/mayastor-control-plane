use crate::{context::Context, registry, registry::GetSpecsRequest};
use common_lib::{
    mbus_api::ReplyError,
    types::v0::{
        message_bus,
        message_bus::{GetSpecs, Specs},
        store::{nexus::NexusSpec, pool::PoolSpec, replica::ReplicaSpec, volume::VolumeSpec},
    },
};
use std::convert::TryFrom;

/// Trait implemented by services which support registry operations.
#[tonic::async_trait]
pub trait RegistryOperations: Send + Sync {
    /// Get all resource specs
    async fn get_specs(
        &self,
        get_spec: &dyn GetSpecsInfo,
        ctx: Option<Context>,
    ) -> Result<message_bus::Specs, ReplyError>;
}

/// GetSpecsInfo trait for the get_specs operation
pub trait GetSpecsInfo: Send + Sync {}

impl GetSpecsInfo for GetSpecs {}

impl GetSpecsInfo for GetSpecsRequest {}

impl From<&dyn GetSpecsInfo> for GetSpecsRequest {
    fn from(_: &dyn GetSpecsInfo) -> Self {
        Self {}
    }
}

impl From<&dyn GetSpecsInfo> for GetSpecs {
    fn from(_: &dyn GetSpecsInfo) -> Self {
        Self {}
    }
}

impl TryFrom<registry::Specs> for message_bus::Specs {
    type Error = ReplyError;

    fn try_from(value: registry::Specs) -> Result<Self, Self::Error> {
        Ok(Self {
            volumes: {
                let mut volume_specs: Vec<VolumeSpec> = vec![];
                for volume_definition in value.volumes {
                    volume_specs.push(VolumeSpec::try_from(volume_definition)?);
                }
                volume_specs
            },
            nexuses: {
                let mut nexus_specs: Vec<NexusSpec> = vec![];
                for nexus_spec in value.nexuses {
                    nexus_specs.push(NexusSpec::try_from(nexus_spec)?);
                }
                nexus_specs
            },
            pools: {
                let mut pool_specs: Vec<PoolSpec> = vec![];
                for pool_definition in value.pools {
                    pool_specs.push(PoolSpec::try_from(pool_definition)?);
                }
                pool_specs
            },
            replicas: {
                let mut replica_specs: Vec<ReplicaSpec> = vec![];
                for replica_spec in value.replicas {
                    replica_specs.push(ReplicaSpec::try_from(replica_spec)?);
                }
                replica_specs
            },
        })
    }
}

impl From<message_bus::Specs> for registry::Specs {
    fn from(value: Specs) -> Self {
        Self {
            volumes: value
                .volumes
                .into_iter()
                .map(|volume_spec| volume_spec.into())
                .collect(),
            pools: value
                .pools
                .into_iter()
                .map(|pool_spec| pool_spec.into())
                .collect(),
            nexuses: value
                .nexuses
                .into_iter()
                .map(|nexus_spec| nexus_spec.into())
                .collect(),
            replicas: value
                .replicas
                .into_iter()
                .map(|replica_spec| replica_spec.into())
                .collect(),
        }
    }
}
