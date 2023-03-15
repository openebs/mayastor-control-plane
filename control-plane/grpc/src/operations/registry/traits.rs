use crate::{
    context::Context,
    registry,
    registry::{GetSpecsRequest, GetStatesRequest},
};
use std::convert::TryFrom;
use stor_port::{
    transport_api::ReplyError,
    types::v0::{
        store,
        store::{
            nexus::NexusSpec,
            pool::PoolSpec,
            replica::ReplicaSpec,
            volume::{VolumeGroupSpec, VolumeSpec},
        },
        transport,
        transport::{GetSpecs, GetStates, Specs},
    },
};

/// Trait implemented by services which support registry operations.
#[tonic::async_trait]
pub trait RegistryOperations: Send + Sync {
    /// Get all resource specs
    async fn get_specs(
        &self,
        get_spec: &dyn GetSpecsInfo,
        ctx: Option<Context>,
    ) -> Result<transport::Specs, ReplyError>;
    /// Get the state information of all resources
    async fn get_states(
        &self,
        get_spec: &dyn GetStatesInfo,
        ctx: Option<Context>,
    ) -> Result<transport::States, ReplyError>;
}

/// GetSpecsInfo trait for the get_specs operation
pub trait GetSpecsInfo: Send + Sync {}

impl GetSpecsInfo for GetSpecs {}

impl GetSpecsInfo for GetSpecsRequest {}

/// GetStatesInfo trait for the get_states operation
pub trait GetStatesInfo: Send + Sync {}

impl GetStatesInfo for GetStates {}

impl GetStatesInfo for GetStatesRequest {}

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

impl From<&dyn GetStatesInfo> for GetStatesRequest {
    fn from(_: &dyn GetStatesInfo) -> Self {
        Self {}
    }
}

impl From<&dyn GetStatesInfo> for GetStates {
    fn from(_: &dyn GetStatesInfo) -> Self {
        Self {}
    }
}

impl TryFrom<registry::Specs> for transport::Specs {
    type Error = ReplyError;

    fn try_from(value: registry::Specs) -> Result<Self, Self::Error> {
        Ok(Self {
            volumes: value
                .volumes
                .into_iter()
                .map(VolumeSpec::try_from)
                .collect::<Result<Vec<VolumeSpec>, ReplyError>>()?,
            nexuses: value
                .nexuses
                .into_iter()
                .map(NexusSpec::try_from)
                .collect::<Result<Vec<NexusSpec>, ReplyError>>()?,
            pools: value
                .pools
                .into_iter()
                .map(PoolSpec::try_from)
                .collect::<Result<Vec<PoolSpec>, ReplyError>>()?,
            replicas: value
                .replicas
                .into_iter()
                .map(ReplicaSpec::try_from)
                .collect::<Result<Vec<ReplicaSpec>, ReplyError>>()?,
            volume_groups: value
                .volume_groups
                .into_iter()
                .map(VolumeGroupSpec::try_from)
                .collect::<Result<Vec<VolumeGroupSpec>, ReplyError>>()?,
        })
    }
}

impl From<transport::Specs> for registry::Specs {
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
            volume_groups: value
                .volume_groups
                .into_iter()
                .map(|vg_spec| vg_spec.into())
                .collect(),
        }
    }
}

impl TryFrom<registry::States> for transport::States {
    type Error = ReplyError;

    fn try_from(value: registry::States) -> Result<Self, Self::Error> {
        Ok(Self {
            nexuses: {
                let mut nexus_states: Vec<store::nexus::NexusState> = vec![];
                for nexus_state in value.nexuses {
                    nexus_states.push(store::nexus::NexusState {
                        nexus: transport::Nexus::try_from(nexus_state)?,
                    });
                }

                nexus_states
            },
            pools: {
                let mut pool_states: Vec<store::pool::PoolState> = vec![];
                for pool_state in value.pools {
                    pool_states.push(store::pool::PoolState {
                        pool: transport::PoolState::try_from(pool_state)?,
                    });
                }
                pool_states
            },
            replicas: {
                let mut replica_states: Vec<store::replica::ReplicaState> = vec![];
                for replica_state in value.replicas {
                    replica_states.push(store::replica::ReplicaState {
                        replica: transport::Replica::try_from(replica_state)?,
                    });
                }
                replica_states
            },
        })
    }
}

impl From<transport::States> for registry::States {
    fn from(value: transport::States) -> Self {
        Self {
            pools: value
                .pools
                .into_iter()
                .map(|pool_state| pool_state.pool.into())
                .collect(),
            nexuses: value
                .nexuses
                .into_iter()
                .map(|nexus_state| nexus_state.nexus.into())
                .collect(),
            replicas: value
                .replicas
                .into_iter()
                .map(|replica_state| replica_state.replica.into())
                .collect(),
        }
    }
}
