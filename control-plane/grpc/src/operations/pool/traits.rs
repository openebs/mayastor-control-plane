use crate::{
    common,
    grpc_opts::Context,
    pool,
    pool::{get_pools_request, CreatePoolRequest, DestroyPoolRequest},
};
use common_lib::{
    mbus_api::{v0::Pools, ReplyError, ResourceKind},
    types::v0::{
        message_bus,
        message_bus::{
            CreatePool, DestroyPool, Filter, NodeId, Pool, PoolDeviceUri, PoolId, PoolState,
        },
        store::pool::{PoolLabel, PoolSpec, PoolSpecStatus},
    },
};
use std::convert::TryFrom;

/// Trait implemented by services which support pool operations.
#[tonic::async_trait]
pub trait PoolOperations: Send + Sync {
    /// Create a pool
    async fn create(
        &self,
        pool: &dyn CreatePoolInfo,
        ctx: Option<Context>,
    ) -> Result<Pool, ReplyError>;
    /// Destroy a pool
    async fn destroy(
        &self,
        pool: &dyn DestroyPoolInfo,
        ctx: Option<Context>,
    ) -> Result<(), ReplyError>;
    /// Get pools based on the filters
    async fn get(&self, filter: Filter, ctx: Option<Context>) -> Result<Pools, ReplyError>;
}

impl TryFrom<pool::Pool> for Pool {
    type Error = ReplyError;
    fn try_from(pool: pool::Pool) -> Result<Self, Self::Error> {
        let pool_state = match pool.state {
            None => None,
            Some(pool_state) => Some(PoolState {
                node: pool_state.node_id.into(),
                id: pool_state.pool_id.into(),
                disks: pool_state.disks_uri.iter().map(|i| i.into()).collect(),
                status: match pool::PoolStatus::from_i32(pool_state.status) {
                    Some(status) => status.into(),
                    None => {
                        return Err(ReplyError::invalid_argument(
                            ResourceKind::Pool,
                            "pool.state.status",
                            "".to_string(),
                        ))
                    }
                },
                capacity: pool_state.capacity,
                used: pool_state.used,
            }),
        };
        let pool_spec = match pool.definition {
            None => None,
            Some(pool_definition) => {
                let pool_spec = match pool_definition.spec {
                    Some(spec) => spec,
                    None => {
                        return Err(ReplyError::missing_argument(
                            ResourceKind::Pool,
                            "pool.definition.spec",
                        ))
                    }
                };
                let pool_meta = match pool_definition.metadata {
                    Some(meta) => meta,
                    None => {
                        return Err(ReplyError::missing_argument(
                            ResourceKind::Pool,
                            "pool.definition.metadata",
                        ))
                    }
                };
                let pool_spec_status = match common::SpecStatus::from_i32(pool_meta.status) {
                    Some(status) => match status {
                        common::SpecStatus::Created => match pool_state {
                            None => PoolSpecStatus::Created(message_bus::PoolStatus::Unknown),
                            Some(ref state) => PoolSpecStatus::Created(state.status.clone()),
                        },
                        _ => status.into(),
                    },
                    None => {
                        return Err(ReplyError::invalid_argument(
                            ResourceKind::Pool,
                            "pool.metadata.status",
                            "".to_string(),
                        ))
                    }
                };
                Some(PoolSpec {
                    node: pool_spec.node_id.into(),
                    id: pool_spec.pool_id.into(),
                    disks: pool_spec.disks.iter().map(|i| i.into()).collect(),
                    status: pool_spec_status,
                    labels: match pool_spec.labels {
                        Some(labels) => Some(labels.value),
                        None => None,
                    },
                    sequencer: Default::default(),
                    operation: None,
                })
            }
        };
        match Pool::try_new(pool_spec, pool_state) {
            Some(pool) => Ok(pool),
            None => Err(ReplyError::missing_argument(
                ResourceKind::Pool,
                "pool.spec and pool.state",
            )),
        }
    }
}

impl From<Pool> for pool::Pool {
    fn from(pool: Pool) -> Self {
        let pool_definition = match pool.spec() {
            None => None,
            Some(pool_spec) => {
                let status: common::SpecStatus = pool_spec.status.into();
                Some(pool::PoolDefinition {
                    spec: Some(pool::PoolSpec {
                        node_id: pool_spec.node.to_string(),
                        pool_id: pool_spec.id.to_string(),
                        disks: pool_spec.disks.iter().map(|i| i.to_string()).collect(),
                        labels: pool_spec
                            .labels
                            .map(|labels| crate::common::StringMapValue { value: labels }),
                    }),
                    metadata: Some(pool::Metadata {
                        uuid: None,
                        status: status as i32,
                    }),
                })
            }
        };
        let pool_state = match pool.state() {
            None => None,
            Some(pool_state) => Some(pool::PoolState {
                node_id: pool_state.node.to_string(),
                pool_id: pool_state.id.to_string(),
                disks_uri: pool_state.disks.iter().map(|i| i.to_string()).collect(),
                status: pool_state.status as i32,
                capacity: pool_state.capacity,
                used: pool_state.used,
            }),
        };
        pool::Pool {
            definition: pool_definition,
            state: pool_state,
        }
    }
}

impl TryFrom<pool::Pools> for Pools {
    type Error = ReplyError;
    fn try_from(grpc_pool_type: pool::Pools) -> Result<Self, Self::Error> {
        let mut pools: Vec<Pool> = vec![];
        for pool in grpc_pool_type.pools {
            pools.push(Pool::try_from(pool.clone())?)
        }
        Ok(Pools(pools))
    }
}

impl From<Pools> for pool::Pools {
    fn from(pools: Pools) -> Self {
        pool::Pools {
            pools: pools
                .into_inner()
                .iter()
                .map(|pool| pool.clone().into())
                .collect(),
        }
    }
}

impl From<get_pools_request::Filter> for Filter {
    fn from(filter: get_pools_request::Filter) -> Self {
        match filter {
            get_pools_request::Filter::Node(node_filter) => {
                Filter::Node(node_filter.node_id.into())
            }
            get_pools_request::Filter::NodePool(node_pool_filter) => Filter::NodePool(
                node_pool_filter.node_id.into(),
                node_pool_filter.pool_id.into(),
            ),
            get_pools_request::Filter::Pool(pool_filter) => {
                Filter::Pool(pool_filter.pool_id.into())
            }
        }
    }
}

/// CreatePoolInfo trait for the pool creation to be implemented by entities which want to avail
/// this operation
pub trait CreatePoolInfo: Send + Sync {
    /// Id of the pool
    fn pool_id(&self) -> PoolId;
    /// Id of the mayastor instance
    fn node_id(&self) -> NodeId;
    /// Disk device paths or URIs to be claimed by the pool
    fn disks(&self) -> Vec<PoolDeviceUri>;
    /// Labels to be set on the pool
    fn labels(&self) -> Option<PoolLabel>;
}

/// DestroyPoolInfo trait for the pool deletion to be implemented by entities which want to avail
/// this operation
pub trait DestroyPoolInfo: Sync + Send {
    /// Id of the pool
    fn pool_id(&self) -> PoolId;
    /// Id of the mayastor instance
    fn node_id(&self) -> NodeId;
}

impl CreatePoolInfo for CreatePool {
    fn pool_id(&self) -> PoolId {
        self.id.clone()
    }

    fn node_id(&self) -> NodeId {
        self.node.clone()
    }

    fn disks(&self) -> Vec<PoolDeviceUri> {
        self.disks.clone()
    }

    fn labels(&self) -> Option<PoolLabel> {
        self.labels.clone()
    }
}

impl CreatePoolInfo for CreatePoolRequest {
    fn pool_id(&self) -> PoolId {
        self.pool_id.clone().into()
    }

    fn node_id(&self) -> NodeId {
        self.node_id.clone().into()
    }

    fn disks(&self) -> Vec<PoolDeviceUri> {
        self.disks.iter().map(|disk| disk.into()).collect()
    }

    fn labels(&self) -> Option<PoolLabel> {
        match self.labels.clone() {
            None => None,
            Some(labels) => Some(labels.value),
        }
    }
}

impl From<&dyn CreatePoolInfo> for CreatePoolRequest {
    fn from(data: &dyn CreatePoolInfo) -> Self {
        Self {
            pool_id: data.pool_id().to_string(),
            node_id: data.node_id().to_string(),
            disks: data.disks().iter().map(|disk| disk.to_string()).collect(),
            labels: data
                .labels()
                .map(|labels| crate::common::StringMapValue { value: labels }),
        }
    }
}

impl From<&dyn CreatePoolInfo> for CreatePool {
    fn from(data: &dyn CreatePoolInfo) -> Self {
        Self {
            node: data.node_id(),
            id: data.pool_id(),
            disks: data.disks(),
            labels: data.labels(),
        }
    }
}

impl DestroyPoolInfo for DestroyPool {
    fn pool_id(&self) -> PoolId {
        self.id.clone()
    }

    fn node_id(&self) -> NodeId {
        self.node.clone()
    }
}

impl DestroyPoolInfo for DestroyPoolRequest {
    fn pool_id(&self) -> PoolId {
        self.pool_id.clone().into()
    }

    fn node_id(&self) -> NodeId {
        self.node_id.clone().into()
    }
}

impl From<&dyn DestroyPoolInfo> for DestroyPoolRequest {
    fn from(data: &dyn DestroyPoolInfo) -> Self {
        Self {
            pool_id: data.pool_id().to_string(),
            node_id: data.node_id().to_string(),
        }
    }
}

impl From<&dyn DestroyPoolInfo> for DestroyPool {
    fn from(data: &dyn DestroyPoolInfo) -> Self {
        Self {
            node: data.node_id(),
            id: data.pool_id(),
        }
    }
}

impl From<pool::PoolStatus> for message_bus::PoolStatus {
    fn from(src: pool::PoolStatus) -> Self {
        match src {
            pool::PoolStatus::Online => Self::Online,
            pool::PoolStatus::Degraded => Self::Degraded,
            pool::PoolStatus::Faulted => Self::Faulted,
            pool::PoolStatus::Unknown => Self::Unknown,
        }
    }
}

impl From<message_bus::PoolStatus> for pool::PoolStatus {
    fn from(pool_status: message_bus::PoolStatus) -> Self {
        match pool_status {
            message_bus::PoolStatus::Unknown => Self::Unknown,
            message_bus::PoolStatus::Online => Self::Online,
            message_bus::PoolStatus::Degraded => Self::Degraded,
            message_bus::PoolStatus::Faulted => Self::Faulted,
        }
    }
}

impl From<common::SpecStatus> for PoolSpecStatus {
    fn from(src: common::SpecStatus) -> Self {
        match src {
            common::SpecStatus::Created => Self::Created(Default::default()),
            common::SpecStatus::Creating => Self::Creating,
            common::SpecStatus::Deleted => Self::Deleted,
            common::SpecStatus::Deleting => Self::Deleting,
        }
    }
}

impl From<PoolSpecStatus> for common::SpecStatus {
    fn from(src: PoolSpecStatus) -> Self {
        match src {
            PoolSpecStatus::Creating => Self::Creating,
            PoolSpecStatus::Created(_) => Self::Created,
            PoolSpecStatus::Deleting => Self::Deleting,
            PoolSpecStatus::Deleted => Self::Deleted,
        }
    }
}
