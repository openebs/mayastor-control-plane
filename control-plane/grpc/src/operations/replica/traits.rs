use crate::{
    common,
    context::Context,
    misc::traits::{StringValue, ValidateRequestTypes},
    replica,
    replica::{
        get_replicas_request, CreateReplicaRequest, DestroyReplicaRequest, ShareReplicaRequest,
        UnshareReplicaRequest,
    },
};
use std::convert::TryFrom;
use stor_port::{
    transport_api::{v0::Replicas, ReplyError, ResourceKind},
    types::v0::{
        store::replica::{
            PoolRef, ReplicaOperation, ReplicaOperationState, ReplicaSpec, ReplicaSpecStatus,
        },
        transport,
        transport::{
            CreateReplica, DestroyReplica, Filter, HostNqn, NexusId, NodeId, PoolId, PoolUuid,
            Replica, ReplicaId, ReplicaKind, ReplicaName, ReplicaOwners, ReplicaSpaceUsage,
            ShareReplica, UnshareReplica, VolumeId,
        },
    },
    IntoOption, IntoVec,
};

/// All replica operations to be a part of the ReplicaOperations trait
#[tonic::async_trait]
pub trait ReplicaOperations: Send + Sync {
    /// Create a replica
    async fn create(
        &self,
        req: &dyn CreateReplicaInfo,
        ctx: Option<Context>,
    ) -> Result<Replica, ReplyError>;
    /// Get replicas based on filters
    async fn get(&self, filter: Filter, ctx: Option<Context>) -> Result<Replicas, ReplyError>;
    /// Destroy a replica
    async fn destroy(
        &self,
        req: &dyn DestroyReplicaInfo,
        ctx: Option<Context>,
    ) -> Result<(), ReplyError>;
    /// Share a replica
    async fn share(
        &self,
        req: &dyn ShareReplicaInfo,
        ctx: Option<Context>,
    ) -> Result<String, ReplyError>;
    /// Unshare a replica
    async fn unshare(
        &self,
        req: &dyn UnshareReplicaInfo,
        ctx: Option<Context>,
    ) -> Result<(), ReplyError>;
}

impl From<Replica> for replica::Replica {
    fn from(replica: Replica) -> Self {
        let share: common::Protocol = replica.share.into();
        let status: replica::ReplicaStatus = replica.status.into();
        replica::Replica {
            node_id: replica.node.into(),
            name: replica.name.into(),
            replica_id: Some(replica.uuid.into()),
            pool_id: replica.pool_id.into(),
            pool_uuid: replica.pool_uuid.into_opt(),
            thin: replica.thin,
            size: replica.size,
            share: share as i32,
            uri: replica.uri,
            status: status as i32,
            space: replica.space.into_opt(),
            kind: replica::ReplicaKind::from(replica.kind).into(),
        }
    }
}
impl From<ReplicaKind> for replica::ReplicaKind {
    fn from(value: ReplicaKind) -> Self {
        match value {
            ReplicaKind::Regular => Self::Regular,
            ReplicaKind::Snapshot => Self::Snapshot,
            ReplicaKind::SnapshotClone => Self::SnapshotClone,
        }
    }
}
impl From<replica::ReplicaKind> for ReplicaKind {
    fn from(value: replica::ReplicaKind) -> Self {
        match value {
            replica::ReplicaKind::Regular => Self::Regular,
            replica::ReplicaKind::Snapshot => Self::Snapshot,
            replica::ReplicaKind::SnapshotClone => Self::SnapshotClone,
        }
    }
}
impl From<ReplicaSpaceUsage> for replica::ReplicaSpaceUsage {
    fn from(space: ReplicaSpaceUsage) -> Self {
        Self {
            capacity_bytes: space.capacity_bytes,
            allocated_bytes: space.allocated_bytes,
            allocated_bytes_snapshots: space.allocated_bytes_snapshots,
            cluster_size: space.cluster_size,
            clusters: space.clusters,
            allocated_clusters: space.allocated_clusters,
            allocated_clusters_snapshots: space.allocated_clusters_snapshots,
        }
    }
}

impl TryFrom<replica::Replica> for Replica {
    type Error = ReplyError;
    fn try_from(replica: replica::Replica) -> Result<Self, Self::Error> {
        Ok(Replica {
            node: replica.node_id.into(),
            name: replica.name.into(),
            uuid: ReplicaId::try_from(StringValue(replica.replica_id))?,
            pool_id: replica.pool_id.into(),
            pool_uuid: match replica.pool_uuid {
                Some(uuid) => Some(match PoolUuid::try_from(uuid) {
                    Ok(uuid) => uuid,
                    Err(err) => {
                        return Err(ReplyError::invalid_argument(
                            ResourceKind::Replica,
                            "replica.pool_uuid",
                            err.to_string(),
                        ))
                    }
                }),
                None => None,
            },
            thin: replica.thin,
            size: replica.size,
            space: replica.space.into_opt(),
            share: match common::Protocol::from_i32(replica.share) {
                Some(share) => share.into(),
                None => {
                    return Err(ReplyError::invalid_argument(
                        ResourceKind::Replica,
                        "replica.share",
                        "".to_string(),
                    ))
                }
            },
            uri: replica.uri,
            status: match replica::ReplicaStatus::from_i32(replica.status) {
                Some(status) => status.into(),
                None => {
                    return Err(ReplyError::invalid_argument(
                        ResourceKind::Replica,
                        "replica.status",
                        "".to_string(),
                    ))
                }
            },
            allowed_hosts: vec![],
            kind: replica::ReplicaKind::from_i32(replica.kind)
                .unwrap_or_default()
                .into(),
        })
    }
}
impl From<replica::ReplicaSpaceUsage> for ReplicaSpaceUsage {
    fn from(space: replica::ReplicaSpaceUsage) -> Self {
        Self {
            capacity_bytes: space.capacity_bytes,
            allocated_bytes: space.allocated_bytes,
            allocated_bytes_snapshots: space.allocated_bytes_snapshots,
            cluster_size: space.cluster_size,
            clusters: space.clusters,
            allocated_clusters: space.allocated_clusters,
            allocated_clusters_snapshots: space.allocated_clusters_snapshots,
        }
    }
}

impl TryFrom<get_replicas_request::Filter> for Filter {
    type Error = ReplyError;
    fn try_from(filter: get_replicas_request::Filter) -> Result<Self, Self::Error> {
        match filter {
            get_replicas_request::Filter::Node(node_filter) => {
                Ok(Filter::Node(node_filter.node_id.into()))
            }
            get_replicas_request::Filter::NodePool(node_pool_filter) => Ok(Filter::NodePool(
                node_pool_filter.node_id.into(),
                node_pool_filter.pool_id.into(),
            )),
            get_replicas_request::Filter::Pool(pool_filter) => {
                Ok(Filter::Pool(pool_filter.pool_id.into()))
            }
            get_replicas_request::Filter::NodePoolReplica(node_pool_replica_filter) => {
                Ok(Filter::NodePoolReplica(
                    node_pool_replica_filter.node_id.into(),
                    node_pool_replica_filter.pool_id.into(),
                    ReplicaId::try_from(StringValue(Some(node_pool_replica_filter.replica_id)))?,
                ))
            }
            get_replicas_request::Filter::NodeReplica(node_replica_filter) => {
                Ok(Filter::NodeReplica(
                    node_replica_filter.node_id.into(),
                    match ReplicaId::try_from(node_replica_filter.replica_id) {
                        Ok(replica_id) => replica_id,
                        Err(err) => {
                            return Err(ReplyError::invalid_argument(
                                ResourceKind::Replica,
                                "replica_filter::node_replica.replica_id",
                                err.to_string(),
                            ))
                        }
                    },
                ))
            }
            get_replicas_request::Filter::PoolReplica(pool_replica_filter) => {
                Ok(Filter::PoolReplica(
                    pool_replica_filter.pool_id.into(),
                    match ReplicaId::try_from(pool_replica_filter.replica_id) {
                        Ok(replica_id) => replica_id,
                        Err(err) => {
                            return Err(ReplyError::invalid_argument(
                                ResourceKind::Replica,
                                "replica_filter::pool_replica.replica_id",
                                err.to_string(),
                            ))
                        }
                    },
                ))
            }
            get_replicas_request::Filter::Replica(replica_filter) => Ok(Filter::Replica(
                match ReplicaId::try_from(replica_filter.replica_id) {
                    Ok(replica_id) => replica_id,
                    Err(err) => {
                        return Err(ReplyError::invalid_argument(
                            ResourceKind::Replica,
                            "replica_filter::replica.replica_id",
                            err.to_string(),
                        ))
                    }
                },
            )),
            get_replicas_request::Filter::Volume(volume_filter) => Ok(Filter::Volume(
                match VolumeId::try_from(volume_filter.volume_id) {
                    Ok(volume_id) => volume_id,
                    Err(err) => {
                        return Err(ReplyError::invalid_argument(
                            ResourceKind::Replica,
                            "replica_filter::volume.volume_id",
                            err.to_string(),
                        ))
                    }
                },
            )),
        }
    }
}

impl TryFrom<replica::Replicas> for Replicas {
    type Error = ReplyError;
    fn try_from(grpc_replicas_type: replica::Replicas) -> Result<Self, Self::Error> {
        let mut replicas: Vec<Replica> = vec![];
        for replica in grpc_replicas_type.replicas {
            replicas.push(Replica::try_from(replica.clone())?)
        }
        Ok(Replicas(replicas))
    }
}

impl From<Replicas> for replica::Replicas {
    fn from(replicas: Replicas) -> Self {
        replica::Replicas {
            replicas: replicas
                .into_inner()
                .iter()
                .map(|replicas| replicas.clone().into())
                .collect(),
        }
    }
}

/// CreateReplicaInfo trait for the replica creation to be implemented by entities which want to
/// avail this operation
pub trait CreateReplicaInfo: Send + Sync + std::fmt::Debug {
    /// Id of the IoEngine instance
    fn node(&self) -> NodeId;
    /// Name of the replica
    fn name(&self) -> Option<ReplicaName>;
    /// Uuid of the replica
    fn uuid(&self) -> ReplicaId;
    /// Id of the pool
    fn pool_id(&self) -> PoolId;
    /// Uuid of the pool
    fn pool_uuid(&self) -> Option<PoolUuid>;
    /// Size of the replica in bytes
    fn size(&self) -> u64;
    /// Thin provisioning
    fn thin(&self) -> bool;
    /// Protocol to expose the replica over
    fn share(&self) -> transport::Protocol;
    /// Managed by our control plane
    fn managed(&self) -> bool;
    /// Owners of the resource
    fn owners(&self) -> ReplicaOwners;
    /// List of host nqn allowed to connect to the target.
    fn allowed_hosts(&self) -> Vec<HostNqn>;
}

impl CreateReplicaInfo for CreateReplica {
    fn node(&self) -> NodeId {
        self.node.clone()
    }

    fn name(&self) -> Option<ReplicaName> {
        self.name.clone()
    }

    fn uuid(&self) -> ReplicaId {
        self.uuid.clone()
    }

    fn pool_id(&self) -> PoolId {
        self.pool_id.clone()
    }

    fn pool_uuid(&self) -> Option<PoolUuid> {
        self.pool_uuid.clone()
    }

    fn size(&self) -> u64 {
        self.size
    }

    fn thin(&self) -> bool {
        self.thin
    }

    fn share(&self) -> transport::Protocol {
        self.share
    }

    fn managed(&self) -> bool {
        self.managed
    }

    fn owners(&self) -> ReplicaOwners {
        self.owners.clone()
    }

    fn allowed_hosts(&self) -> Vec<HostNqn> {
        self.allowed_hosts.clone()
    }
}

/// Intermediate structure that validates the conversion to CreateVolumeRequest type
#[derive(Debug)]
pub struct ValidatedCreateReplicaRequest {
    inner: CreateReplicaRequest,
    uuid: ReplicaId,
    share: transport::Protocol,
    owners: ReplicaOwners,
    pool_uuid: Option<PoolUuid>,
    allowed_hosts: Vec<HostNqn>,
}

impl CreateReplicaInfo for ValidatedCreateReplicaRequest {
    fn node(&self) -> NodeId {
        self.inner.node_id.clone().into()
    }

    fn name(&self) -> Option<ReplicaName> {
        self.inner.name.clone().map(|e| e.into())
    }

    fn uuid(&self) -> ReplicaId {
        self.uuid.clone()
    }

    fn pool_id(&self) -> PoolId {
        self.inner.pool_id.clone().into()
    }

    fn pool_uuid(&self) -> Option<PoolUuid> {
        self.pool_uuid.clone()
    }

    fn size(&self) -> u64 {
        self.inner.size
    }

    fn thin(&self) -> bool {
        self.inner.thin
    }

    fn share(&self) -> transport::Protocol {
        self.share
    }

    fn managed(&self) -> bool {
        self.inner.managed
    }

    fn owners(&self) -> ReplicaOwners {
        self.owners.clone()
    }

    fn allowed_hosts(&self) -> Vec<HostNqn> {
        self.allowed_hosts.clone()
    }
}

impl ValidateRequestTypes for CreateReplicaRequest {
    type Validated = ValidatedCreateReplicaRequest;
    fn validated(self) -> Result<Self::Validated, ReplyError> {
        Ok(ValidatedCreateReplicaRequest {
            uuid: ReplicaId::try_from(StringValue(self.replica_id.clone()))?,
            share: match common::Protocol::from_i32(self.share) {
                Some(share) => share.into(),
                None => {
                    return Err(ReplyError::invalid_argument(
                        ResourceKind::Replica,
                        "create_replica_request.share",
                        "".to_string(),
                    ))
                }
            },
            owners: match self.owners.clone() {
                Some(owners) => ReplicaOwners::try_from(owners)?,
                None => {
                    return Err(ReplyError::missing_argument(
                        ResourceKind::ReplicaSpec,
                        "replica_spec.owners",
                    ))
                }
            },
            pool_uuid: match self.pool_uuid.clone() {
                Some(uuid) => Some(match PoolUuid::try_from(uuid) {
                    Ok(uuid) => uuid,
                    Err(err) => {
                        return Err(ReplyError::invalid_argument(
                            ResourceKind::Replica,
                            "replica.pool_uuid",
                            err.to_string(),
                        ))
                    }
                }),
                None => None,
            },
            allowed_hosts: self
                .allowed_hosts
                .iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            inner: self,
        })
    }
}

/// DestroyReplicaInfo trait for the replica deletion to be implemented by entities which want to
/// avail this operation
pub trait DestroyReplicaInfo: Send + Sync + std::fmt::Debug {
    /// Id of the IoEngine instance
    fn node(&self) -> NodeId;
    /// Id of the pool
    fn pool_id(&self) -> PoolId;
    /// Uuid of the pool
    fn pool_uuid(&self) -> Option<PoolUuid>;
    /// Name of the replica
    fn name(&self) -> Option<ReplicaName>;
    /// Uuid of the replica
    fn uuid(&self) -> ReplicaId;
    /// Delete by owners
    fn disowners(&self) -> ReplicaOwners;
}

impl DestroyReplicaInfo for DestroyReplica {
    fn node(&self) -> NodeId {
        self.node.clone()
    }

    fn pool_id(&self) -> PoolId {
        self.pool_id.clone()
    }

    fn pool_uuid(&self) -> Option<PoolUuid> {
        self.pool_uuid.clone()
    }

    fn name(&self) -> Option<ReplicaName> {
        self.name.clone()
    }

    fn uuid(&self) -> ReplicaId {
        self.uuid.clone()
    }

    fn disowners(&self) -> ReplicaOwners {
        self.disowners.clone()
    }
}

/// Intermediate structure that validates the conversion to DestroyVolumeRequest type
#[derive(Debug)]
pub struct ValidatedDestroyReplicaRequest {
    inner: DestroyReplicaRequest,
    uuid: ReplicaId,
    disowners: ReplicaOwners,
    pool_uuid: Option<PoolUuid>,
}

impl DestroyReplicaInfo for ValidatedDestroyReplicaRequest {
    fn node(&self) -> NodeId {
        self.inner.node_id.clone().into()
    }

    fn pool_id(&self) -> PoolId {
        self.inner.pool_id.clone().into()
    }

    fn pool_uuid(&self) -> Option<PoolUuid> {
        self.pool_uuid.clone()
    }

    fn name(&self) -> Option<ReplicaName> {
        self.inner.name.clone().map(|e| e.into())
    }

    fn uuid(&self) -> ReplicaId {
        self.uuid.clone()
    }

    fn disowners(&self) -> ReplicaOwners {
        self.disowners.clone()
    }
}

impl ValidateRequestTypes for DestroyReplicaRequest {
    type Validated = ValidatedDestroyReplicaRequest;
    fn validated(self) -> Result<Self::Validated, ReplyError> {
        Ok(ValidatedDestroyReplicaRequest {
            uuid: ReplicaId::try_from(StringValue(self.replica_id.clone()))?,
            disowners: match self.disowners.clone() {
                Some(disowners) => ReplicaOwners::try_from(disowners)?,
                None => {
                    return Err(ReplyError::missing_argument(
                        ResourceKind::Replica,
                        "replica_spec.disowners",
                    ))
                }
            },
            pool_uuid: match self.pool_uuid.clone() {
                Some(uuid) => Some(match PoolUuid::try_from(uuid) {
                    Ok(uuid) => uuid,
                    Err(err) => {
                        return Err(ReplyError::invalid_argument(
                            ResourceKind::Replica,
                            "replica.pool_uuid",
                            err.to_string(),
                        ))
                    }
                }),
                None => None,
            },
            inner: self,
        })
    }
}

/// ShareReplicaInfo trait for the replica sharing to be implemented by entities which want to avail
/// this operation
pub trait ShareReplicaInfo: Send + Sync + std::fmt::Debug {
    /// Id of the IoEngine instance
    fn node(&self) -> NodeId;
    /// Id of the pool
    fn pool_id(&self) -> PoolId;
    /// Uuid of the pool
    fn pool_uuid(&self) -> Option<PoolUuid>;
    /// Name of the replica,
    fn name(&self) -> Option<ReplicaName>;
    /// Uuid of the replica
    fn uuid(&self) -> ReplicaId;
    /// Protocol used for exposing the replica
    fn protocol(&self) -> transport::ReplicaShareProtocol;
    /// List of host nqn allowed to connect to the target.
    fn allowed_hosts(&self) -> Vec<HostNqn>;
}

impl ShareReplicaInfo for ShareReplica {
    fn node(&self) -> NodeId {
        self.node.clone()
    }

    fn pool_id(&self) -> PoolId {
        self.pool_id.clone()
    }

    fn pool_uuid(&self) -> Option<PoolUuid> {
        self.pool_uuid.clone()
    }

    fn name(&self) -> Option<ReplicaName> {
        self.name.clone()
    }

    fn uuid(&self) -> ReplicaId {
        self.uuid.clone()
    }

    fn protocol(&self) -> transport::ReplicaShareProtocol {
        self.protocol
    }

    fn allowed_hosts(&self) -> Vec<HostNqn> {
        self.allowed_hosts.clone()
    }
}

/// Intermediate structure that validates the conversion to ShareVolumeRequest type.
#[derive(Debug)]
pub struct ValidatedShareReplicaRequest {
    inner: ShareReplicaRequest,
    uuid: ReplicaId,
    protocol: transport::ReplicaShareProtocol,
    pool_uuid: Option<PoolUuid>,
    allowed_hosts: Vec<HostNqn>,
}

impl ShareReplicaInfo for ValidatedShareReplicaRequest {
    fn node(&self) -> NodeId {
        self.inner.node_id.clone().into()
    }

    fn pool_id(&self) -> PoolId {
        self.inner.pool_id.clone().into()
    }

    fn pool_uuid(&self) -> Option<PoolUuid> {
        self.pool_uuid.clone()
    }

    fn name(&self) -> Option<ReplicaName> {
        self.inner.name.clone().map(|e| e.into())
    }

    fn uuid(&self) -> ReplicaId {
        self.uuid.clone()
    }

    fn protocol(&self) -> transport::ReplicaShareProtocol {
        self.protocol
    }

    fn allowed_hosts(&self) -> Vec<HostNqn> {
        self.allowed_hosts.clone()
    }
}

impl ValidateRequestTypes for ShareReplicaRequest {
    type Validated = ValidatedShareReplicaRequest;
    fn validated(self) -> Result<Self::Validated, ReplyError> {
        Ok(ValidatedShareReplicaRequest {
            uuid: ReplicaId::try_from(StringValue(self.replica_id.clone()))?,
            protocol: match replica::ReplicaShareProtocol::from_i32(self.protocol) {
                Some(protocol) => protocol.into(),
                None => {
                    return Err(ReplyError::invalid_argument(
                        ResourceKind::Replica,
                        "share_replica_request.protocol",
                        "".to_string(),
                    ))
                }
            },
            pool_uuid: match self.pool_uuid.clone() {
                Some(uuid) => Some(match PoolUuid::try_from(uuid) {
                    Ok(uuid) => uuid,
                    Err(err) => {
                        return Err(ReplyError::invalid_argument(
                            ResourceKind::Replica,
                            "replica.pool_uuid",
                            err.to_string(),
                        ))
                    }
                }),
                None => None,
            },
            allowed_hosts: self
                .allowed_hosts
                .iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            inner: self,
        })
    }
}

/// UnshareReplicaInfo trait for the replica sharing to be implemented by entities which want to
/// avail this operation
pub trait UnshareReplicaInfo: Send + Sync + std::fmt::Debug {
    /// Id of the IoEngine instance
    fn node(&self) -> NodeId;
    /// Id of the pool
    fn pool_id(&self) -> PoolId;
    /// Uuid of the pool
    fn pool_uuid(&self) -> Option<PoolUuid>;
    /// Name of the replica
    fn name(&self) -> Option<ReplicaName>;
    /// Uuid of the replica
    fn uuid(&self) -> ReplicaId;
}

impl UnshareReplicaInfo for UnshareReplica {
    fn node(&self) -> NodeId {
        self.node.clone()
    }

    fn pool_id(&self) -> PoolId {
        self.pool_id.clone()
    }

    fn pool_uuid(&self) -> Option<PoolUuid> {
        self.pool_uuid.clone()
    }

    fn name(&self) -> Option<ReplicaName> {
        self.name.clone()
    }

    fn uuid(&self) -> ReplicaId {
        self.uuid.clone()
    }
}

/// Intermediate structure that validates the conversion to ShareVolumeRequest type
#[derive(Debug)]
pub struct ValidatedUnshareReplicaRequest {
    inner: UnshareReplicaRequest,
    uuid: ReplicaId,
    pool_uuid: Option<PoolUuid>,
}

impl UnshareReplicaInfo for ValidatedUnshareReplicaRequest {
    fn node(&self) -> NodeId {
        self.inner.node_id.clone().into()
    }

    fn pool_id(&self) -> PoolId {
        self.inner.pool_id.clone().into()
    }

    fn pool_uuid(&self) -> Option<PoolUuid> {
        self.pool_uuid.clone()
    }

    fn name(&self) -> Option<ReplicaName> {
        self.inner.name.clone().map(|e| e.into())
    }

    fn uuid(&self) -> ReplicaId {
        self.uuid.clone()
    }
}

impl ValidateRequestTypes for UnshareReplicaRequest {
    type Validated = ValidatedUnshareReplicaRequest;
    fn validated(self) -> Result<Self::Validated, ReplyError> {
        Ok(ValidatedUnshareReplicaRequest {
            uuid: ReplicaId::try_from(StringValue(self.replica_id.clone()))?,
            pool_uuid: match self.pool_uuid.clone() {
                Some(uuid) => Some(match PoolUuid::try_from(uuid) {
                    Ok(uuid) => uuid,
                    Err(err) => {
                        return Err(ReplyError::invalid_argument(
                            ResourceKind::Replica,
                            "replica.pool_uuid",
                            err.to_string(),
                        ))
                    }
                }),
                None => None,
            },
            inner: self,
        })
    }
}

impl From<&dyn CreateReplicaInfo> for CreateReplicaRequest {
    fn from(data: &dyn CreateReplicaInfo) -> Self {
        let share: common::Protocol = data.share().into();
        Self {
            node_id: data.node().to_string(),
            pool_id: data.pool_id().to_string(),
            pool_uuid: data.pool_uuid().map(|uuid| uuid.into()),
            name: data.name().map(|name| name.to_string()),
            replica_id: Some(data.uuid().to_string()),
            thin: data.thin(),
            size: data.size(),
            share: share as i32,
            managed: data.managed(),
            owners: Some(data.owners().into()),
            allowed_hosts: data.allowed_hosts().into_vec(),
        }
    }
}

impl From<&dyn CreateReplicaInfo> for CreateReplica {
    fn from(data: &dyn CreateReplicaInfo) -> Self {
        Self {
            node: data.node(),
            name: data.name(),
            uuid: data.uuid(),
            pool_id: data.pool_id(),
            pool_uuid: data.pool_uuid(),
            size: data.size(),
            thin: data.thin(),
            share: data.share(),
            managed: data.managed(),
            owners: data.owners(),
            allowed_hosts: data.allowed_hosts(),
        }
    }
}

impl From<&dyn DestroyReplicaInfo> for DestroyReplicaRequest {
    fn from(data: &dyn DestroyReplicaInfo) -> Self {
        Self {
            node_id: data.node().to_string(),
            pool_id: data.pool_id().to_string(),
            pool_uuid: data.pool_uuid().map(|uuid| uuid.into()),
            name: data.name().map(|name| name.to_string()),
            replica_id: Some(data.uuid().to_string()),
            disowners: Some(data.disowners().into()),
        }
    }
}

impl From<&dyn DestroyReplicaInfo> for DestroyReplica {
    fn from(data: &dyn DestroyReplicaInfo) -> Self {
        Self {
            node: data.node(),
            pool_id: data.pool_id(),
            pool_uuid: data.pool_uuid(),
            uuid: data.uuid(),
            name: data.name(),
            disowners: data.disowners(),
        }
    }
}

impl From<&dyn ShareReplicaInfo> for ShareReplicaRequest {
    fn from(data: &dyn ShareReplicaInfo) -> Self {
        let protocol: replica::ReplicaShareProtocol = data.protocol().into();
        Self {
            node_id: data.node().to_string(),
            pool_id: data.pool_id().to_string(),
            pool_uuid: data.pool_uuid().map(|uuid| uuid.into()),
            name: data.name().map(|name| name.to_string()),
            replica_id: Some(data.uuid().to_string()),
            protocol: protocol as i32,
            allowed_hosts: data.allowed_hosts().into_vec(),
        }
    }
}

impl From<&dyn ShareReplicaInfo> for ShareReplica {
    fn from(data: &dyn ShareReplicaInfo) -> Self {
        Self {
            node: data.node(),
            pool_id: data.pool_id(),
            pool_uuid: data.pool_uuid(),
            uuid: data.uuid(),
            name: data.name(),
            protocol: data.protocol(),
            allowed_hosts: data.allowed_hosts(),
        }
    }
}

impl From<&dyn UnshareReplicaInfo> for UnshareReplicaRequest {
    fn from(data: &dyn UnshareReplicaInfo) -> Self {
        Self {
            node_id: data.node().to_string(),
            pool_id: data.pool_id().to_string(),
            pool_uuid: data.pool_uuid().map(|uuid| uuid.into()),
            name: data.name().map(|name| name.to_string()),
            replica_id: Some(data.uuid().to_string()),
        }
    }
}

impl From<&dyn UnshareReplicaInfo> for UnshareReplica {
    fn from(data: &dyn UnshareReplicaInfo) -> Self {
        Self {
            node: data.node(),
            pool_id: data.pool_id(),
            pool_uuid: data.pool_uuid(),
            uuid: data.uuid(),
            name: data.name(),
        }
    }
}

impl From<common::Protocol> for transport::Protocol {
    fn from(src: common::Protocol) -> Self {
        match src {
            common::Protocol::None => Self::None,
            common::Protocol::Nvmf => Self::Nvmf,
            common::Protocol::Iscsi => Self::Iscsi,
            common::Protocol::Nbd => Self::Nbd,
        }
    }
}

impl From<transport::Protocol> for common::Protocol {
    fn from(src: transport::Protocol) -> Self {
        match src {
            transport::Protocol::None => Self::None,
            transport::Protocol::Nvmf => Self::Nvmf,
            transport::Protocol::Iscsi => Self::Iscsi,
            transport::Protocol::Nbd => Self::Nbd,
        }
    }
}

impl From<replica::ReplicaStatus> for transport::ReplicaStatus {
    fn from(src: replica::ReplicaStatus) -> Self {
        match src {
            replica::ReplicaStatus::Unknown => Self::Unknown,
            replica::ReplicaStatus::Online => Self::Online,
            replica::ReplicaStatus::Degraded => Self::Degraded,
            replica::ReplicaStatus::Faulted => Self::Faulted,
        }
    }
}

impl From<transport::ReplicaStatus> for replica::ReplicaStatus {
    fn from(src: transport::ReplicaStatus) -> Self {
        match src {
            transport::ReplicaStatus::Unknown => Self::Unknown,
            transport::ReplicaStatus::Online => Self::Online,
            transport::ReplicaStatus::Degraded => Self::Degraded,
            transport::ReplicaStatus::Faulted => Self::Faulted,
        }
    }
}

impl From<replica::ReplicaShareProtocol> for transport::ReplicaShareProtocol {
    fn from(src: replica::ReplicaShareProtocol) -> Self {
        match src {
            replica::ReplicaShareProtocol::NvmfProtocol => Self::Nvmf,
        }
    }
}

impl From<transport::ReplicaShareProtocol> for replica::ReplicaShareProtocol {
    fn from(src: transport::ReplicaShareProtocol) -> Self {
        match src {
            transport::ReplicaShareProtocol::Nvmf => Self::NvmfProtocol,
        }
    }
}

impl From<common::SpecStatus> for ReplicaSpecStatus {
    fn from(src: common::SpecStatus) -> Self {
        match src {
            common::SpecStatus::Created => Self::Created(Default::default()),
            common::SpecStatus::Creating => Self::Creating,
            common::SpecStatus::Deleted => Self::Deleted,
            common::SpecStatus::Deleting => Self::Deleting,
        }
    }
}

impl From<ReplicaSpecStatus> for common::SpecStatus {
    fn from(src: ReplicaSpecStatus) -> Self {
        match src {
            ReplicaSpecStatus::Creating => Self::Creating,
            ReplicaSpecStatus::Created(_) => Self::Created,
            ReplicaSpecStatus::Deleting => Self::Deleting,
            ReplicaSpecStatus::Deleted => Self::Deleted,
        }
    }
}

impl TryFrom<replica::ReplicaOwners> for ReplicaOwners {
    type Error = ReplyError;

    fn try_from(value: replica::ReplicaOwners) -> Result<Self, Self::Error> {
        Ok(ReplicaOwners::new(
            match value.volume.clone() {
                Some(volume) => match VolumeId::try_from(volume) {
                    Ok(volumeid) => Some(volumeid),
                    Err(err) => {
                        return Err(ReplyError::invalid_argument(
                            ResourceKind::ReplicaSpec,
                            "replica_spec.owners.volume",
                            err.to_string(),
                        ))
                    }
                },
                None => None,
            },
            {
                let mut nexuses: Vec<NexusId> = vec![];
                for nexus in value.nexuses {
                    let nexusid = NexusId::try_from(StringValue(Some(nexus)))?;
                    nexuses.push(nexusid);
                }
                nexuses
            },
        ))
    }
}

impl From<ReplicaOwners> for replica::ReplicaOwners {
    fn from(owners: ReplicaOwners) -> Self {
        replica::ReplicaOwners {
            volume: owners.volume().map(|id| id.to_string()),
            nexuses: owners.nexuses().iter().map(|id| id.to_string()).collect(),
        }
    }
}

impl TryFrom<replica::ReplicaSpec> for ReplicaSpec {
    type Error = ReplyError;

    fn try_from(value: replica::ReplicaSpec) -> Result<Self, Self::Error> {
        let replica_spec_status = match common::SpecStatus::from_i32(value.spec_status) {
            Some(status) => status.into(),
            None => {
                return Err(ReplyError::invalid_argument(
                    ResourceKind::ReplicaSpec,
                    "replica_spec.status",
                    "".to_string(),
                ))
            }
        };
        Ok(Self {
            name: ReplicaName::from_string(value.name),
            uuid: ReplicaId::try_from(StringValue(value.replica_id))?,
            size: value.size,
            pool: match value.pool_uuid {
                Some(uuid) => PoolRef::Uuid(
                    value.pool_id.into(),
                    match PoolUuid::try_from(uuid) {
                        Ok(uuid) => uuid,
                        Err(err) => {
                            return Err(ReplyError::invalid_argument(
                                ResourceKind::Replica,
                                "replica_spec.pool_uuid",
                                err.to_string(),
                            ))
                        }
                    },
                ),
                None => PoolRef::Named(value.pool_id.into()),
            },
            share: match common::Protocol::from_i32(value.share) {
                Some(share) => share.into(),
                None => {
                    return Err(ReplyError::invalid_argument(
                        ResourceKind::ReplicaSpec,
                        "replica_spec.share",
                        "".to_string(),
                    ))
                }
            },
            thin: value.thin,
            status: replica_spec_status,
            managed: value.managed,
            owners: match value.owners {
                Some(owners) => ReplicaOwners::try_from(owners)?,
                None => {
                    return Err(ReplyError::missing_argument(
                        ResourceKind::ReplicaSpec,
                        "replica_spec.owners",
                    ))
                }
            },
            sequencer: Default::default(),
            operation: value.operation.map(|op| ReplicaOperationState {
                operation: ReplicaOperation::Create,
                result: op.result,
            }),
            allowed_hosts: vec![],
        })
    }
}

impl From<ReplicaSpec> for replica::ReplicaSpec {
    fn from(value: ReplicaSpec) -> Self {
        let share: common::Protocol = value.share.into();
        let spec_status: common::SpecStatus = value.status.into();
        Self {
            name: value.name.to_string(),
            replica_id: Some(value.uuid.to_string()),
            size: value.size,
            pool_id: match value.pool.clone() {
                PoolRef::Named(id) => id.into(),
                PoolRef::Uuid(id, _) => id.into(),
            },
            pool_uuid: match value.pool {
                PoolRef::Named(_) => None,
                PoolRef::Uuid(_, uuid) => Some(uuid.into()),
            },
            share: share as i32,
            thin: value.thin,
            spec_status: spec_status as i32,
            managed: value.managed,
            owners: Some(value.owners.into()),
            operation: value.operation.map(|operation| common::SpecOperation {
                result: operation.result,
            }),
        }
    }
}

impl TryFrom<StringValue> for ReplicaId {
    type Error = ReplyError;

    fn try_from(value: StringValue) -> Result<Self, Self::Error> {
        match value.0 {
            Some(uuid) => match ReplicaId::try_from(uuid) {
                Ok(replicaid) => Ok(replicaid),
                Err(err) => Err(ReplyError::invalid_argument(
                    ResourceKind::Replica,
                    "replica.uuid",
                    err.to_string(),
                )),
            },
            None => Err(ReplyError::missing_argument(
                ResourceKind::Replica,
                "replica.uuid",
            )),
        }
    }
}
