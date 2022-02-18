use crate::{
    common,
    grpc_opts::Context,
    misc::traits::ValidateRequestTypes,
    nexus, replica, volume,
    volume::{
        get_volumes_request, CreateVolumeRequest, DestroyVolumeRequest, PublishVolumeRequest,
        SetVolumeReplicaRequest, ShareVolumeRequest, UnpublishVolumeRequest, UnshareVolumeRequest,
    },
};
use common_lib::{
    mbus_api::{v0::Volumes, ReplyError, ResourceKind},
    types::v0::{
        message_bus::{
            CreateVolume, DestroyVolume, ExplicitNodeTopology, Filter, LabelledTopology, Nexus,
            NexusId, NodeId, NodeTopology, PoolTopology, PublishVolume, ReplicaId, ReplicaStatus,
            ReplicaTopology, SetVolumeReplica, ShareVolume, Topology, UnpublishVolume,
            UnshareVolume, Volume, VolumeId, VolumeLabels, VolumePolicy, VolumeShareProtocol,
            VolumeState,
        },
        store::volume::{VolumeSpec, VolumeSpecStatus, VolumeTarget},
    },
};
use std::{collections::HashMap, convert::TryFrom};

/// All volume crud operations to be a part of the VolumeOperations trait
#[tonic::async_trait]
pub trait VolumeOperations: Send + Sync {
    /// Create a volume
    async fn create(
        &self,
        req: &dyn CreateVolumeInfo,
        ctx: Option<Context>,
    ) -> Result<Volume, ReplyError>;
    /// Get volumes
    async fn get(&self, filter: Filter, ctx: Option<Context>) -> Result<Volumes, ReplyError>;
    /// Destroy a volume
    async fn destroy(
        &self,
        req: &dyn DestroyVolumeInfo,
        ctx: Option<Context>,
    ) -> Result<(), ReplyError>;
    /// Share a volume
    async fn share(
        &self,
        req: &dyn ShareVolumeInfo,
        ctx: Option<Context>,
    ) -> Result<String, ReplyError>;
    /// Unshare a volume
    async fn unshare(
        &self,
        req: &dyn UnshareVolumeInfo,
        ctx: Option<Context>,
    ) -> Result<(), ReplyError>;
    /// Publish a volume
    async fn publish(
        &self,
        req: &dyn PublishVolumeInfo,
        ctx: Option<Context>,
    ) -> Result<Volume, ReplyError>;
    /// Unpublish a volume
    async fn unpublish(
        &self,
        req: &dyn UnpublishVolumeInfo,
        ctx: Option<Context>,
    ) -> Result<Volume, ReplyError>;
    /// Increase or decrease volume replica
    async fn set_volume_replica(
        &self,
        req: &dyn SetVolumeReplicaInfo,
        ctx: Option<Context>,
    ) -> Result<Volume, ReplyError>;
    /// Liveness probe for volume service
    async fn probe(&self, ctx: Option<Context>) -> Result<bool, ReplyError>;
}

impl From<Volume> for volume::Volume {
    fn from(volume: Volume) -> Self {
        let spec_status: common::SpecStatus = volume.spec().status.into();
        let volume_definition = volume::VolumeDefinition {
            spec: Some(volume::VolumeSpec {
                uuid: Some(volume.spec().uuid.to_string()),
                size: volume.spec().size,
                labels: volume
                    .spec()
                    .labels
                    .map(|labels| crate::common::StringMapValue { value: labels }),
                num_replicas: volume.spec().num_replicas.into(),
                target: volume.spec().target.map(|target| target.into()),
                policy: Some(volume.spec().policy.into()),
                topology: volume.spec().topology.map(|topology| topology.into()),
                last_nexus_id: volume.spec().last_nexus_id.map(|id| id.to_string()),
            }),
            metadata: Some(volume::Metadata {
                status: spec_status as i32,
            }),
        };
        let status: nexus::NexusStatus = volume.state().status.into();
        let volume_state = volume::VolumeState {
            uuid: Some(volume.state().uuid.to_string()),
            size: volume.state().size,
            status: status as i32,
            target: volume.state().target.map(|target| target.into()),
            replica_topology: to_grpc_replica_topology_map(volume.state().replica_topology),
        };
        volume::Volume {
            definition: Some(volume_definition),
            state: Some(volume_state),
        }
    }
}

impl TryFrom<volume::Volume> for Volume {
    type Error = ReplyError;
    fn try_from(volume_grpc_type: volume::Volume) -> Result<Self, Self::Error> {
        let grpc_volume_definition = match volume_grpc_type.definition {
            Some(definition) => definition,
            None => {
                return Err(ReplyError::missing_argument(
                    ResourceKind::Volume,
                    "volume.definition",
                ))
            }
        };
        let grpc_volume_spec = match grpc_volume_definition.spec {
            Some(spec) => spec,
            None => {
                return Err(ReplyError::missing_argument(
                    ResourceKind::Volume,
                    "volume.definition.spec",
                ))
            }
        };
        let grpc_volume_meta = match grpc_volume_definition.metadata {
            Some(meta) => meta,
            None => {
                return Err(ReplyError::missing_argument(
                    ResourceKind::Volume,
                    "volume.definition.metadata",
                ))
            }
        };
        let grpc_volume_state = match volume_grpc_type.state {
            Some(state) => state,
            None => {
                return Err(ReplyError::missing_argument(
                    ResourceKind::Volume,
                    "volume.state",
                ))
            }
        };
        let volume_spec_status = match common::SpecStatus::from_i32(grpc_volume_meta.status) {
            Some(status) => match status {
                common::SpecStatus::Created => {
                    VolumeSpecStatus::Created(grpc_volume_meta.status.into())
                }
                _ => match common::SpecStatus::from_i32(grpc_volume_meta.status) {
                    Some(status) => status.into(),
                    None => {
                        return Err(ReplyError::invalid_argument(
                            ResourceKind::Volume,
                            "volume.definition.spec.status",
                            "".to_string(),
                        ))
                    }
                },
            },
            None => {
                return Err(ReplyError::invalid_argument(
                    ResourceKind::Volume,
                    "volume.definition.spec.status",
                    "".to_string(),
                ))
            }
        };
        let volume_spec = VolumeSpec {
            uuid: match grpc_volume_spec.uuid {
                Some(id) => match VolumeId::try_from(id) {
                    Ok(volume_id) => volume_id,
                    Err(err) => {
                        return Err(ReplyError::invalid_argument(
                            ResourceKind::Volume,
                            "volume.definition.spec.uuid",
                            err.to_string(),
                        ))
                    }
                },
                None => {
                    return Err(ReplyError::missing_argument(
                        ResourceKind::Volume,
                        "volume.definition.spec.uuid",
                    ))
                }
            },
            size: grpc_volume_spec.size,
            labels: match grpc_volume_spec.labels {
                Some(labels) => Some(labels.value),
                None => None,
            },
            num_replicas: grpc_volume_spec.num_replicas as u8,
            status: volume_spec_status,
            target: match grpc_volume_spec.target {
                Some(target) => match VolumeTarget::try_from(target) {
                    Ok(target) => Some(target),
                    Err(err) => {
                        return Err(ReplyError::invalid_argument(
                            ResourceKind::Volume,
                            "volume.definition.spec.target",
                            err.to_string(),
                        ))
                    }
                },
                None => None,
            },
            policy: match grpc_volume_spec.policy {
                Some(policy) => policy.into(),
                None => {
                    return Err(ReplyError::missing_argument(
                        ResourceKind::Volume,
                        "volume.definition.spec.policy",
                    ))
                }
            },
            topology: match grpc_volume_spec.topology {
                Some(topology) => match Topology::try_from(topology) {
                    Ok(topology) => Some(topology),
                    Err(err) => {
                        return Err(ReplyError::invalid_argument(
                            ResourceKind::Volume,
                            "volume.definition.spec.topology",
                            err.to_string(),
                        ))
                    }
                },
                None => None,
            },
            sequencer: Default::default(),
            last_nexus_id: match grpc_volume_spec.last_nexus_id {
                Some(id) => match NexusId::try_from(id) {
                    Ok(volume_id) => Some(volume_id),
                    Err(err) => {
                        return Err(ReplyError::invalid_argument(
                            ResourceKind::Volume,
                            "volume.definition.spec.last_nexus_id",
                            err.to_string(),
                        ))
                    }
                },
                None => None,
            },
            operation: None,
        };
        let volume_state = VolumeState {
            uuid: match grpc_volume_state.uuid {
                Some(id) => match VolumeId::try_from(id) {
                    Ok(volume_id) => volume_id,
                    Err(err) => {
                        return Err(ReplyError::invalid_argument(
                            ResourceKind::Volume,
                            "volume.state.uuid",
                            err.to_string(),
                        ))
                    }
                },
                None => {
                    return Err(ReplyError::missing_argument(
                        ResourceKind::Volume,
                        "volume.state.uuid",
                    ))
                }
            },
            size: grpc_volume_state.size,
            status: match nexus::NexusStatus::from_i32(grpc_volume_state.status) {
                Some(status) => status.into(),
                None => {
                    return Err(ReplyError::invalid_argument(
                        ResourceKind::Volume,
                        "volume.state.status",
                        "".to_string(),
                    ))
                }
            },
            target: match grpc_volume_state.target {
                Some(target) => match Nexus::try_from(target) {
                    Ok(target) => Some(target),
                    Err(err) => {
                        return Err(ReplyError::invalid_argument(
                            ResourceKind::Volume,
                            "volume.state.target",
                            err.to_string(),
                        ))
                    }
                },
                None => None,
            },
            replica_topology: match to_replica_topology_map(grpc_volume_state.replica_topology) {
                Ok(replica_topology_map) => replica_topology_map,
                Err(err) => {
                    return Err(ReplyError::invalid_argument(
                        ResourceKind::Volume,
                        "volume.state.replica_topology",
                        err.to_string(),
                    ))
                }
            },
        };
        Ok(Volume::new(volume_spec, volume_state))
    }
}

impl TryFrom<volume::Volumes> for Volumes {
    type Error = ReplyError;
    fn try_from(grpc_volumes: volume::Volumes) -> Result<Self, Self::Error> {
        let mut volumes: Vec<Volume> = vec![];
        for volume in grpc_volumes.volumes {
            volumes.push(Volume::try_from(volume)?)
        }
        Ok(Volumes(volumes))
    }
}

impl From<Volumes> for volume::Volumes {
    fn from(volumes: Volumes) -> Self {
        volume::Volumes {
            volumes: volumes
                .into_inner()
                .iter()
                .map(|volume| volume.clone().into())
                .collect(),
        }
    }
}

impl TryFrom<volume::ReplicaTopology> for ReplicaTopology {
    type Error = ReplyError;
    fn try_from(replica_topology_grpc_type: volume::ReplicaTopology) -> Result<Self, Self::Error> {
        let node = replica_topology_grpc_type.node.map(|node| node.into());
        let pool = replica_topology_grpc_type.pool.map(|pool| pool.into());
        let status = match ReplicaStatus::try_from(replica_topology_grpc_type.status) {
            Ok(status) => status,
            Err(err) => {
                return Err(ReplyError::invalid_argument(
                    ResourceKind::Volume,
                    "replica_topology.status",
                    err.to_string(),
                ))
            }
        };
        Ok(ReplicaTopology::new(node, pool, status))
    }
}

impl From<ReplicaTopology> for volume::ReplicaTopology {
    fn from(replica_topology: ReplicaTopology) -> Self {
        let node = replica_topology.node().as_ref().map(|id| id.to_string());
        let pool = replica_topology.pool().as_ref().map(|id| id.to_string());
        let status: replica::ReplicaStatus = replica_topology.status().clone().into();
        volume::ReplicaTopology {
            node,
            pool,
            status: status as i32,
        }
    }
}

impl TryFrom<volume::Topology> for Topology {
    type Error = ReplyError;
    fn try_from(topology_grpc_type: volume::Topology) -> Result<Self, Self::Error> {
        let topo = Topology {
            node: match topology_grpc_type.node {
                Some(node_topo) => match NodeTopology::try_from(node_topo) {
                    Ok(node_topo) => Some(node_topo),
                    Err(err) => {
                        return Err(ReplyError::invalid_argument(
                            ResourceKind::Volume,
                            "topology.node_topology",
                            err.to_string(),
                        ))
                    }
                },
                None => None,
            },
            pool: match topology_grpc_type.pool {
                Some(pool_topo) => match PoolTopology::try_from(pool_topo) {
                    Ok(pool_topo) => Some(pool_topo),
                    Err(err) => {
                        return Err(ReplyError::invalid_argument(
                            ResourceKind::Volume,
                            "topology.pool_topology",
                            err.to_string(),
                        ))
                    }
                },
                None => None,
            },
        };
        Ok(topo)
    }
}

impl From<Topology> for volume::Topology {
    fn from(topology: Topology) -> Self {
        volume::Topology {
            node: topology.node.map(|topo| topo.into()),
            pool: topology.pool.map(|topo| topo.into()),
        }
    }
}

impl TryFrom<volume::NodeTopology> for NodeTopology {
    type Error = ReplyError;
    fn try_from(node_topology_grpc_type: volume::NodeTopology) -> Result<Self, Self::Error> {
        let node_topo = match node_topology_grpc_type.topology {
            Some(topo) => match topo {
                volume::node_topology::Topology::Labelled(labels) => {
                    NodeTopology::Labelled(labels.into())
                }
                volume::node_topology::Topology::Explicit(explicit) => {
                    NodeTopology::Explicit(explicit.into())
                }
            },
            None => {
                return Err(ReplyError::missing_argument(
                    ResourceKind::Volume,
                    "node_topology.topology",
                ))
            }
        };
        Ok(node_topo)
    }
}

impl From<NodeTopology> for volume::NodeTopology {
    fn from(src: NodeTopology) -> Self {
        match src {
            NodeTopology::Labelled(labels) => volume::NodeTopology {
                topology: Some(volume::node_topology::Topology::Labelled(labels.into())),
            },
            NodeTopology::Explicit(explicit) => volume::NodeTopology {
                topology: Some(volume::node_topology::Topology::Explicit(explicit.into())),
            },
        }
    }
}

impl TryFrom<volume::PoolTopology> for PoolTopology {
    type Error = ReplyError;
    fn try_from(pool_topology_grpc_type: volume::PoolTopology) -> Result<Self, Self::Error> {
        let pool_topo = match pool_topology_grpc_type.topology {
            Some(topology) => match topology {
                volume::pool_topology::Topology::Labelled(labels) => {
                    PoolTopology::Labelled(labels.into())
                }
            },
            None => {
                return Err(ReplyError::missing_argument(
                    ResourceKind::Volume,
                    "pool_topology.topology",
                ))
            }
        };
        Ok(pool_topo)
    }
}

impl From<PoolTopology> for volume::PoolTopology {
    fn from(src: PoolTopology) -> Self {
        match src {
            PoolTopology::Labelled(labels) => volume::PoolTopology {
                topology: Some(volume::pool_topology::Topology::Labelled(labels.into())),
            },
        }
    }
}

impl From<volume::LabelledTopology> for LabelledTopology {
    fn from(labelled_topology_grpc_type: volume::LabelledTopology) -> Self {
        LabelledTopology {
            exclusion: match labelled_topology_grpc_type.exclusion {
                Some(labels) => labels.value,
                None => HashMap::new(),
            },
            inclusion: match labelled_topology_grpc_type.inclusion {
                Some(labels) => labels.value,
                None => HashMap::new(),
            },
        }
    }
}

impl From<LabelledTopology> for volume::LabelledTopology {
    fn from(topo: LabelledTopology) -> Self {
        volume::LabelledTopology {
            exclusion: Some(crate::common::StringMapValue {
                value: topo.exclusion,
            }),
            inclusion: Some(crate::common::StringMapValue {
                value: topo.inclusion,
            }),
        }
    }
}

impl From<volume::ExplicitNodeTopology> for ExplicitNodeTopology {
    fn from(explicit_topology_grpc_type: volume::ExplicitNodeTopology) -> Self {
        ExplicitNodeTopology {
            allowed_nodes: explicit_topology_grpc_type
                .allowed_nodes
                .into_iter()
                .map(|node| node.into())
                .collect(),
            preferred_nodes: explicit_topology_grpc_type
                .preferred_nodes
                .into_iter()
                .map(|node| node.into())
                .collect(),
        }
    }
}

impl From<ExplicitNodeTopology> for volume::ExplicitNodeTopology {
    fn from(explicit_topo: ExplicitNodeTopology) -> Self {
        volume::ExplicitNodeTopology {
            allowed_nodes: explicit_topo
                .allowed_nodes
                .into_iter()
                .map(|node| node.to_string())
                .collect(),
            preferred_nodes: explicit_topo
                .preferred_nodes
                .into_iter()
                .map(|node| node.to_string())
                .collect(),
        }
    }
}

impl From<volume::VolumePolicy> for VolumePolicy {
    fn from(policy_grpc_type: volume::VolumePolicy) -> Self {
        VolumePolicy {
            self_heal: policy_grpc_type.self_heal,
        }
    }
}

impl From<VolumePolicy> for volume::VolumePolicy {
    fn from(policy: VolumePolicy) -> Self {
        volume::VolumePolicy {
            self_heal: policy.self_heal,
        }
    }
}

impl TryFrom<volume::VolumeTarget> for VolumeTarget {
    type Error = ReplyError;
    fn try_from(volume_target_grpc_type: volume::VolumeTarget) -> Result<Self, Self::Error> {
        let target = VolumeTarget::new(
            volume_target_grpc_type.node_id.into(),
            match volume_target_grpc_type.nexus_id {
                Some(id) => match NexusId::try_from(id) {
                    Ok(nexusid) => nexusid,
                    Err(err) => {
                        return Err(ReplyError::invalid_argument(
                            ResourceKind::Volume,
                            "target.nexus_id",
                            err.to_string(),
                        ))
                    }
                },
                None => {
                    return Err(ReplyError::missing_argument(
                        ResourceKind::Volume,
                        "target.nexus_id",
                    ))
                }
            },
            match volume_target_grpc_type.protocol {
                Some(i) => match volume::VolumeShareProtocol::from_i32(i) {
                    Some(protocol) => Some(protocol.into()),
                    None => {
                        return Err(ReplyError::invalid_argument(
                            ResourceKind::Volume,
                            "target.protocol",
                            "".to_string(),
                        ))
                    }
                },
                None => None,
            },
        );
        Ok(target)
    }
}

impl From<VolumeTarget> for volume::VolumeTarget {
    fn from(target: VolumeTarget) -> Self {
        volume::VolumeTarget {
            node_id: target.node().to_string(),
            nexus_id: Some(target.nexus().to_string()),
            protocol: match target.protocol() {
                None => None,
                Some(protocol) => {
                    let protocol: volume::VolumeShareProtocol = (*protocol).into();
                    Some(protocol as i32)
                }
            },
        }
    }
}

impl From<volume::VolumeShareProtocol> for VolumeShareProtocol {
    fn from(src: volume::VolumeShareProtocol) -> Self {
        match src {
            volume::VolumeShareProtocol::Nvmf => Self::Nvmf,
            volume::VolumeShareProtocol::Iscsi => Self::Iscsi,
        }
    }
}

impl From<VolumeShareProtocol> for volume::VolumeShareProtocol {
    fn from(src: VolumeShareProtocol) -> Self {
        match src {
            VolumeShareProtocol::Nvmf => Self::Nvmf,
            VolumeShareProtocol::Iscsi => Self::Iscsi,
        }
    }
}

impl TryFrom<get_volumes_request::Filter> for Filter {
    type Error = ReplyError;
    fn try_from(filter: get_volumes_request::Filter) -> Result<Self, Self::Error> {
        Ok(match filter {
            get_volumes_request::Filter::Volume(volume_filter) => {
                Filter::Volume(match VolumeId::try_from(volume_filter.volume_id) {
                    Ok(volumeid) => volumeid,
                    Err(err) => {
                        return Err(ReplyError::invalid_argument(
                            ResourceKind::Volume,
                            "volume_filter.volume_id",
                            err.to_string(),
                        ))
                    }
                })
            }
        })
    }
}

/// Trait to be implemented for CreateVolume operation
pub trait CreateVolumeInfo: Send + Sync {
    /// Uuid of the volume
    fn uuid(&self) -> VolumeId;
    /// Size in bytes of the volume
    fn size(&self) -> u64;
    /// No of replicas of the volume
    fn replicas(&self) -> u64;
    /// Volume policy of the volume, i.e self_heal
    fn policy(&self) -> VolumePolicy;
    /// Topology configuration of the volume
    fn topology(&self) -> Option<Topology>;
    /// Labels to be added to the volumes for topology based scheduling
    fn labels(&self) -> Option<VolumeLabels>;
}

impl CreateVolumeInfo for CreateVolume {
    fn uuid(&self) -> VolumeId {
        self.uuid.clone()
    }

    fn size(&self) -> u64 {
        self.size
    }

    fn replicas(&self) -> u64 {
        self.replicas
    }

    fn policy(&self) -> VolumePolicy {
        self.policy.clone()
    }

    fn topology(&self) -> Option<Topology> {
        self.topology.clone()
    }

    fn labels(&self) -> Option<VolumeLabels> {
        self.labels.clone()
    }
}

/// Intermediate structure that validates the conversion to CreateVolumeRequest type
pub struct ValidatedCreateVolumeRequest {
    inner: CreateVolumeRequest,
    uuid: VolumeId,
    topology: Option<Topology>,
}

impl CreateVolumeInfo for ValidatedCreateVolumeRequest {
    fn uuid(&self) -> VolumeId {
        self.uuid.clone()
    }

    fn size(&self) -> u64 {
        self.inner.size
    }

    fn replicas(&self) -> u64 {
        self.inner.replicas
    }

    fn policy(&self) -> VolumePolicy {
        match self.inner.policy.clone() {
            Some(policy) => policy.into(),
            None => VolumePolicy::default(),
        }
    }

    fn topology(&self) -> Option<Topology> {
        self.topology.clone()
    }

    fn labels(&self) -> Option<VolumeLabels> {
        match self.inner.labels.clone() {
            None => None,
            Some(labels) => Some(labels.value),
        }
    }
}

impl ValidateRequestTypes for CreateVolumeRequest {
    type Validated = ValidatedCreateVolumeRequest;
    fn validated(self) -> Result<Self::Validated, ReplyError> {
        Ok(ValidatedCreateVolumeRequest {
            uuid: match self.uuid.clone() {
                Some(uuid) => match VolumeId::try_from(uuid) {
                    Ok(volumeid) => volumeid,
                    Err(err) => {
                        return Err(ReplyError::invalid_argument(
                            ResourceKind::Volume,
                            "create_volume_request.uuid",
                            err.to_string(),
                        ))
                    }
                },
                None => {
                    return Err(ReplyError::missing_argument(
                        ResourceKind::Volume,
                        "create_volume_request.uuid",
                    ))
                }
            },
            topology: match self.topology.clone() {
                Some(topology) => match Topology::try_from(topology) {
                    Ok(topology) => Some(topology),
                    Err(err) => {
                        return Err(ReplyError::invalid_argument(
                            ResourceKind::Volume,
                            "create_volume_request.topology",
                            err.to_string(),
                        ))
                    }
                },
                None => None,
            },
            inner: self,
        })
    }
}

impl From<&dyn CreateVolumeInfo> for CreateVolume {
    fn from(data: &dyn CreateVolumeInfo) -> Self {
        Self {
            uuid: data.uuid(),
            size: data.size(),
            replicas: data.replicas(),
            policy: data.policy(),
            topology: data.topology(),
            labels: data.labels(),
        }
    }
}

impl From<&dyn CreateVolumeInfo> for CreateVolumeRequest {
    fn from(data: &dyn CreateVolumeInfo) -> Self {
        Self {
            uuid: Some(data.uuid().to_string()),
            size: data.size(),
            replicas: data.replicas(),
            policy: Some(data.policy().into()),
            topology: data.topology().map(|topo| topo.into()),
            labels: data
                .labels()
                .map(|labels| crate::common::StringMapValue { value: labels }),
        }
    }
}

/// Trait to be implemented for DestroyVolume operation
pub trait DestroyVolumeInfo: Send + Sync {
    /// Uuid of the volume to be destroyed
    fn uuid(&self) -> VolumeId;
}

impl DestroyVolumeInfo for DestroyVolume {
    fn uuid(&self) -> VolumeId {
        self.uuid.clone()
    }
}

/// Intermediate structure that validates the conversion to DestroyVolumeRequest type
pub struct ValidatedDestroyVolumeRequest {
    uuid: VolumeId,
}

impl DestroyVolumeInfo for ValidatedDestroyVolumeRequest {
    fn uuid(&self) -> VolumeId {
        self.uuid.clone()
    }
}

impl ValidateRequestTypes for DestroyVolumeRequest {
    type Validated = ValidatedDestroyVolumeRequest;
    fn validated(self) -> Result<Self::Validated, ReplyError> {
        Ok(ValidatedDestroyVolumeRequest {
            uuid: match self.uuid {
                Some(uuid) => match VolumeId::try_from(uuid) {
                    Ok(volumeid) => volumeid,
                    Err(err) => {
                        return Err(ReplyError::invalid_argument(
                            ResourceKind::Volume,
                            "destroy_volume_request.uuid",
                            err.to_string(),
                        ))
                    }
                },
                None => {
                    return Err(ReplyError::missing_argument(
                        ResourceKind::Volume,
                        "destroy_volume_request.uuid",
                    ))
                }
            },
        })
    }
}

impl From<&dyn DestroyVolumeInfo> for DestroyVolume {
    fn from(data: &dyn DestroyVolumeInfo) -> Self {
        Self { uuid: data.uuid() }
    }
}

impl From<&dyn DestroyVolumeInfo> for DestroyVolumeRequest {
    fn from(data: &dyn DestroyVolumeInfo) -> Self {
        Self {
            uuid: Some(data.uuid().to_string()),
        }
    }
}

/// Trait to be implemented for ShareVolume operation
pub trait ShareVolumeInfo: Send + Sync {
    /// Uuid of the volume to be shared
    fn uuid(&self) -> VolumeId;
    /// Protocol over which the volume be shared
    fn share(&self) -> VolumeShareProtocol;
}

impl ShareVolumeInfo for ShareVolume {
    fn uuid(&self) -> VolumeId {
        self.uuid.clone()
    }

    fn share(&self) -> VolumeShareProtocol {
        self.protocol
    }
}

/// Intermediate structure that validates the conversion to ShareVolumeRequest type
pub struct ValidatedShareVolumeRequest {
    inner: ShareVolumeRequest,
    uuid: VolumeId,
}

impl ShareVolumeInfo for ValidatedShareVolumeRequest {
    fn uuid(&self) -> VolumeId {
        self.uuid.clone()
    }

    fn share(&self) -> VolumeShareProtocol {
        self.inner.share.into()
    }
}

impl ValidateRequestTypes for ShareVolumeRequest {
    type Validated = ValidatedShareVolumeRequest;
    fn validated(self) -> Result<Self::Validated, ReplyError> {
        Ok(ValidatedShareVolumeRequest {
            uuid: match self.uuid.clone() {
                Some(uuid) => match VolumeId::try_from(uuid) {
                    Ok(volumeid) => volumeid,
                    Err(err) => {
                        return Err(ReplyError::invalid_argument(
                            ResourceKind::Volume,
                            "share_volume_request.uuid",
                            err.to_string(),
                        ))
                    }
                },
                None => {
                    return Err(ReplyError::missing_argument(
                        ResourceKind::Volume,
                        "share_volume_request.uuid",
                    ))
                }
            },
            inner: self,
        })
    }
}

impl From<&dyn ShareVolumeInfo> for ShareVolume {
    fn from(data: &dyn ShareVolumeInfo) -> Self {
        Self {
            uuid: data.uuid(),
            protocol: data.share(),
        }
    }
}

impl From<&dyn ShareVolumeInfo> for ShareVolumeRequest {
    fn from(data: &dyn ShareVolumeInfo) -> Self {
        let share: volume::VolumeShareProtocol = data.share().into();
        Self {
            uuid: Some(data.uuid().to_string()),
            share: share as i32,
        }
    }
}

/// Trait to be implemented for UnshareVolume operation
pub trait UnshareVolumeInfo: Send + Sync {
    /// Uuid of the volume to be unshared
    fn uuid(&self) -> VolumeId;
}

impl UnshareVolumeInfo for UnshareVolume {
    fn uuid(&self) -> VolumeId {
        self.uuid.clone()
    }
}

/// Intermediate structure that validates the conversion to UnshareVolumeRequest type
pub struct ValidatedUnshareVolumeRequest {
    uuid: VolumeId,
}

impl UnshareVolumeInfo for ValidatedUnshareVolumeRequest {
    fn uuid(&self) -> VolumeId {
        self.uuid.clone()
    }
}

impl ValidateRequestTypes for UnshareVolumeRequest {
    type Validated = ValidatedUnshareVolumeRequest;
    fn validated(self) -> Result<Self::Validated, ReplyError> {
        Ok(ValidatedUnshareVolumeRequest {
            uuid: match self.uuid {
                Some(uuid) => match VolumeId::try_from(uuid) {
                    Ok(volumeid) => volumeid,
                    Err(err) => {
                        return Err(ReplyError::invalid_argument(
                            ResourceKind::Volume,
                            "unshare_volume_request.uuid",
                            err.to_string(),
                        ))
                    }
                },
                None => {
                    return Err(ReplyError::missing_argument(
                        ResourceKind::Volume,
                        "unshare_volume_request.uuid",
                    ))
                }
            },
        })
    }
}

impl From<&dyn UnshareVolumeInfo> for UnshareVolume {
    fn from(data: &dyn UnshareVolumeInfo) -> Self {
        Self { uuid: data.uuid() }
    }
}

impl From<&dyn UnshareVolumeInfo> for UnshareVolumeRequest {
    fn from(data: &dyn UnshareVolumeInfo) -> Self {
        Self {
            uuid: Some(data.uuid().to_string()),
        }
    }
}

/// Trait to be implemented for PublishVolume operation
pub trait PublishVolumeInfo: Send + Sync {
    /// Uuid of the volume to be published
    fn uuid(&self) -> VolumeId;
    /// The node where front-end IO will be sent to
    fn target_node(&self) -> Option<NodeId>;
    /// The protocol over which volume be published
    fn share(&self) -> Option<VolumeShareProtocol>;
}

impl PublishVolumeInfo for PublishVolume {
    fn uuid(&self) -> VolumeId {
        self.uuid.clone()
    }

    fn target_node(&self) -> Option<NodeId> {
        self.target_node.clone()
    }

    fn share(&self) -> Option<VolumeShareProtocol> {
        self.share
    }
}

/// Intermediate structure that validates the conversion to PublishVolumeRequest type
pub struct ValidatedPublishVolumeRequest {
    inner: PublishVolumeRequest,
    uuid: VolumeId,
    share: Option<VolumeShareProtocol>,
}

impl PublishVolumeInfo for ValidatedPublishVolumeRequest {
    fn uuid(&self) -> VolumeId {
        self.uuid.clone()
    }

    fn target_node(&self) -> Option<NodeId> {
        self.inner
            .target_node
            .clone()
            .map(|target_node| target_node.into())
    }

    fn share(&self) -> Option<VolumeShareProtocol> {
        self.share
    }
}

impl ValidateRequestTypes for PublishVolumeRequest {
    type Validated = ValidatedPublishVolumeRequest;
    fn validated(self) -> Result<Self::Validated, ReplyError> {
        Ok(ValidatedPublishVolumeRequest {
            uuid: match self.uuid.clone() {
                Some(uuid) => match VolumeId::try_from(uuid) {
                    Ok(volumeid) => volumeid,
                    Err(err) => {
                        return Err(ReplyError::invalid_argument(
                            ResourceKind::Volume,
                            "publish_volume_request.uuid",
                            err.to_string(),
                        ))
                    }
                },
                None => {
                    return Err(ReplyError::missing_argument(
                        ResourceKind::Volume,
                        "publish_volume_request.uuid",
                    ))
                }
            },
            share: match self.share {
                Some(share) => match volume::VolumeShareProtocol::from_i32(share) {
                    Some(share) => Some(share.into()),
                    None => {
                        return Err(ReplyError::invalid_argument(
                            ResourceKind::Volume,
                            "publish_volume_request.share",
                            "".to_string(),
                        ))
                    }
                },
                None => None,
            },
            inner: self,
        })
    }
}

impl From<&dyn PublishVolumeInfo> for PublishVolume {
    fn from(data: &dyn PublishVolumeInfo) -> Self {
        Self {
            uuid: data.uuid(),
            target_node: data.target_node(),
            share: data.share(),
        }
    }
}

impl From<&dyn PublishVolumeInfo> for PublishVolumeRequest {
    fn from(data: &dyn PublishVolumeInfo) -> Self {
        let share: Option<i32> = match data.share() {
            None => None,
            Some(protocol) => {
                let protocol: volume::VolumeShareProtocol = protocol.into();
                Some(protocol as i32)
            }
        };
        Self {
            uuid: Some(data.uuid().to_string()),
            target_node: data.target_node().map(|node_id| node_id.to_string()),
            share,
        }
    }
}

/// Trait to be implemented for PublishVolume operation
pub trait UnpublishVolumeInfo: Send + Sync {
    /// Uuid of the volume to unpublish
    fn uuid(&self) -> VolumeId;
    /// Force unpublish
    fn force(&self) -> bool;
}

impl UnpublishVolumeInfo for UnpublishVolume {
    fn uuid(&self) -> VolumeId {
        self.uuid.clone()
    }

    fn force(&self) -> bool {
        self.force()
    }
}

/// Intermediate structure that validates the conversion to UnpublishVolumeRequest type
pub struct ValidatedUnpublishVolumeRequest {
    inner: UnpublishVolumeRequest,
    uuid: VolumeId,
}

impl UnpublishVolumeInfo for ValidatedUnpublishVolumeRequest {
    fn uuid(&self) -> VolumeId {
        self.uuid.clone()
    }
    fn force(&self) -> bool {
        self.inner.force
    }
}

impl ValidateRequestTypes for UnpublishVolumeRequest {
    type Validated = ValidatedUnpublishVolumeRequest;
    fn validated(self) -> Result<Self::Validated, ReplyError> {
        Ok(ValidatedUnpublishVolumeRequest {
            uuid: match self.uuid.clone() {
                Some(uuid) => match VolumeId::try_from(uuid) {
                    Ok(volumeid) => volumeid,
                    Err(err) => {
                        return Err(ReplyError::invalid_argument(
                            ResourceKind::Volume,
                            "unpublish_volume_request.uuid",
                            err.to_string(),
                        ))
                    }
                },
                None => {
                    return Err(ReplyError::missing_argument(
                        ResourceKind::Volume,
                        "unpublish_volume_request.uuid",
                    ))
                }
            },
            inner: self,
        })
    }
}

impl From<&dyn UnpublishVolumeInfo> for UnpublishVolume {
    fn from(data: &dyn UnpublishVolumeInfo) -> Self {
        UnpublishVolume::new(&data.uuid(), data.force())
    }
}

impl From<&dyn UnpublishVolumeInfo> for UnpublishVolumeRequest {
    fn from(data: &dyn UnpublishVolumeInfo) -> Self {
        Self {
            uuid: Some(data.uuid().to_string()),
            force: data.force(),
        }
    }
}

/// Trait to be implemented for SetVolumeReplica operation
pub trait SetVolumeReplicaInfo: Send + Sync {
    /// Uuid of the concerned volume
    fn uuid(&self) -> VolumeId;
    /// No of replicas we want to set for the volume
    fn replicas(&self) -> u8;
}

impl SetVolumeReplicaInfo for SetVolumeReplica {
    fn uuid(&self) -> VolumeId {
        self.uuid.clone()
    }

    fn replicas(&self) -> u8 {
        self.replicas
    }
}

/// Intermediate structure that validates the conversion to SetVolumeReplicaRequest type
pub struct ValidatedSetVolumeReplicaRequest {
    inner: SetVolumeReplicaRequest,
    uuid: VolumeId,
}

impl SetVolumeReplicaInfo for ValidatedSetVolumeReplicaRequest {
    fn uuid(&self) -> VolumeId {
        self.uuid.clone()
    }
    fn replicas(&self) -> u8 {
        self.inner.replicas as u8
    }
}

impl ValidateRequestTypes for SetVolumeReplicaRequest {
    type Validated = ValidatedSetVolumeReplicaRequest;
    fn validated(self) -> Result<Self::Validated, ReplyError> {
        Ok(ValidatedSetVolumeReplicaRequest {
            uuid: match self.uuid.clone() {
                Some(uuid) => match VolumeId::try_from(uuid) {
                    Ok(volumeid) => volumeid,
                    Err(err) => {
                        return Err(ReplyError::invalid_argument(
                            ResourceKind::Volume,
                            "set_volume_replica.uuid",
                            err.to_string(),
                        ))
                    }
                },
                None => {
                    return Err(ReplyError::missing_argument(
                        ResourceKind::Volume,
                        "set_volume_replica.uuid",
                    ))
                }
            },
            inner: self,
        })
    }
}

impl From<&dyn SetVolumeReplicaInfo> for SetVolumeReplica {
    fn from(data: &dyn SetVolumeReplicaInfo) -> Self {
        Self {
            uuid: data.uuid(),
            replicas: data.replicas(),
        }
    }
}

impl From<&dyn SetVolumeReplicaInfo> for SetVolumeReplicaRequest {
    fn from(data: &dyn SetVolumeReplicaInfo) -> Self {
        Self {
            uuid: Some(data.uuid().to_string()),
            replicas: data.replicas().into(),
        }
    }
}

/// A helper to convert the replica topology map form grpc type to corresponding control plane type
fn to_replica_topology_map(
    map: HashMap<String, volume::ReplicaTopology>,
) -> Result<HashMap<ReplicaId, ReplicaTopology>, ReplyError> {
    let mut replica_topology_map: HashMap<ReplicaId, ReplicaTopology> = HashMap::new();
    for (k, v) in map {
        let replica_id = match ReplicaId::try_from(k) {
            Ok(id) => id,
            Err(err) => {
                return Err(ReplyError::invalid_argument(
                    ResourceKind::Volume,
                    "replica_id",
                    err.to_string(),
                ))
            }
        };
        let replica_topology = match ReplicaTopology::try_from(v) {
            Ok(topology) => topology,
            Err(err) => {
                return Err(ReplyError::invalid_argument(
                    ResourceKind::Volume,
                    "replica_toplogy",
                    err.to_string(),
                ))
            }
        };
        replica_topology_map.insert(replica_id, replica_topology);
    }
    Ok(replica_topology_map)
}

/// A helper to convert the replica topology map form control plane type to corresponding grpc type
fn to_grpc_replica_topology_map(
    map: HashMap<ReplicaId, ReplicaTopology>,
) -> HashMap<String, volume::ReplicaTopology> {
    let mut replica_topology_map: HashMap<String, volume::ReplicaTopology> = HashMap::new();
    for (k, v) in map {
        replica_topology_map.insert(k.to_string(), v.into());
    }
    replica_topology_map
}
