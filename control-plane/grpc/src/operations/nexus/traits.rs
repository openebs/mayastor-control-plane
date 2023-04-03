use crate::{
    common,
    context::Context,
    misc::traits::{StringValue, ValidateRequestTypes},
    nexus,
    nexus::{
        get_nexuses_request, AddNexusChildRequest, CreateNexusRequest, DestroyNexusRequest,
        RemoveNexusChildRequest, ShareNexusRequest, UnshareNexusRequest,
    },
};
use std::convert::TryFrom;
use stor_port::{
    transport_api::{v0::Nexuses, ReplyError, ResourceKind},
    types::v0::{
        store::{
            nexus::{
                NexusOperation, NexusOperationState, NexusSpec, NexusSpecStatus, NexusStatusInfo,
                ReplicaUri,
            },
            nexus_child::NexusChild,
        },
        transport::{
            AddNexusChild, Child, ChildState, ChildStateReason, ChildUri, CreateNexus,
            DestroyNexus, Filter, HostNqn, Nexus, NexusId, NexusNvmePreemption, NexusNvmfConfig,
            NexusShareProtocol, NexusStatus, NodeId, NvmeReservation, NvmfControllerIdRange,
            RemoveNexusChild, ReplicaId, ShareNexus, UnshareNexus, VolumeId,
        },
    },
    IntoOption, TryIntoOption,
};

/// All nexus operations to be a part of the NexusOperations trait
#[tonic::async_trait]
pub trait NexusOperations: Send + Sync {
    /// Create a Nexus
    async fn create(
        &self,
        req: &dyn CreateNexusInfo,
        ctx: Option<Context>,
    ) -> Result<Nexus, ReplyError>;
    /// Get Nexuses based on filters
    async fn get(&self, filter: Filter, ctx: Option<Context>) -> Result<Nexuses, ReplyError>;
    /// Destroy a Nexus
    async fn destroy(
        &self,
        req: &dyn DestroyNexusInfo,
        ctx: Option<Context>,
    ) -> Result<(), ReplyError>;
    /// Share a Nexus
    async fn share(
        &self,
        req: &dyn ShareNexusInfo,
        ctx: Option<Context>,
    ) -> Result<String, ReplyError>;
    /// Unshare a Nexus
    async fn unshare(
        &self,
        req: &dyn UnshareNexusInfo,
        ctx: Option<Context>,
    ) -> Result<(), ReplyError>;
    /// Add Nexus child
    async fn add_nexus_child(
        &self,
        req: &dyn AddNexusChildInfo,
        ctx: Option<Context>,
    ) -> Result<Child, ReplyError>;
    /// Remove Nexus Child
    async fn remove_nexus_child(
        &self,
        req: &dyn RemoveNexusChildInfo,
        ctx: Option<Context>,
    ) -> Result<(), ReplyError>;
}

impl TryFrom<nexus::Nexus> for Nexus {
    type Error = ReplyError;
    fn try_from(nexus_grpc_type: nexus::Nexus) -> Result<Self, Self::Error> {
        let mut children: Vec<Child> = vec![];
        for child_grpc_type in nexus_grpc_type.children {
            let child = match Child::try_from(child_grpc_type) {
                Ok(child) => child,
                Err(err) => {
                    return Err(ReplyError::invalid_argument(
                        ResourceKind::Nexus,
                        "nexus.children",
                        err.to_string(),
                    ))
                }
            };
            children.push(child)
        }
        let nexus = Nexus {
            node: nexus_grpc_type.node_id.into(),
            name: nexus_grpc_type.name,
            uuid: NexusId::try_from(StringValue(nexus_grpc_type.uuid))?,
            size: nexus_grpc_type.size,
            status: match nexus::NexusStatus::from_i32(nexus_grpc_type.status) {
                Some(status) => status.into(),
                None => {
                    return Err(ReplyError::invalid_argument(
                        ResourceKind::Nexus,
                        "nexus.status",
                        "".to_string(),
                    ))
                }
            },
            children,
            device_uri: nexus_grpc_type.device_uri,
            rebuilds: nexus_grpc_type.rebuilds,
            share: match common::Protocol::from_i32(nexus_grpc_type.share) {
                Some(share) => share.into(),
                None => {
                    return Err(ReplyError::invalid_argument(
                        ResourceKind::Nexus,
                        "nexus.share",
                        "".to_string(),
                    ))
                }
            },
            allowed_hosts: vec![],
        };
        Ok(nexus)
    }
}

impl From<Nexus> for nexus::Nexus {
    fn from(nexus: Nexus) -> Self {
        let share: common::Protocol = nexus.share.into();
        let status: nexus::NexusStatus = nexus.status.into();
        nexus::Nexus {
            node_id: nexus.node.to_string(),
            name: nexus.name.to_string(),
            uuid: Some(nexus.uuid.to_string()),
            size: nexus.size,
            children: nexus
                .children
                .into_iter()
                .map(|child| child.into())
                .collect(),
            device_uri: nexus.device_uri.to_string(),
            rebuilds: nexus.rebuilds,
            share: share as i32,
            status: status as i32,
        }
    }
}

impl TryFrom<nexus::Nexuses> for Nexuses {
    type Error = ReplyError;
    fn try_from(grpc_nexuses_type: nexus::Nexuses) -> Result<Self, Self::Error> {
        let mut nexuses: Vec<Nexus> = vec![];
        for nexus in grpc_nexuses_type.nexuses {
            nexuses.push(Nexus::try_from(nexus.clone())?)
        }
        Ok(Nexuses(nexuses))
    }
}

impl From<Nexuses> for nexus::Nexuses {
    fn from(nexuses: Nexuses) -> Self {
        nexus::Nexuses {
            nexuses: nexuses
                .into_inner()
                .iter()
                .map(|nexuses| nexuses.clone().into())
                .collect(),
        }
    }
}

impl From<nexus::NexusStatus> for NexusStatus {
    fn from(src: nexus::NexusStatus) -> Self {
        match src {
            nexus::NexusStatus::Unknown => Self::Unknown,
            nexus::NexusStatus::Online => Self::Online,
            nexus::NexusStatus::Degraded => Self::Degraded,
            nexus::NexusStatus::Faulted => Self::Faulted,
            nexus::NexusStatus::Shutdown => Self::Shutdown,
        }
    }
}
impl From<NexusStatus> for nexus::NexusStatus {
    fn from(src: NexusStatus) -> Self {
        match src {
            NexusStatus::Online => Self::Online,
            NexusStatus::Degraded => Self::Degraded,
            NexusStatus::Faulted => Self::Faulted,
            NexusStatus::Shutdown => Self::Shutdown,
            NexusStatus::Unknown => Self::Unknown,
        }
    }
}

impl From<nexus::ChildState> for ChildState {
    fn from(src: nexus::ChildState) -> Self {
        match src {
            nexus::ChildState::ChildUnknown => Self::Unknown,
            nexus::ChildState::ChildOnline => Self::Online,
            nexus::ChildState::ChildDegraded => Self::Degraded,
            nexus::ChildState::ChildFaulted => Self::Faulted,
        }
    }
}
impl From<ChildState> for nexus::ChildState {
    fn from(src: ChildState) -> Self {
        Self::from(&src)
    }
}
impl From<&ChildState> for nexus::ChildState {
    fn from(src: &ChildState) -> Self {
        match src {
            ChildState::Unknown => Self::ChildUnknown,
            ChildState::Online => Self::ChildOnline,
            ChildState::Degraded => Self::ChildDegraded,
            ChildState::Faulted => Self::ChildFaulted,
        }
    }
}

impl From<nexus::ChildStateReason> for ChildStateReason {
    fn from(src: nexus::ChildStateReason) -> Self {
        match src {
            nexus::ChildStateReason::None => Self::Unknown,
            nexus::ChildStateReason::Init => Self::Init,
            nexus::ChildStateReason::Closed => Self::Closed,
            nexus::ChildStateReason::CannotOpen => Self::CantOpen,
            nexus::ChildStateReason::ConfigInvalid => Self::ConfigInvalid,
            nexus::ChildStateReason::RebuildFailed => Self::RebuildFailed,
            nexus::ChildStateReason::IoFailure => Self::IoError,
            nexus::ChildStateReason::ByClient => Self::ByClient,
            nexus::ChildStateReason::OutOfSync => Self::OutOfSync,
            nexus::ChildStateReason::NoSpace => Self::NoSpace,
            nexus::ChildStateReason::TimedOut => Self::TimedOut,
            nexus::ChildStateReason::AdminFailed => Self::AdminCommandFailed,
        }
    }
}
impl From<ChildStateReason> for nexus::ChildStateReason {
    fn from(src: ChildStateReason) -> Self {
        Self::from(&src)
    }
}
impl From<&ChildStateReason> for nexus::ChildStateReason {
    fn from(src: &ChildStateReason) -> Self {
        match src {
            ChildStateReason::Unknown => Self::None,
            ChildStateReason::Init => Self::Init,
            ChildStateReason::Closed => Self::Closed,
            ChildStateReason::ConfigInvalid => Self::ConfigInvalid,
            ChildStateReason::OutOfSync => Self::OutOfSync,
            ChildStateReason::NoSpace => Self::NoSpace,
            ChildStateReason::TimedOut => Self::TimedOut,
            ChildStateReason::CantOpen => Self::CannotOpen,
            ChildStateReason::RebuildFailed => Self::RebuildFailed,
            ChildStateReason::IoError => Self::IoFailure,
            ChildStateReason::ByClient => Self::ByClient,
            ChildStateReason::AdminCommandFailed => Self::AdminFailed,
        }
    }
}

impl TryFrom<nexus::Child> for Child {
    type Error = ReplyError;
    fn try_from(child_grpc_type: nexus::Child) -> Result<Self, Self::Error> {
        let child = Child {
            uri: child_grpc_type.uri.into(),
            state: match nexus::ChildState::from_i32(child_grpc_type.state).map(ChildState::from) {
                Some(state) => state,
                None => {
                    return Err(ReplyError::invalid_argument(
                        ResourceKind::Child,
                        "state",
                        child_grpc_type.state.to_string(),
                    ))
                }
            },
            rebuild_progress: child_grpc_type.rebuild_progress.map(|i| i as u8),
            state_reason: nexus::ChildStateReason::from_i32(child_grpc_type.reason)
                .map(ChildStateReason::from)
                .unwrap_or(ChildStateReason::Unknown),
            faulted_at: None,
        };
        Ok(child)
    }
}

impl From<Child> for nexus::Child {
    fn from(child: Child) -> Self {
        nexus::Child {
            uri: child.uri.to_string(),
            state: nexus::ChildState::from(child.state) as i32,
            rebuild_progress: child.rebuild_progress.map(|i| i.into()),
            reason: nexus::ChildStateReason::from(child.state_reason).into(),
        }
    }
}

impl From<common::SpecStatus> for NexusSpecStatus {
    fn from(src: common::SpecStatus) -> Self {
        match src {
            common::SpecStatus::Created => Self::Created(Default::default()),
            common::SpecStatus::Creating => Self::Creating,
            common::SpecStatus::Deleted => Self::Deleted,
            common::SpecStatus::Deleting => Self::Deleting,
        }
    }
}

impl From<NexusSpecStatus> for common::SpecStatus {
    fn from(src: NexusSpecStatus) -> Self {
        match src {
            NexusSpecStatus::Created(_) => Self::Created,
            NexusSpecStatus::Creating => Self::Creating,
            NexusSpecStatus::Deleted => Self::Deleted,
            NexusSpecStatus::Deleting => Self::Deleting,
        }
    }
}

impl TryFrom<nexus::NexusSpec> for NexusSpec {
    type Error = ReplyError;

    fn try_from(value: nexus::NexusSpec) -> Result<Self, Self::Error> {
        let nexus_spec_status = match common::SpecStatus::from_i32(value.spec_status) {
            Some(status) => status.into(),
            None => {
                return Err(ReplyError::invalid_argument(
                    ResourceKind::Nexus,
                    "nexus_spec.status",
                    "".to_string(),
                ))
            }
        };
        Ok(Self {
            uuid: NexusId::try_from(StringValue(value.nexus_id))?,
            name: value.name,
            node: value.node_id.into(),
            children: {
                let mut children: Vec<NexusChild> = vec![];
                for child in value.children {
                    children.push(NexusChild::try_from(child)?)
                }
                children
            },
            size: value.size,
            spec_status: nexus_spec_status,
            share: match common::Protocol::from_i32(value.share) {
                Some(share) => share.into(),
                None => {
                    return Err(ReplyError::invalid_argument(
                        ResourceKind::Nexus,
                        "nexus_spec.share",
                        "".to_string(),
                    ))
                }
            },
            managed: value.managed,
            owner: match value.owner {
                Some(owner) => match VolumeId::try_from(owner) {
                    Ok(id) => Some(id),
                    Err(err) => {
                        return Err(ReplyError::invalid_argument(
                            ResourceKind::Nexus,
                            "nexus_spec.owner",
                            err.to_string(),
                        ))
                    }
                },
                None => None,
            },
            sequencer: Default::default(),
            operation: value.operation.map(|op| NexusOperationState {
                operation: NexusOperation::Create,
                result: op.result,
            }),
            nvmf_config: value.nvmf_config.try_into_opt()?,
            status_info: NexusStatusInfo::new(false),
            allowed_hosts: value
                .allowed_hosts
                .clone()
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        })
    }
}

impl From<NexusSpec> for nexus::NexusSpec {
    fn from(value: NexusSpec) -> Self {
        let share: common::Protocol = value.share.into();
        let spec_status: common::SpecStatus = value.spec_status.into();
        Self {
            nexus_id: Some(value.uuid.to_string()),
            name: value.name,
            node_id: value.node.to_string(),
            children: value
                .children
                .into_iter()
                .map(|child| child.into())
                .collect(),
            size: value.size,
            spec_status: spec_status as i32,
            share: share as i32,
            managed: value.managed,
            owner: value.owner.map(|volumeid| volumeid.to_string()),
            operation: value.operation.map(|operation| common::SpecOperation {
                result: operation.result,
            }),
            nvmf_config: value.nvmf_config.into_opt(),
            allowed_hosts: match value
                .allowed_hosts
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()
            {
                Ok(host_nqn) => host_nqn,
                Err(_) => vec![],
            },
        }
    }
}

impl TryFrom<StringValue> for NexusId {
    type Error = ReplyError;

    fn try_from(value: StringValue) -> Result<Self, Self::Error> {
        match value.0 {
            Some(uuid) => match NexusId::try_from(uuid) {
                Ok(nexusid) => Ok(nexusid),
                Err(err) => Err(ReplyError::invalid_argument(
                    ResourceKind::Nexus,
                    "nexus.uuid",
                    err.to_string(),
                )),
            },
            None => Err(ReplyError::missing_argument(
                ResourceKind::Nexus,
                "nexus.uuid",
            )),
        }
    }
}

impl From<NexusChild> for nexus::NexusChild {
    fn from(value: NexusChild) -> Self {
        match value {
            NexusChild::Replica(replica_uri) => nexus::NexusChild {
                child: Some(nexus::nexus_child::Child::Replica(nexus::Replica {
                    replica_id: Some(replica_uri.uuid().to_string()),
                    child_uri: replica_uri.uri().to_string(),
                })),
            },
            NexusChild::Uri(child_uri) => nexus::NexusChild {
                child: Some(nexus::nexus_child::Child::Uri(nexus::Uri {
                    child_uri: child_uri.to_string(),
                })),
            },
        }
    }
}

impl TryFrom<nexus::NexusChild> for NexusChild {
    type Error = ReplyError;

    fn try_from(value: nexus::NexusChild) -> Result<Self, Self::Error> {
        match value.child {
            Some(child) => Ok(match child {
                nexus::nexus_child::Child::Replica(replica) => {
                    NexusChild::Replica(ReplicaUri::new(
                        &ReplicaId::try_from(StringValue(replica.replica_id))?,
                        &ChildUri::from(replica.child_uri),
                    ))
                }
                nexus::nexus_child::Child::Uri(uri) => {
                    NexusChild::Uri(ChildUri::from(uri.child_uri))
                }
            }),
            None => Err(ReplyError::invalid_argument(
                ResourceKind::Nexus,
                "nexus_child",
                "".to_string(),
            )),
        }
    }
}

impl TryFrom<nexus::NexusNvmfConfig> for NexusNvmfConfig {
    type Error = ReplyError;
    fn try_from(data: nexus::NexusNvmfConfig) -> Result<Self, Self::Error> {
        Ok(NexusNvmfConfig::new(
            match data.controller_id_range {
                Some(range) => NvmfControllerIdRange::try_from(range)?,
                None => {
                    return Err(ReplyError::invalid_argument(
                        ResourceKind::Nexus,
                        "nexus_nvmf_config.controller_id_range",
                        "".to_string(),
                    ))
                }
            },
            data.reservation_key,
            {
                match data
                    .reservation_type
                    .and_then(nexus::NvmeReservation::from_i32)
                {
                    None => {
                        return Err(ReplyError::invalid_argument(
                            ResourceKind::Nexus,
                            "nexus_nvmf_config.reservation_type",
                            format!("{:?}", data.reservation_type),
                        ))
                    }
                    Some(s) => s.into(),
                }
            },
            match nexus::NexusNvmePreemption::from_i32(data.preempt_policy) {
                None => {
                    return Err(ReplyError::invalid_argument(
                        ResourceKind::Nexus,
                        "nexus_nvmf_config.preempt_policy",
                        format!("{:?}", data.preempt_policy),
                    ))
                }
                Some(v) => match v {
                    nexus::NexusNvmePreemption::ArgKey => {
                        NexusNvmePreemption::ArgKey(data.preempt_reservation_key)
                    }
                    nexus::NexusNvmePreemption::Holder => NexusNvmePreemption::Holder,
                },
            },
        ))
    }
}
impl From<nexus::NvmeReservation> for NvmeReservation {
    fn from(src: nexus::NvmeReservation) -> Self {
        match src {
            nexus::NvmeReservation::Reserved => Self::Reserved,
            nexus::NvmeReservation::WriteExclusive => Self::WriteExclusive,
            nexus::NvmeReservation::ExclusiveAccess => Self::ExclusiveAccess,
            nexus::NvmeReservation::WriteExclusiveRegsOnly => Self::WriteExclusiveRegsOnly,
            nexus::NvmeReservation::ExclusiveAccessRegsOnly => Self::ExclusiveAccessRegsOnly,
            nexus::NvmeReservation::WriteExclusiveAllRegs => Self::WriteExclusiveAllRegs,
            nexus::NvmeReservation::ExclusiveAccessAllRegs => Self::ExclusiveAccessAllRegs,
        }
    }
}

impl TryFrom<nexus::NvmfControllerIdRange> for NvmfControllerIdRange {
    type Error = ReplyError;
    fn try_from(value: nexus::NvmfControllerIdRange) -> Result<Self, Self::Error> {
        NvmfControllerIdRange::new(u16::try_from(value.start)?, u16::try_from(value.end)?)
    }
}

impl From<NexusNvmfConfig> for nexus::NexusNvmfConfig {
    fn from(data: NexusNvmfConfig) -> Self {
        Self {
            controller_id_range: Some(data.controller_id_range().into()),
            reservation_key: data.resv_key(),
            preempt_reservation_key: data.preempt_key_opt(),
            reservation_type: Some(data.resv_type() as i32),
            preempt_policy: match data.preempt_policy() {
                NexusNvmePreemption::ArgKey(_) => nexus::NexusNvmePreemption::ArgKey as i32,
                NexusNvmePreemption::Holder => nexus::NexusNvmePreemption::Holder as i32,
            },
        }
    }
}

impl From<NvmfControllerIdRange> for nexus::NvmfControllerIdRange {
    fn from(data: NvmfControllerIdRange) -> Self {
        Self {
            start: *data.min() as u32,
            end: *data.max() as u32,
        }
    }
}

/// CreateNexusInfo trait for the nexus creation to be implemented by entities which want to
/// use this operation
pub trait CreateNexusInfo: Send + Sync + std::fmt::Debug {
    /// id of the io-engine instance
    fn node(&self) -> NodeId;
    /// the nexus uuid will be set to this
    fn uuid(&self) -> NexusId;
    /// size of the device in bytes
    fn size(&self) -> u64;
    /// replica can be iscsi and nvmf remote targets or a local spdk bdev
    /// (i.e. bdev:///name-of-the-bdev).
    ///
    /// uris to the targets we connect to
    fn children(&self) -> Vec<NexusChild>;
    /// Managed by our control plane
    fn managed(&self) -> bool;
    /// Volume which owns this nexus, if any
    fn owner(&self) -> Option<VolumeId>;
    /// Nexus Nvmf Configuration
    fn config(&self) -> Option<NexusNvmfConfig>;
}

/// Intermediate structure that validates the conversion to CreateNexusRequest type
#[derive(Debug)]
pub struct ValidatedCreateNexusRequest {
    inner: CreateNexusRequest,
    uuid: NexusId,
    children: Vec<NexusChild>,
    owner: Option<VolumeId>,
    config: Option<NexusNvmfConfig>,
}

impl CreateNexusInfo for CreateNexus {
    fn node(&self) -> NodeId {
        self.node.clone()
    }

    fn uuid(&self) -> NexusId {
        self.uuid.clone()
    }

    fn size(&self) -> u64 {
        self.size
    }

    fn children(&self) -> Vec<NexusChild> {
        self.children.clone()
    }

    fn managed(&self) -> bool {
        self.managed
    }

    fn owner(&self) -> Option<VolumeId> {
        self.owner.clone()
    }

    fn config(&self) -> Option<NexusNvmfConfig> {
        self.config.clone()
    }
}

impl CreateNexusInfo for ValidatedCreateNexusRequest {
    fn node(&self) -> NodeId {
        self.inner.node_id.clone().into()
    }

    fn uuid(&self) -> NexusId {
        self.uuid.clone()
    }

    fn size(&self) -> u64 {
        self.inner.size
    }

    fn children(&self) -> Vec<NexusChild> {
        self.children.clone()
    }

    fn managed(&self) -> bool {
        self.inner.managed
    }

    fn owner(&self) -> Option<VolumeId> {
        self.owner.clone()
    }

    fn config(&self) -> Option<NexusNvmfConfig> {
        self.config.clone()
    }
}

impl ValidateRequestTypes for CreateNexusRequest {
    type Validated = ValidatedCreateNexusRequest;

    fn validated(self) -> Result<Self::Validated, ReplyError> {
        Ok(ValidatedCreateNexusRequest {
            uuid: NexusId::try_from(StringValue(self.nexus_id.clone()))?,
            children: {
                let mut children = Vec::with_capacity(self.children.len());
                for child in self.children.clone() {
                    let x = NexusChild::try_from(child)?;
                    children.push(x)
                }
                children
            },
            owner: match self.owner.clone() {
                Some(owner) => Some(VolumeId::try_from(StringValue(Some(owner)))?),
                None => None,
            },
            config: match self.config.clone() {
                Some(config) => Some(NexusNvmfConfig::try_from(config)?),
                None => None,
            },
            inner: self,
        })
    }
}

impl From<&dyn CreateNexusInfo> for CreateNexus {
    fn from(data: &dyn CreateNexusInfo) -> Self {
        Self {
            node: data.node(),
            uuid: data.uuid(),
            size: data.size(),
            children: data.children(),
            managed: data.managed(),
            owner: data.owner(),
            config: data.config(),
        }
    }
}

impl From<&dyn CreateNexusInfo> for CreateNexusRequest {
    fn from(data: &dyn CreateNexusInfo) -> Self {
        Self {
            node_id: data.node().to_string(),
            nexus_id: Some(data.uuid().to_string()),
            size: data.size(),
            children: data
                .children()
                .into_iter()
                .map(|child| child.into())
                .collect(),
            managed: data.managed(),
            owner: data.owner().map(|owner| owner.to_string()),
            config: data.config().map(|config| config.into()),
        }
    }
}

impl TryFrom<get_nexuses_request::Filter> for Filter {
    type Error = ReplyError;
    fn try_from(filter: get_nexuses_request::Filter) -> Result<Self, Self::Error> {
        match filter {
            get_nexuses_request::Filter::Node(node_filter) => {
                Ok(Filter::Node(node_filter.node_id.into()))
            }
            get_nexuses_request::Filter::NodeNexus(node_nexus_filter) => Ok(Filter::NodeNexus(
                node_nexus_filter.node_id.into(),
                match NexusId::try_from(node_nexus_filter.nexus_id) {
                    Ok(nexus_id) => nexus_id,
                    Err(err) => {
                        return Err(ReplyError::invalid_argument(
                            ResourceKind::Nexus,
                            "nexus_filter::node_nexus.nexus_id",
                            err.to_string(),
                        ))
                    }
                },
            )),
            get_nexuses_request::Filter::Nexus(nexus_filter) => Ok(Filter::Nexus(
                match NexusId::try_from(nexus_filter.nexus_id) {
                    Ok(nexus_id) => nexus_id,
                    Err(err) => {
                        return Err(ReplyError::invalid_argument(
                            ResourceKind::Nexus,
                            "nexus_filter::nexus.nexus_id",
                            err.to_string(),
                        ))
                    }
                },
            )),
        }
    }
}

/// DestroyNexusInfo trait for the nexus deletion to be implemented by entities which want to
/// use this operation
pub trait DestroyNexusInfo: Send + Sync + std::fmt::Debug {
    /// Id of the IoEngine instance
    fn node(&self) -> NodeId;
    /// Uuid of the nexus
    fn uuid(&self) -> NexusId;
}

impl DestroyNexusInfo for DestroyNexus {
    fn node(&self) -> NodeId {
        self.node.clone()
    }

    fn uuid(&self) -> NexusId {
        self.uuid.clone()
    }
}

/// Intermediate structure that validates the conversion to DestroyNexusRequest type
#[derive(Debug)]
pub struct ValidatedDestroyNexusRequest {
    inner: DestroyNexusRequest,
    uuid: NexusId,
}

impl DestroyNexusInfo for ValidatedDestroyNexusRequest {
    fn node(&self) -> NodeId {
        self.inner.node_id.clone().into()
    }

    fn uuid(&self) -> NexusId {
        self.uuid.clone()
    }
}

impl ValidateRequestTypes for DestroyNexusRequest {
    type Validated = ValidatedDestroyNexusRequest;
    fn validated(self) -> Result<Self::Validated, ReplyError> {
        Ok(ValidatedDestroyNexusRequest {
            uuid: NexusId::try_from(StringValue(self.nexus_id.clone()))?,
            inner: self,
        })
    }
}

impl From<&dyn DestroyNexusInfo> for DestroyNexusRequest {
    fn from(data: &dyn DestroyNexusInfo) -> Self {
        Self {
            node_id: data.node().to_string(),
            nexus_id: Some(data.uuid().to_string()),
        }
    }
}

impl From<&dyn DestroyNexusInfo> for DestroyNexus {
    fn from(data: &dyn DestroyNexusInfo) -> Self {
        Self::new(data.node(), data.uuid())
    }
}

/// ShareNexusInfo trait for the nexus sharing to be implemented by entities which want to avail
/// this operation
pub trait ShareNexusInfo: Send + Sync + std::fmt::Debug {
    /// Id of the IoEngine instance
    fn node(&self) -> NodeId;
    /// Uuid of the nexus
    fn uuid(&self) -> NexusId;
    /// encryption key
    fn key(&self) -> Option<String>;
    /// Protocol used for exposing the nexus
    fn protocol(&self) -> NexusShareProtocol;
    /// Allowed hosts to access nexus.
    fn allowed_hosts(&self) -> Vec<HostNqn>;
}

impl ShareNexusInfo for ShareNexus {
    fn node(&self) -> NodeId {
        self.node.clone()
    }

    fn uuid(&self) -> NexusId {
        self.uuid.clone()
    }

    fn key(&self) -> Option<String> {
        self.key.clone()
    }

    fn protocol(&self) -> NexusShareProtocol {
        self.protocol
    }

    fn allowed_hosts(&self) -> Vec<HostNqn> {
        self.allowed_hosts.clone()
    }
}

impl From<nexus::NexusShareProtocol> for NexusShareProtocol {
    fn from(src: nexus::NexusShareProtocol) -> Self {
        match src {
            nexus::NexusShareProtocol::Nvmf => Self::Nvmf,
            nexus::NexusShareProtocol::Iscsi => Self::Iscsi,
        }
    }
}

impl From<NexusShareProtocol> for nexus::NexusShareProtocol {
    fn from(src: NexusShareProtocol) -> Self {
        match src {
            NexusShareProtocol::Nvmf => Self::Nvmf,
            NexusShareProtocol::Iscsi => Self::Iscsi,
        }
    }
}

/// Intermediate structure that validates the conversion to ShareNexusRequest type
#[derive(Debug)]
pub struct ValidatedShareNexusRequest {
    inner: ShareNexusRequest,
    uuid: NexusId,
    protocol: NexusShareProtocol,
    allowed_hosts: Vec<HostNqn>,
}

impl ShareNexusInfo for ValidatedShareNexusRequest {
    fn node(&self) -> NodeId {
        self.inner.node_id.clone().into()
    }

    fn protocol(&self) -> NexusShareProtocol {
        self.protocol
    }

    fn key(&self) -> Option<String> {
        self.inner.key.clone()
    }

    fn uuid(&self) -> NexusId {
        self.uuid.clone()
    }

    fn allowed_hosts(&self) -> Vec<HostNqn> {
        self.allowed_hosts.clone()
    }
}

impl ValidateRequestTypes for ShareNexusRequest {
    type Validated = ValidatedShareNexusRequest;
    fn validated(self) -> Result<Self::Validated, ReplyError> {
        Ok(ValidatedShareNexusRequest {
            uuid: NexusId::try_from(StringValue(self.nexus_id.clone()))?,
            protocol: match nexus::NexusShareProtocol::from_i32(self.protocol) {
                Some(protocol) => protocol.into(),
                None => {
                    return Err(ReplyError::invalid_argument(
                        ResourceKind::Nexus,
                        "share_nexus_request.protocol",
                        "".to_string(),
                    ))
                }
            },
            inner: self.clone(),
            allowed_hosts: self
                .allowed_hosts
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        })
    }
}

impl From<&dyn ShareNexusInfo> for ShareNexusRequest {
    fn from(data: &dyn ShareNexusInfo) -> Self {
        let protocol: nexus::NexusShareProtocol = data.protocol().into();
        Self {
            node_id: data.node().to_string(),
            nexus_id: Some(data.uuid().to_string()),
            protocol: protocol as i32,
            key: data.key(),
            allowed_hosts: data
                .allowed_hosts()
                .into_iter()
                .map(|nqn| nqn.to_string())
                .collect(),
        }
    }
}

impl From<&dyn ShareNexusInfo> for ShareNexus {
    fn from(data: &dyn ShareNexusInfo) -> Self {
        Self {
            node: data.node(),
            uuid: data.uuid(),
            key: data.key(),
            protocol: data.protocol(),
            allowed_hosts: data.allowed_hosts(),
        }
    }
}

/// UnshareNexusInfo trait for the nexus unsharing to be implemented by entities which want to avail
/// this operation
pub trait UnshareNexusInfo: Send + Sync + std::fmt::Debug {
    /// Id of the IoEngine instance
    fn node(&self) -> NodeId;
    /// Uuid of the nexus
    fn uuid(&self) -> NexusId;
}

impl UnshareNexusInfo for UnshareNexus {
    fn node(&self) -> NodeId {
        self.node.clone()
    }

    fn uuid(&self) -> NexusId {
        self.uuid.clone()
    }
}

/// Intermediate structure that validates the conversion to UnshareNexusRequest type
#[derive(Debug)]
pub struct ValidatedUnshareNexusRequest {
    inner: UnshareNexusRequest,
    uuid: NexusId,
}

impl UnshareNexusInfo for ValidatedUnshareNexusRequest {
    fn node(&self) -> NodeId {
        self.inner.node_id.clone().into()
    }

    fn uuid(&self) -> NexusId {
        self.uuid.clone()
    }
}

impl ValidateRequestTypes for UnshareNexusRequest {
    type Validated = ValidatedUnshareNexusRequest;
    fn validated(self) -> Result<Self::Validated, ReplyError> {
        Ok(ValidatedUnshareNexusRequest {
            uuid: NexusId::try_from(StringValue(self.nexus_id.clone()))?,
            inner: self,
        })
    }
}

impl From<&dyn UnshareNexusInfo> for UnshareNexusRequest {
    fn from(data: &dyn UnshareNexusInfo) -> Self {
        Self {
            node_id: data.node().to_string(),
            nexus_id: Some(data.uuid().to_string()),
        }
    }
}

impl From<&dyn UnshareNexusInfo> for UnshareNexus {
    fn from(data: &dyn UnshareNexusInfo) -> Self {
        Self {
            node: data.node(),
            uuid: data.uuid(),
        }
    }
}

/// AddNexusChildInfo trait for the add nexus child to be implemented by entities which want to
/// use this operation
pub trait AddNexusChildInfo: Send + Sync + std::fmt::Debug {
    /// id of the io-engine instance
    fn node(&self) -> NodeId;
    /// uuid of the nexus
    fn nexus(&self) -> NexusId;
    /// URI of the child device to be added
    fn uri(&self) -> ChildUri;
    /// auto start rebuilding
    fn auto_rebuild(&self) -> bool;
}

impl AddNexusChildInfo for AddNexusChild {
    fn node(&self) -> NodeId {
        self.node.clone()
    }

    fn nexus(&self) -> NexusId {
        self.nexus.clone()
    }

    fn uri(&self) -> ChildUri {
        self.uri.clone()
    }

    fn auto_rebuild(&self) -> bool {
        self.auto_rebuild
    }
}

/// Intermediate structure that validates the conversion to AddNexusChildRequest type
#[derive(Debug)]
pub struct ValidatedAddNexusChildRequest {
    inner: AddNexusChildRequest,
    nexus: NexusId,
}

impl AddNexusChildInfo for ValidatedAddNexusChildRequest {
    fn node(&self) -> NodeId {
        self.inner.node_id.clone().into()
    }

    fn nexus(&self) -> NexusId {
        self.nexus.clone()
    }

    fn uri(&self) -> ChildUri {
        ChildUri::from(self.inner.uri.clone())
    }

    fn auto_rebuild(&self) -> bool {
        self.inner.auto_rebuild
    }
}

impl ValidateRequestTypes for AddNexusChildRequest {
    type Validated = ValidatedAddNexusChildRequest;
    fn validated(self) -> Result<Self::Validated, ReplyError> {
        Ok(ValidatedAddNexusChildRequest {
            nexus: NexusId::try_from(StringValue(self.nexus_id.clone()))?,
            inner: self,
        })
    }
}

impl From<&dyn AddNexusChildInfo> for AddNexusChildRequest {
    fn from(data: &dyn AddNexusChildInfo) -> Self {
        Self {
            node_id: data.node().to_string(),
            nexus_id: Some(data.nexus().to_string()),
            uri: data.uri().to_string(),
            auto_rebuild: data.auto_rebuild(),
        }
    }
}

impl From<&dyn AddNexusChildInfo> for AddNexusChild {
    fn from(data: &dyn AddNexusChildInfo) -> Self {
        Self {
            node: data.node(),
            nexus: data.nexus(),
            uri: data.uri(),
            auto_rebuild: data.auto_rebuild(),
        }
    }
}

/// RemoveNexusChildInfo trait for the remove nexus child to be implemented by entities which want
/// to use this operation
pub trait RemoveNexusChildInfo: Send + Sync + std::fmt::Debug {
    /// id of the io-engine instance
    fn node(&self) -> NodeId;
    /// uuid of the nexus
    fn nexus(&self) -> NexusId;
    /// URI of the child device to be added
    fn uri(&self) -> ChildUri;
}

impl RemoveNexusChildInfo for RemoveNexusChild {
    fn node(&self) -> NodeId {
        self.node.clone()
    }

    fn nexus(&self) -> NexusId {
        self.nexus.clone()
    }

    fn uri(&self) -> ChildUri {
        self.uri.clone()
    }
}

/// Intermediate structure that validates the conversion to RemoveNexusChildRequest type
#[derive(Debug)]
pub struct ValidatedRemoveNexusChildRequest {
    inner: RemoveNexusChildRequest,
    nexus: NexusId,
}

impl RemoveNexusChildInfo for ValidatedRemoveNexusChildRequest {
    fn node(&self) -> NodeId {
        self.inner.node_id.clone().into()
    }

    fn nexus(&self) -> NexusId {
        self.nexus.clone()
    }

    fn uri(&self) -> ChildUri {
        ChildUri::from(self.inner.uri.clone())
    }
}

impl ValidateRequestTypes for RemoveNexusChildRequest {
    type Validated = ValidatedRemoveNexusChildRequest;
    fn validated(self) -> Result<Self::Validated, ReplyError> {
        Ok(ValidatedRemoveNexusChildRequest {
            nexus: NexusId::try_from(StringValue(self.nexus_id.clone()))?,
            inner: self,
        })
    }
}

impl From<&dyn RemoveNexusChildInfo> for RemoveNexusChildRequest {
    fn from(data: &dyn RemoveNexusChildInfo) -> Self {
        Self {
            node_id: data.node().to_string(),
            nexus_id: Some(data.nexus().to_string()),
            uri: data.uri().to_string(),
        }
    }
}

impl From<&dyn RemoveNexusChildInfo> for RemoveNexusChild {
    fn from(data: &dyn RemoveNexusChildInfo) -> Self {
        Self {
            node: data.node(),
            nexus: data.nexus(),
            uri: data.uri(),
        }
    }
}
