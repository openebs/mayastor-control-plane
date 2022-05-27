use crate::{common, misc::traits::StringValue, nexus};
use common_lib::{
    mbus_api::{ReplyError, ResourceKind},
    types::v0::{
        message_bus::{
            Child, ChildState, ChildUri, Nexus, NexusId, NexusStatus, ReplicaId, VolumeId,
        },
        store::{
            nexus::{NexusOperation, NexusOperationState, NexusSpec, NexusSpecStatus, ReplicaUri},
            nexus_child::NexusChild,
        },
    },
};
use std::convert::TryFrom;

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

impl From<nexus::NexusStatus> for NexusStatus {
    fn from(src: nexus::NexusStatus) -> Self {
        match src {
            nexus::NexusStatus::Unknown => Self::Unknown,
            nexus::NexusStatus::Online => Self::Online,
            nexus::NexusStatus::Degraded => Self::Degraded,
            nexus::NexusStatus::Faulted => Self::Faulted,
        }
    }
}

impl From<NexusStatus> for nexus::NexusStatus {
    fn from(src: NexusStatus) -> Self {
        match src {
            NexusStatus::Unknown => Self::Unknown,
            NexusStatus::Online => Self::Online,
            NexusStatus::Degraded => Self::Degraded,
            NexusStatus::Faulted => Self::Faulted,
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
        match src {
            ChildState::Unknown => Self::ChildUnknown,
            ChildState::Online => Self::ChildOnline,
            ChildState::Degraded => Self::ChildDegraded,
            ChildState::Faulted => Self::ChildFaulted,
        }
    }
}

impl TryFrom<nexus::Child> for Child {
    type Error = ReplyError;
    fn try_from(child_grpc_type: nexus::Child) -> Result<Self, Self::Error> {
        let child = Child {
            uri: child_grpc_type.uri.into(),
            state: match ChildState::try_from(child_grpc_type.state) {
                Ok(state) => state,
                Err(err) => {
                    return Err(ReplyError::invalid_argument(
                        ResourceKind::Nexus,
                        "child.state",
                        err.to_string(),
                    ))
                }
            },
            rebuild_progress: child_grpc_type.rebuild_progress.map(|i| i as u8),
        };
        Ok(child)
    }
}

impl From<Child> for nexus::Child {
    fn from(child: Child) -> Self {
        let child_state: nexus::ChildState = child.state.into();
        nexus::Child {
            uri: child.uri.to_string(),
            state: child_state as i32,
            rebuild_progress: child.rebuild_progress.map(|i| i.into()),
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
                nexus::nexus_child::Child::Uri(_uri) => NexusChild::Uri(Default::default()),
            }),
            None => Err(ReplyError::invalid_argument(
                ResourceKind::Nexus,
                "nexus_child",
                "".to_string(),
            )),
        }
    }
}
