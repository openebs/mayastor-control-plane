use crate::{common, nexus};
use common_lib::{
    mbus_api::{ReplyError, ResourceKind},
    types::v0::{
        message_bus::{Child, ChildState, Nexus, NexusId, NexusStatus},
        store::nexus::NexusSpecStatus,
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
                Err(_) => return Err(ReplyError::unwrap_err(ResourceKind::Nexus)),
            };
            children.push(child)
        }
        let nexus = Nexus {
            node: nexus_grpc_type.node_id.into(),
            name: nexus_grpc_type.name,
            uuid: match nexus_grpc_type.uuid {
                Some(uuid) => match NexusId::try_from(uuid) {
                    Ok(nexusid) => nexusid,
                    Err(_) => return Err(ReplyError::unwrap_err(ResourceKind::Nexus)),
                },
                None => return Err(ReplyError::unwrap_err(ResourceKind::Nexus)),
            },
            size: nexus_grpc_type.size,
            status: match nexus::NexusStatus::from_i32(nexus_grpc_type.status) {
                Some(status) => status.into(),
                None => return Err(ReplyError::unwrap_err(ResourceKind::Nexus)),
            },
            children,
            device_uri: nexus_grpc_type.device_uri,
            rebuilds: nexus_grpc_type.rebuilds,
            share: match common::Protocol::from_i32(nexus_grpc_type.share) {
                Some(share) => share.into(),
                None => return Err(ReplyError::unwrap_err(ResourceKind::Nexus)),
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
                Err(_) => return Err(ReplyError::unwrap_err(ResourceKind::Nexus)),
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
