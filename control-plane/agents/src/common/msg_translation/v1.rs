use crate::{
    errors::SvcError,
    msg_translation::{IoEngineToAgent, TryIoEngineToAgent},
};
use common_lib::{
    transport_api::ResourceKind,
    types::v0::{
        openapi::apis::IntoVec,
        transport::{
            self, Child, ChildState, ChildStateReason, Nexus, NexusId, NexusStatus, NodeId,
            PoolUuid, Protocol, Replica, ReplicaId, ReplicaName, ReplicaStatus,
        },
    },
};
use rpc::v1 as v1_rpc;
use std::convert::TryFrom;

/// Trait for converting agent messages to io-engine messages.
pub trait AgentToIoEngine {
    /// RpcIoEngine message type.
    type IoEngineMessage;
    /// Conversion of agent message to io-engine message.
    fn to_rpc(&self) -> Self::IoEngineMessage;
}

impl IoEngineToAgent for v1_rpc::host::Partition {
    type AgentMessage = transport::Partition;
    fn to_agent(&self) -> Self::AgentMessage {
        Self::AgentMessage {
            parent: self.parent.clone(),
            number: self.number,
            name: self.name.clone(),
            scheme: self.scheme.clone(),
            typeid: self.typeid.clone(),
            uuid: self.uuid.clone(),
        }
    }
}

impl IoEngineToAgent for v1_rpc::host::Filesystem {
    type AgentMessage = transport::Filesystem;
    fn to_agent(&self) -> Self::AgentMessage {
        Self::AgentMessage {
            fstype: self.fstype.clone(),
            label: self.label.clone(),
            uuid: self.uuid.clone(),
            mountpoint: self.mountpoints.get(0).cloned().unwrap_or_default(),
        }
    }
}

impl IoEngineToAgent for v1_rpc::host::BlockDevice {
    type AgentMessage = transport::BlockDevice;
    fn to_agent(&self) -> Self::AgentMessage {
        Self::AgentMessage {
            devname: self.devname.clone(),
            devtype: self.devtype.clone(),
            devmajor: self.devmajor,
            devminor: self.devminor,
            model: self.model.clone(),
            devpath: self.devpath.clone(),
            devlinks: self.devlinks.clone(),
            size: self.size,
            partition: match &self.partition {
                Some(partition) => partition.to_agent(),
                None => transport::Partition {
                    ..Default::default()
                },
            },
            filesystem: match &self.filesystem {
                Some(filesystem) => filesystem.to_agent(),
                None => transport::Filesystem {
                    ..Default::default()
                },
            },
            available: self.available,
        }
    }
}

/// Pool Agent Conversions
impl TryIoEngineToAgent for v1_rpc::replica::Replica {
    type AgentMessage = transport::Replica;
    fn try_to_agent(&self) -> Result<Self::AgentMessage, SvcError> {
        Ok(Self::AgentMessage {
            node: Default::default(),
            name: self.name.clone().into(),
            uuid: ReplicaId::try_from(self.uuid.as_str()).map_err(|_| SvcError::InvalidUuid {
                uuid: self.uuid.to_owned(),
                kind: ResourceKind::Replica,
            })?,
            pool_id: self.poolname.clone().into(),
            pool_uuid: Some(PoolUuid::try_from(self.pooluuid.clone()).map_err(|_| {
                SvcError::InvalidUuid {
                    uuid: self.pooluuid.to_owned(),
                    kind: ResourceKind::Replica,
                }
            })?),
            thin: self.thin,
            size: self.size,
            share: self.share.into(),
            uri: self.uri.clone(),
            status: ReplicaStatus::Online,
        })
    }
}

impl AgentToIoEngine for transport::CreateReplica {
    type IoEngineMessage = v1_rpc::replica::CreateReplicaRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            name: ReplicaName::from_opt_uuid(self.name.as_ref(), &self.uuid).into(),
            uuid: self.uuid.clone().into(),
            pooluuid: match self.pool_uuid.clone() {
                Some(uuid) => uuid.into(),
                // TODO implement a getter function to fetch the uuid of the pool from the given
                //      name
                None => self.pool_id.clone().into(),
            },
            thin: self.thin,
            size: self.size,
            share: self.share as i32,
        }
    }
}

impl AgentToIoEngine for transport::ShareReplica {
    type IoEngineMessage = v1_rpc::replica::ShareReplicaRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            uuid: ReplicaName::from_opt_uuid(self.name.as_ref(), &self.uuid).into(),
            share: self.protocol as i32,
        }
    }
}

impl AgentToIoEngine for transport::UnshareReplica {
    type IoEngineMessage = v1_rpc::replica::UnshareReplicaRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            uuid: ReplicaName::from_opt_uuid(self.name.as_ref(), &self.uuid).into(),
        }
    }
}

impl AgentToIoEngine for transport::DestroyReplica {
    type IoEngineMessage = v1_rpc::replica::DestroyReplicaRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            uuid: ReplicaName::from_opt_uuid(self.name.as_ref(), &self.uuid).into(),
        }
    }
}

/// convert rpc replica to a agent replica
pub fn rpc_replica_to_agent(
    rpc_replica: &v1_rpc::replica::Replica,
    id: &NodeId,
) -> Result<Replica, SvcError> {
    let mut replica = rpc_replica.try_to_agent()?;
    replica.node = id.clone();
    Ok(replica)
}

/// Volume Agent conversions

impl TryIoEngineToAgent for v1_rpc::nexus::Nexus {
    type AgentMessage = transport::Nexus;

    fn try_to_agent(&self) -> Result<Self::AgentMessage, SvcError> {
        Ok(Self::AgentMessage {
            node: Default::default(),
            name: self.name.clone(),
            uuid: NexusId::try_from(self.uuid.as_str()).map_err(|_| SvcError::InvalidUuid {
                uuid: self.uuid.to_owned(),
                kind: ResourceKind::Nexus,
            })?,
            size: self.size,
            status: NexusStatus::from(self.state),
            children: self.children.iter().map(|c| c.to_agent()).collect(),
            device_uri: self.device_uri.clone(),
            rebuilds: self.rebuilds,
            // todo: do we need an "other" Protocol variant in case we don't recognise it?
            share: Protocol::try_from(self.device_uri.as_str()).unwrap_or(Protocol::None),
        })
    }
}

/// New-type wrapper for external types.
/// Allows us to convert from external types which would otherwise not be allowed.
struct ExternalType<T>(T);
impl From<ExternalType<v1_rpc::nexus::ChildState>> for ChildState {
    fn from(src: ExternalType<v1_rpc::nexus::ChildState>) -> Self {
        match src.0 {
            v1_rpc::nexus::ChildState::Unknown => ChildState::Unknown,
            v1_rpc::nexus::ChildState::Online => ChildState::Online,
            v1_rpc::nexus::ChildState::Degraded => ChildState::Degraded,
            v1_rpc::nexus::ChildState::Faulted => ChildState::Faulted,
        }
    }
}
impl From<ExternalType<v1_rpc::nexus::ChildStateReason>> for ChildStateReason {
    fn from(src: ExternalType<v1_rpc::nexus::ChildStateReason>) -> Self {
        match src.0 {
            v1_rpc::nexus::ChildStateReason::None => Self::Unknown,
            v1_rpc::nexus::ChildStateReason::Init => Self::Init,
            v1_rpc::nexus::ChildStateReason::Closed => Self::Closed,
            v1_rpc::nexus::ChildStateReason::CannotOpen => Self::CantOpen,
            v1_rpc::nexus::ChildStateReason::ConfigInvalid => Self::ConfigInvalid,
            v1_rpc::nexus::ChildStateReason::RebuildFailed => Self::RebuildFailed,
            v1_rpc::nexus::ChildStateReason::IoFailure => Self::IoError,
            v1_rpc::nexus::ChildStateReason::ByClient => Self::ByClient,
            v1_rpc::nexus::ChildStateReason::OutOfSync => Self::OutOfSync,
            v1_rpc::nexus::ChildStateReason::NoSpace => Self::NoSpace,
            v1_rpc::nexus::ChildStateReason::TimedOut => Self::TimedOut,
            v1_rpc::nexus::ChildStateReason::AdminFailed => Self::AdminCommandFailed,
        }
    }
}

impl IoEngineToAgent for v1_rpc::nexus::Child {
    type AgentMessage = transport::Child;

    fn to_agent(&self) -> Self::AgentMessage {
        Self::AgentMessage {
            uri: self.uri.clone().into(),
            state: ChildState::from(ExternalType(
                v1_rpc::nexus::ChildState::from_i32(self.state)
                    .unwrap_or(v1_rpc::nexus::ChildState::Unknown),
            )),
            rebuild_progress: u8::try_from(self.rebuild_progress).ok(),
            state_reason: v1_rpc::nexus::ChildStateReason::from_i32(self.state_reason)
                .map(|f| From::from(ExternalType(f)))
                .unwrap_or(ChildStateReason::Unknown),
        }
    }
}

/// Volume Agent Conversions

impl AgentToIoEngine for transport::CreateNexus {
    type IoEngineMessage = v1_rpc::nexus::CreateNexusRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        let nexus_config = self.config.clone().unwrap_or_default();
        Self::IoEngineMessage {
            name: self.name(),
            uuid: self.uuid.clone().into(),
            size: self.size,
            min_cntl_id: nexus_config.min_cntl_id() as u32,
            max_cntl_id: nexus_config.max_cntl_id() as u32,
            resv_key: nexus_config.resv_key(),
            preempt_key: nexus_config.preempt_key(),
            children: self.children.clone().into_vec(),
            nexus_info_key: self.nexus_info_key(),
        }
    }
}

impl AgentToIoEngine for transport::DestroyNexus {
    type IoEngineMessage = v1_rpc::nexus::DestroyNexusRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            uuid: self.uuid.clone().into(),
        }
    }
}

impl AgentToIoEngine for transport::ShareNexus {
    type IoEngineMessage = v1_rpc::nexus::PublishNexusRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            uuid: self.uuid.clone().into(),
            key: self.key.clone().unwrap_or_default(),
            share: self.protocol as i32,
        }
    }
}

impl AgentToIoEngine for transport::UnshareNexus {
    type IoEngineMessage = v1_rpc::nexus::UnpublishNexusRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            uuid: self.uuid.clone().into(),
        }
    }
}

impl AgentToIoEngine for transport::AddNexusChild {
    type IoEngineMessage = v1_rpc::nexus::AddChildNexusRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            uuid: self.nexus.clone().into(),
            uri: self.uri.clone().into(),
            norebuild: !self.auto_rebuild,
        }
    }
}

impl AgentToIoEngine for transport::RemoveNexusChild {
    type IoEngineMessage = v1_rpc::nexus::RemoveChildNexusRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            uuid: self.nexus.clone().into(),
            uri: self.uri.clone().into(),
        }
    }
}

/// convert rpc nexus to a agent nexus
pub fn rpc_nexus_to_agent(
    rpc_nexus: &v1_rpc::nexus::Nexus,
    id: &NodeId,
) -> Result<Nexus, SvcError> {
    let mut nexus = rpc_nexus.try_to_agent()?;
    nexus.node = id.clone();
    Ok(nexus)
}

/// convert rpc nexus to a agent child
pub fn rpc_nexus_to_child_agent(
    rpc_nexus: &v1_rpc::nexus::Nexus,
    child_uri: String,
) -> Result<Child, SvcError> {
    for child in &rpc_nexus.children {
        if child.uri == child_uri {
            return Ok(child.to_agent());
        }
    }
    // This error will ideally not occur
    Err(SvcError::NotFound {
        kind: ResourceKind::Child,
        id: child_uri,
    })
}
