//! Converts rpc messages to rpc agent messages and vice versa.

use crate::controller::io_engine::translation::{IoEngineToAgent, TryIoEngineToAgent};
use agents::errors::SvcError;
use rpc::io_engine as v0;
use stor_port::{
    transport_api::ResourceKind,
    types::v0::{
        openapi::apis::IntoVec,
        transport::{
            self, ChildState, ChildStateReason, Nexus, NexusId, NexusNvmePreemption,
            NexusNvmfConfig, NexusStatus, NodeId, NvmeReservation, PoolState, Protocol, Replica,
            ReplicaId, ReplicaName, ReplicaStatus,
        },
    },
};

use std::convert::TryFrom;

/// Trait for converting agent messages to io-engine messages.
pub(super) trait AgentToIoEngine {
    /// RpcIoEngine message type.
    type IoEngineMessage;
    /// Conversion of agent message to io-engine message.
    fn to_rpc(&self) -> Self::IoEngineMessage;
}

impl IoEngineToAgent for v0::block_device::Partition {
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

impl IoEngineToAgent for v0::block_device::Filesystem {
    type AgentMessage = transport::Filesystem;
    fn to_agent(&self) -> Self::AgentMessage {
        Self::AgentMessage {
            fstype: self.fstype.clone(),
            label: self.label.clone(),
            uuid: self.uuid.clone(),
            mountpoint: self.mountpoint.clone(),
        }
    }
}

/// Node Agent Conversions

impl IoEngineToAgent for v0::BlockDevice {
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

/// Pool Agent conversions.
impl IoEngineToAgent for v0::Pool {
    type AgentMessage = transport::PoolState;
    fn to_agent(&self) -> Self::AgentMessage {
        Self::AgentMessage {
            node: Default::default(),
            id: self.name.clone().into(),
            disks: self.disks.clone().into_vec(),
            status: self.state.into(),
            capacity: self.capacity,
            used: self.used,
            committed: None,
        }
    }
}

impl TryIoEngineToAgent for v0::ReplicaV2 {
    type AgentMessage = transport::Replica;
    fn try_to_agent(&self) -> Result<Self::AgentMessage, SvcError> {
        Ok(transport::Replica {
            node: Default::default(),
            name: self.name.clone().into(),
            uuid: ReplicaId::try_from(self.uuid.as_str()).map_err(|_| SvcError::InvalidUuid {
                uuid: self.uuid.to_owned(),
                kind: ResourceKind::Replica,
            })?,
            pool_id: self.pool.clone().into(),
            pool_uuid: None,
            thin: self.thin,
            size: self.size,
            space: None,
            share: self.share.into(),
            uri: self.uri.clone(),
            status: ReplicaStatus::Online,
            allowed_hosts: self
                .allowed_hosts
                .iter()
                .map(|n| {
                    // should we allow for invalid here since it comes directly from the dataplane?
                    transport::HostNqn::try_from(n)
                        .unwrap_or(transport::HostNqn::Invalid { nqn: n.to_string() })
                })
                .collect(),
        })
    }
}

/// Volume Agent conversions

impl TryIoEngineToAgent for v0::NexusV2 {
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
            allowed_hosts: self
                .allowed_hosts
                .iter()
                .map(|n| {
                    // should we allow for invalid here since it comes directly from the dataplane?
                    transport::HostNqn::try_from(n)
                        .unwrap_or(transport::HostNqn::Invalid { nqn: n.to_string() })
                })
                .collect(),
        })
    }
}
impl TryIoEngineToAgent for v0::Nexus {
    type AgentMessage = transport::Nexus;

    fn try_to_agent(&self) -> Result<Self::AgentMessage, SvcError> {
        Ok(Self::AgentMessage {
            node: Default::default(),
            // todo: fix CAS-1107
            // CreateNexusV2 returns NexusV1... patch it up after this call...
            name: self.uuid.clone(),
            uuid: Default::default(),
            size: self.size,
            status: NexusStatus::from(self.state),
            children: self.children.iter().map(|c| c.to_agent()).collect(),
            device_uri: self.device_uri.clone(),
            rebuilds: self.rebuilds,
            // todo: do we need an "other" Protocol variant in case we don't recognise it?
            share: Protocol::try_from(self.device_uri.as_str()).unwrap_or(Protocol::None),
            allowed_hosts: self
                .allowed_hosts
                .iter()
                .map(|n| {
                    // should we allow for invalid here since it comes directly from the dataplane?
                    transport::HostNqn::try_from(n)
                        .unwrap_or(transport::HostNqn::Invalid { nqn: n.to_string() })
                })
                .collect(),
        })
    }
}

/// New-type wrapper for external types.
/// Allows us to convert from external types which would otherwise not be allowed.
struct ExternalType<T>(T);
impl From<ExternalType<v0::ChildState>> for ChildState {
    fn from(src: ExternalType<v0::ChildState>) -> Self {
        match src.0 {
            v0::ChildState::ChildUnknown => ChildState::Unknown,
            v0::ChildState::ChildOnline => ChildState::Online,
            v0::ChildState::ChildDegraded => ChildState::Degraded,
            v0::ChildState::ChildFaulted => ChildState::Faulted,
        }
    }
}
impl From<ExternalType<v0::ChildStateReason>> for ChildStateReason {
    fn from(src: ExternalType<v0::ChildStateReason>) -> Self {
        match src.0 {
            v0::ChildStateReason::None => Self::Unknown,
            v0::ChildStateReason::Init => Self::Init,
            v0::ChildStateReason::Closed => Self::Closed,
            v0::ChildStateReason::CannotOpen => Self::CantOpen,
            v0::ChildStateReason::ConfigInvalid => Self::ConfigInvalid,
            v0::ChildStateReason::RebuildFailed => Self::RebuildFailed,
            v0::ChildStateReason::IoFailure => Self::IoError,
            v0::ChildStateReason::ByClient => Self::ByClient,
            v0::ChildStateReason::OutOfSync => Self::OutOfSync,
            v0::ChildStateReason::NoSpace => Self::NoSpace,
            v0::ChildStateReason::TimedOut => Self::TimedOut,
            v0::ChildStateReason::AdminFailed => Self::AdminCommandFailed,
        }
    }
}

impl IoEngineToAgent for v0::Child {
    type AgentMessage = transport::Child;

    fn to_agent(&self) -> Self::AgentMessage {
        Self::AgentMessage {
            uri: self.uri.clone().into(),
            state: ChildState::from(ExternalType(
                v0::ChildState::from_i32(self.state).unwrap_or(v0::ChildState::ChildUnknown),
            )),
            rebuild_progress: u8::try_from(self.rebuild_progress).ok(),
            state_reason: v0::ChildStateReason::from_i32(self.reason)
                .map(|f| From::from(ExternalType(f)))
                .unwrap_or(ChildStateReason::Unknown),
            faulted_at: None,
        }
    }
}

/// Pool Agent Conversions
impl AgentToIoEngine for transport::CreateReplica {
    type IoEngineMessage = v0::CreateReplicaRequestV2;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            name: ReplicaName::from_opt_uuid(self.name.as_ref(), &self.uuid).into(),
            uuid: self.uuid.clone().into(),
            pool: self.pool_id.clone().into(),
            thin: self.thin,
            size: self.size,
            share: self.share as i32,
            allowed_hosts: self.allowed_hosts.clone().into_vec(),
        }
    }
}

impl AgentToIoEngine for transport::ShareReplica {
    type IoEngineMessage = v0::ShareReplicaRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            // todo: CAS-1107
            uuid: ReplicaName::from_opt_uuid(self.name.as_ref(), &self.uuid).into(),
            share: self.protocol as i32,
            allowed_hosts: self.allowed_hosts.clone().into_vec(),
        }
    }
}

impl AgentToIoEngine for transport::UnshareReplica {
    type IoEngineMessage = v0::ShareReplicaRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            uuid: ReplicaName::from_opt_uuid(self.name.as_ref(), &self.uuid).into(),
            share: Protocol::None as i32,
            ..Default::default()
        }
    }
}

impl AgentToIoEngine for transport::CreatePool {
    type IoEngineMessage = v0::CreatePoolRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            name: self.id.clone().into(),
            disks: self.disks.iter().map(|d| d.to_string()).collect(),
        }
    }
}

impl AgentToIoEngine for transport::DestroyReplica {
    type IoEngineMessage = v0::DestroyReplicaRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            uuid: ReplicaName::from_opt_uuid(self.name.as_ref(), &self.uuid).into(),
        }
    }
}

impl AgentToIoEngine for transport::DestroyPool {
    type IoEngineMessage = v0::DestroyPoolRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            name: self.id.clone().into(),
        }
    }
}

/// Volume Agent Conversions

impl AgentToIoEngine for transport::CreateNexus {
    type IoEngineMessage = v0::CreateNexusV2Request;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        let nexus_config = self
            .config
            .clone()
            .unwrap_or_else(|| NexusNvmfConfig::default().with_no_resv());
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
            resv_type: Some(
                v0::NvmeReservation::from(ExternalType(nexus_config.resv_type())) as i32,
            ),
            preempt_policy: v0::NexusNvmePreemption::from(ExternalType(
                nexus_config.preempt_policy(),
            )) as i32,
        }
    }
}

impl AgentToIoEngine for transport::ShareNexus {
    type IoEngineMessage = v0::PublishNexusRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            uuid: self.uuid.clone().into(),
            key: self.key.clone().unwrap_or_default(),
            share: self.protocol as i32,
            allowed_hosts: self.allowed_hosts.clone().into_vec(),
        }
    }
}

impl AgentToIoEngine for transport::UnshareNexus {
    type IoEngineMessage = v0::UnpublishNexusRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            uuid: self.uuid.clone().into(),
        }
    }
}

impl AgentToIoEngine for transport::DestroyNexus {
    type IoEngineMessage = v0::DestroyNexusRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            uuid: self.uuid.clone().into(),
        }
    }
}

impl AgentToIoEngine for transport::AddNexusChild {
    type IoEngineMessage = v0::AddChildNexusRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            uuid: self.nexus.clone().into(),
            uri: self.uri.clone().into(),
            norebuild: !self.auto_rebuild,
        }
    }
}

impl AgentToIoEngine for transport::RemoveNexusChild {
    type IoEngineMessage = v0::RemoveChildNexusRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            uuid: self.nexus.clone().into(),
            uri: self.uri.clone().into(),
        }
    }
}

impl AgentToIoEngine for transport::FaultNexusChild {
    type IoEngineMessage = v0::FaultNexusChildRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            uuid: self.nexus.clone().into(),
            uri: self.uri.clone().into(),
        }
    }
}

/// Converts Control plane Nexus shutdown struct to IO Engine message
impl AgentToIoEngine for transport::ShutdownNexus {
    type IoEngineMessage = v0::ShutdownNexusRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            uuid: self.uuid().into(),
        }
    }
}

/// convert rpc replica to a agent replica
pub fn rpc_replica_to_agent(rpc_replica: &v0::ReplicaV2, id: &NodeId) -> Result<Replica, SvcError> {
    let mut replica = rpc_replica.try_to_agent()?;
    replica.node = id.clone();
    Ok(replica)
}

/// convert rpc nexus to a agent nexus
pub fn rpc_nexus_to_agent(rpc_nexus: &v0::Nexus, id: &NodeId) -> Result<Nexus, SvcError> {
    let mut nexus = rpc_nexus.try_to_agent()?;
    nexus.node = id.clone();
    Ok(nexus)
}

/// convert rpc nexus v2 to a agent nexus
pub fn rpc_nexus_v2_to_agent(rpc_nexus: &v0::NexusV2, id: &NodeId) -> Result<Nexus, SvcError> {
    let mut nexus = rpc_nexus.try_to_agent()?;
    nexus.node = id.clone();
    Ok(nexus)
}

/// Converts rpc pool to an agent pool.
pub fn rpc_pool_to_agent(rpc_pool: &rpc::io_engine::Pool, id: &NodeId) -> PoolState {
    let mut pool = rpc_pool.to_agent();
    pool.node = id.clone();
    pool
}

impl From<ExternalType<NvmeReservation>> for v0::NvmeReservation {
    fn from(value: ExternalType<NvmeReservation>) -> Self {
        match value.0 {
            NvmeReservation::Reserved => Self::Reserved,
            NvmeReservation::WriteExclusive => Self::WriteExclusive,
            NvmeReservation::ExclusiveAccess => Self::ExclusiveAccess,
            NvmeReservation::WriteExclusiveRegsOnly => Self::WriteExclusiveRegsOnly,
            NvmeReservation::ExclusiveAccessRegsOnly => Self::ExclusiveAccessRegsOnly,
            NvmeReservation::WriteExclusiveAllRegs => Self::WriteExclusiveAllRegs,
            NvmeReservation::ExclusiveAccessAllRegs => Self::ExclusiveAccessAllRegs,
        }
    }
}
impl From<ExternalType<NexusNvmePreemption>> for v0::NexusNvmePreemption {
    fn from(value: ExternalType<NexusNvmePreemption>) -> Self {
        match value.0 {
            NexusNvmePreemption::ArgKey(_) => Self::ArgKey,
            NexusNvmePreemption::Holder => Self::Holder,
        }
    }
}
