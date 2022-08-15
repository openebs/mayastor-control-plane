//! Converts rpc messages to message bus messages and vice versa.

use crate::errors::SvcError;
use common_lib::{
    transport_api::ResourceKind,
    types::v0::{
        openapi::apis::IntoVec,
        transport::{
            self, ChildState, NexusId, NexusStatus, Protocol, ReplicaId, ReplicaName, ReplicaStatus,
        },
    },
};

use crate::msg_translation::{AgentToIoEngine, IoEngineToAgent, TryIoEngineToAgent};
use rpc::io_engine as v0_rpc;
use std::convert::TryFrom;

impl IoEngineToAgent for v0_rpc::block_device::Partition {
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

impl IoEngineToAgent for v0_rpc::block_device::Filesystem {
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

impl IoEngineToAgent for v0_rpc::BlockDevice {
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

///  Pool Agent conversions

impl IoEngineToAgent for v0_rpc::Pool {
    type AgentMessage = transport::PoolState;
    fn to_agent(&self) -> Self::AgentMessage {
        Self::AgentMessage {
            node: Default::default(),
            id: self.name.clone().into(),
            disks: self.disks.clone().into_vec(),
            status: self.state.into(),
            capacity: self.capacity,
            used: self.used,
        }
    }
}

impl TryIoEngineToAgent for v0_rpc::ReplicaV2 {
    type AgentMessage = transport::Replica;
    fn try_to_agent(&self) -> Result<Self::AgentMessage, SvcError> {
        Ok(Self::AgentMessage {
            node: Default::default(),
            name: self.name.clone().into(),
            uuid: ReplicaId::try_from(self.uuid.as_str()).map_err(|_| SvcError::InvalidUuid {
                uuid: self.uuid.to_owned(),
                kind: ResourceKind::Replica,
            })?,
            pool: self.pool.clone().into(),
            thin: self.thin,
            size: self.size,
            share: self.share.into(),
            uri: self.uri.clone(),
            status: ReplicaStatus::Online,
        })
    }
}

/// Volume Agent conversions

impl TryIoEngineToAgent for v0_rpc::NexusV2 {
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
impl TryIoEngineToAgent for v0_rpc::Nexus {
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
        })
    }
}

impl IoEngineToAgent for v0_rpc::Child {
    type AgentMessage = transport::Child;

    fn to_agent(&self) -> Self::AgentMessage {
        Self::AgentMessage {
            uri: self.uri.clone().into(),
            state: ChildState::from(self.state),
            rebuild_progress: u8::try_from(self.rebuild_progress).ok(),
        }
    }
}

/// Pool Agent Conversions
impl AgentToIoEngine for transport::CreateReplica {
    type IoEngineMessage = v0_rpc::CreateReplicaRequestV2;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            name: ReplicaName::from_opt_uuid(self.name.as_ref(), &self.uuid).into(),
            uuid: self.uuid.clone().into(),
            pool: self.pool.clone().into(),
            thin: self.thin,
            size: self.size,
            share: self.share as i32,
        }
    }
}

impl AgentToIoEngine for transport::ShareReplica {
    type IoEngineMessage = v0_rpc::ShareReplicaRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            // todo: CAS-1107
            uuid: ReplicaName::from_opt_uuid(self.name.as_ref(), &self.uuid).into(),
            share: self.protocol as i32,
        }
    }
}

impl AgentToIoEngine for transport::UnshareReplica {
    type IoEngineMessage = v0_rpc::ShareReplicaRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            uuid: ReplicaName::from_opt_uuid(self.name.as_ref(), &self.uuid).into(),
            share: Protocol::None as i32,
        }
    }
}

impl AgentToIoEngine for transport::CreatePool {
    type IoEngineMessage = v0_rpc::CreatePoolRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            name: self.id.clone().into(),
            disks: self.disks.iter().map(|d| d.to_string()).collect(),
        }
    }
}

impl AgentToIoEngine for transport::DestroyReplica {
    type IoEngineMessage = v0_rpc::DestroyReplicaRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            uuid: ReplicaName::from_opt_uuid(self.name.as_ref(), &self.uuid).into(),
        }
    }
}

impl AgentToIoEngine for transport::DestroyPool {
    type IoEngineMessage = v0_rpc::DestroyPoolRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            name: self.id.clone().into(),
        }
    }
}

/// Volume Agent Conversions

impl AgentToIoEngine for transport::CreateNexus {
    type IoEngineMessage = v0_rpc::CreateNexusV2Request;
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

impl AgentToIoEngine for transport::ShareNexus {
    type IoEngineMessage = v0_rpc::PublishNexusRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            uuid: self.uuid.clone().into(),
            key: self.key.clone().unwrap_or_default(),
            share: self.protocol as i32,
        }
    }
}

impl AgentToIoEngine for transport::UnshareNexus {
    type IoEngineMessage = v0_rpc::UnpublishNexusRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            uuid: self.uuid.clone().into(),
        }
    }
}

impl AgentToIoEngine for transport::DestroyNexus {
    type IoEngineMessage = v0_rpc::DestroyNexusRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            uuid: self.uuid.clone().into(),
        }
    }
}

impl AgentToIoEngine for transport::AddNexusChild {
    type IoEngineMessage = v0_rpc::AddChildNexusRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            uuid: self.nexus.clone().into(),
            uri: self.uri.clone().into(),
            norebuild: !self.auto_rebuild,
        }
    }
}

impl AgentToIoEngine for transport::RemoveNexusChild {
    type IoEngineMessage = v0_rpc::RemoveChildNexusRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            uuid: self.nexus.clone().into(),
            uri: self.uri.clone().into(),
        }
    }
}
