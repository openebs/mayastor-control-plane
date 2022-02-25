//! Converts rpc messages to message bus messages and vice versa.

use crate::errors::SvcError;
use common_lib::{
    mbus_api::ResourceKind,
    types::v0::{
        message_bus::{
            self, ChildState, NexusId, NexusStatus, Protocol, ReplicaId, ReplicaName, ReplicaStatus,
        },
        openapi::apis::IntoVec,
    },
};
use rpc::mayastor as rpc;
use std::convert::TryFrom;

/// Trait for converting rpc messages to message bus messages.
pub trait TryRpcToMessageBus {
    /// Message bus message type.
    type BusMessage;
    /// Conversion of rpc message to message bus message.
    fn try_to_mbus(&self) -> Result<Self::BusMessage, SvcError>;
}
/// Trait for converting rpc messages to message bus messages.
pub trait RpcToMessageBus {
    /// Message bus message type.
    type BusMessage;
    /// Conversion of rpc message to message bus message.
    fn to_mbus(&self) -> Self::BusMessage;
}

impl RpcToMessageBus for rpc::block_device::Partition {
    type BusMessage = message_bus::Partition;
    fn to_mbus(&self) -> Self::BusMessage {
        Self::BusMessage {
            parent: self.parent.clone(),
            number: self.number,
            name: self.name.clone(),
            scheme: self.scheme.clone(),
            typeid: self.typeid.clone(),
            uuid: self.uuid.clone(),
        }
    }
}

impl RpcToMessageBus for rpc::block_device::Filesystem {
    type BusMessage = message_bus::Filesystem;
    fn to_mbus(&self) -> Self::BusMessage {
        Self::BusMessage {
            fstype: self.fstype.clone(),
            label: self.label.clone(),
            uuid: self.uuid.clone(),
            mountpoint: self.mountpoint.clone(),
        }
    }
}

/// Node Agent Conversions

impl RpcToMessageBus for rpc::BlockDevice {
    type BusMessage = message_bus::BlockDevice;
    fn to_mbus(&self) -> Self::BusMessage {
        Self::BusMessage {
            devname: self.devname.clone(),
            devtype: self.devtype.clone(),
            devmajor: self.devmajor,
            devminor: self.devminor,
            model: self.model.clone(),
            devpath: self.devpath.clone(),
            devlinks: self.devlinks.clone(),
            size: self.size,
            partition: match &self.partition {
                Some(partition) => partition.to_mbus(),
                None => message_bus::Partition {
                    ..Default::default()
                },
            },
            filesystem: match &self.filesystem {
                Some(filesystem) => filesystem.to_mbus(),
                None => message_bus::Filesystem {
                    ..Default::default()
                },
            },
            available: self.available,
        }
    }
}

///  Pool Agent conversions

impl RpcToMessageBus for rpc::Pool {
    type BusMessage = message_bus::PoolState;
    fn to_mbus(&self) -> Self::BusMessage {
        Self::BusMessage {
            node: Default::default(),
            id: self.name.clone().into(),
            disks: self.disks.clone().into_vec(),
            status: self.state.into(),
            capacity: self.capacity,
            used: self.used,
        }
    }
}

impl TryRpcToMessageBus for rpc::ReplicaV2 {
    type BusMessage = message_bus::Replica;
    fn try_to_mbus(&self) -> Result<Self::BusMessage, SvcError> {
        Ok(Self::BusMessage {
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

impl TryRpcToMessageBus for rpc::NexusV2 {
    type BusMessage = message_bus::Nexus;

    fn try_to_mbus(&self) -> Result<Self::BusMessage, SvcError> {
        Ok(Self::BusMessage {
            node: Default::default(),
            name: self.name.clone(),
            uuid: NexusId::try_from(self.uuid.as_str()).map_err(|_| SvcError::InvalidUuid {
                uuid: self.uuid.to_owned(),
                kind: ResourceKind::Nexus,
            })?,
            size: self.size,
            status: NexusStatus::from(self.state),
            children: self.children.iter().map(|c| c.to_mbus()).collect(),
            device_uri: self.device_uri.clone(),
            rebuilds: self.rebuilds,
            // todo: do we need an "other" Protocol variant in case we don't recognise it?
            share: Protocol::try_from(self.device_uri.as_str()).unwrap_or(Protocol::None),
        })
    }
}
impl TryRpcToMessageBus for rpc::Nexus {
    type BusMessage = message_bus::Nexus;

    fn try_to_mbus(&self) -> Result<Self::BusMessage, SvcError> {
        Ok(Self::BusMessage {
            node: Default::default(),
            // todo: fix CAS-1107
            // CreateNexusV2 returns NexusV1... patch it up after this call...
            name: self.uuid.clone(),
            uuid: Default::default(),
            size: self.size,
            status: NexusStatus::from(self.state),
            children: self.children.iter().map(|c| c.to_mbus()).collect(),
            device_uri: self.device_uri.clone(),
            rebuilds: self.rebuilds,
            // todo: do we need an "other" Protocol variant in case we don't recognise it?
            share: Protocol::try_from(self.device_uri.as_str()).unwrap_or(Protocol::None),
        })
    }
}

impl RpcToMessageBus for rpc::Child {
    type BusMessage = message_bus::Child;

    fn to_mbus(&self) -> Self::BusMessage {
        Self::BusMessage {
            uri: self.uri.clone().into(),
            state: ChildState::from(self.state),
            rebuild_progress: u8::try_from(self.rebuild_progress).ok(),
        }
    }
}

/// Trait for converting message bus messages to rpc messages.
pub trait MessageBusToRpc {
    /// RPC message type.
    type RpcMessage;
    /// Conversion of message bus message to rpc message.
    fn to_rpc(&self) -> Self::RpcMessage;
}

/// Pool Agent Conversions

impl MessageBusToRpc for message_bus::CreateReplica {
    type RpcMessage = rpc::CreateReplicaRequestV2;
    fn to_rpc(&self) -> Self::RpcMessage {
        Self::RpcMessage {
            name: ReplicaName::from_opt_uuid(self.name.as_ref(), &self.uuid).into(),
            uuid: self.uuid.clone().into(),
            pool: self.pool.clone().into(),
            thin: self.thin,
            size: self.size,
            share: self.share as i32,
        }
    }
}

impl MessageBusToRpc for message_bus::ShareReplica {
    type RpcMessage = rpc::ShareReplicaRequest;
    fn to_rpc(&self) -> Self::RpcMessage {
        Self::RpcMessage {
            // todo: CAS-1107
            uuid: ReplicaName::from_opt_uuid(self.name.as_ref(), &self.uuid).into(),
            share: self.protocol as i32,
        }
    }
}

impl MessageBusToRpc for message_bus::UnshareReplica {
    type RpcMessage = rpc::ShareReplicaRequest;
    fn to_rpc(&self) -> Self::RpcMessage {
        Self::RpcMessage {
            uuid: ReplicaName::from_opt_uuid(self.name.as_ref(), &self.uuid).into(),
            share: Protocol::None as i32,
        }
    }
}

impl MessageBusToRpc for message_bus::CreatePool {
    type RpcMessage = rpc::CreatePoolRequest;
    fn to_rpc(&self) -> Self::RpcMessage {
        Self::RpcMessage {
            name: self.id.clone().into(),
            disks: self.disks.iter().map(|d| d.to_string()).collect(),
        }
    }
}

impl MessageBusToRpc for message_bus::DestroyReplica {
    type RpcMessage = rpc::DestroyReplicaRequest;
    fn to_rpc(&self) -> Self::RpcMessage {
        Self::RpcMessage {
            uuid: ReplicaName::from_opt_uuid(self.name.as_ref(), &self.uuid).into(),
        }
    }
}

impl MessageBusToRpc for message_bus::DestroyPool {
    type RpcMessage = rpc::DestroyPoolRequest;
    fn to_rpc(&self) -> Self::RpcMessage {
        Self::RpcMessage {
            name: self.id.clone().into(),
        }
    }
}

/// Volume Agent Conversions

impl MessageBusToRpc for message_bus::CreateNexus {
    type RpcMessage = rpc::CreateNexusV2Request;
    fn to_rpc(&self) -> Self::RpcMessage {
        let nexus_config = self.config.clone().unwrap_or_default();
        Self::RpcMessage {
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

impl MessageBusToRpc for message_bus::ShareNexus {
    type RpcMessage = rpc::PublishNexusRequest;
    fn to_rpc(&self) -> Self::RpcMessage {
        Self::RpcMessage {
            uuid: self.uuid.clone().into(),
            key: self.key.clone().unwrap_or_default(),
            share: self.protocol as i32,
        }
    }
}

impl MessageBusToRpc for message_bus::UnshareNexus {
    type RpcMessage = rpc::UnpublishNexusRequest;
    fn to_rpc(&self) -> Self::RpcMessage {
        Self::RpcMessage {
            uuid: self.uuid.clone().into(),
        }
    }
}

impl MessageBusToRpc for message_bus::DestroyNexus {
    type RpcMessage = rpc::DestroyNexusRequest;
    fn to_rpc(&self) -> Self::RpcMessage {
        Self::RpcMessage {
            uuid: self.uuid.clone().into(),
        }
    }
}

impl MessageBusToRpc for message_bus::AddNexusChild {
    type RpcMessage = rpc::AddChildNexusRequest;
    fn to_rpc(&self) -> Self::RpcMessage {
        Self::RpcMessage {
            uuid: self.nexus.clone().into(),
            uri: self.uri.clone().into(),
            norebuild: !self.auto_rebuild,
        }
    }
}

impl MessageBusToRpc for message_bus::RemoveNexusChild {
    type RpcMessage = rpc::RemoveChildNexusRequest;
    fn to_rpc(&self) -> Self::RpcMessage {
        Self::RpcMessage {
            uuid: self.nexus.clone().into(),
            uri: self.uri.clone().into(),
        }
    }
}
