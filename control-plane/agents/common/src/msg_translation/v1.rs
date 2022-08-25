use crate::{
    errors::SvcError,
    msg_translation::{IoEngineToAgent, TryIoEngineToAgent},
};
use common_lib::{
    transport_api::ResourceKind,
    types::v0::transport::{
        self, NodeId, PoolUuid, Replica, ReplicaId, ReplicaName, ReplicaStatus,
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
            // Replica only contains pooluuid.
            // Patch the pool name after this call.
            pool_id: Default::default(),
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
