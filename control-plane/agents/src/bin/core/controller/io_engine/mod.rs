pub(crate) mod client;
/// Message translation to agent types from rpc v0,v1 types.
mod translation;
pub(crate) mod v0;
pub(crate) mod v1;

pub(crate) use client::*;

use agents::errors::SvcError;
use async_trait::async_trait;
use common_lib::{
    transport_api::v0::BlockDevices,
    types::v0::transport::{
        AddNexusChild, CreateNexus, CreatePool, CreateReplica, DestroyNexus, DestroyPool,
        DestroyReplica, FaultNexusChild, GetBlockDevices, Nexus, NexusId, NodeId, PoolState,
        Register, RemoveNexusChild, Replica, ShareNexus, ShareReplica, ShutdownNexus, UnshareNexus,
        UnshareReplica,
    },
};

#[async_trait]
#[dyn_clonable::clonable]
pub(crate) trait NodeApi:
    PoolListApi
    + PoolApi
    + ReplicaListApi
    + ReplicaApi
    + NexusListApi
    + NexusApi<()>
    + NexusShareApi<Nexus, Nexus>
    + NexusChildApi<Nexus, Nexus, ()>
    + HostApi
    + Sync
    + Send
    + Clone
{
}

#[async_trait]
pub(crate) trait PoolListApi {
    /// List pools based on api version in context.
    async fn list_pools(&self, node_id: &NodeId) -> Result<Vec<PoolState>, SvcError>;
}

#[async_trait]
pub(crate) trait PoolApi {
    /// Create a pool on the node via gRPC.
    async fn create_pool(&self, request: &CreatePool) -> Result<PoolState, SvcError>;
    /// Destroy a pool on the node via gRPC.
    async fn destroy_pool(&self, request: &DestroyPool) -> Result<(), SvcError>;
}

#[async_trait]
pub(crate) trait ReplicaListApi {
    /// List replicas based on api version in context.
    async fn list_replicas(&self, node_id: &NodeId) -> Result<Vec<Replica>, SvcError>;
}

#[async_trait]
pub(crate) trait ReplicaApi {
    /// Create a replica on the pool via gRPC.
    async fn create_replica(&self, request: &CreateReplica) -> Result<Replica, SvcError>;
    /// Destroy a replica on the pool via gRPC.
    async fn destroy_replica(&self, request: &DestroyReplica) -> Result<(), SvcError>;

    /// Share a replica on the pool via gRPC.
    async fn share_replica(&self, request: &ShareReplica) -> Result<String, SvcError>;
    /// Unshare a replica on the pool via gRPC.
    async fn unshare_replica(&self, request: &UnshareReplica) -> Result<String, SvcError>;
}

#[async_trait]
pub(crate) trait NexusListApi {
    /// List nexus based on api version in context.
    async fn list_nexuses(&self, node_id: &NodeId) -> Result<Vec<Nexus>, SvcError>;
    /// Get nexus based on api version in context.
    async fn get_nexus(&self, node_id: &NodeId, nexus_id: &NexusId) -> Result<Nexus, SvcError>;
}

#[async_trait]
pub(crate) trait NexusApi<Sht> {
    /// Create a nexus on a node via gRPC.
    async fn create_nexus(&self, request: &CreateNexus) -> Result<Nexus, SvcError>;
    /// Destroy a nexus on a node via gRPC.
    async fn destroy_nexus(&self, request: &DestroyNexus) -> Result<(), SvcError>;

    /// Shutdown a nexus via gRPC.
    async fn shutdown_nexus(&self, request: &ShutdownNexus) -> Result<Sht, SvcError>;
}

#[async_trait]
pub(crate) trait NexusShareApi<Share, Unshare> {
    /// Share a nexus on the node via gRPC.
    async fn share_nexus(&self, request: &ShareNexus) -> Result<Share, SvcError>;
    /// Unshare a nexus on the node via gRPC.
    async fn unshare_nexus(&self, request: &UnshareNexus) -> Result<Unshare, SvcError>;
}

#[async_trait]
pub(crate) trait NexusChildApi<Add, Rm, Flt> {
    /// Add a child to a nexus via gRPC.
    async fn add_child(&self, request: &AddNexusChild) -> Result<Add, SvcError>;
    /// Remove a child from its parent nexus via gRPC.
    async fn remove_child(&self, request: &RemoveNexusChild) -> Result<Rm, SvcError>;
    /// Fault a child from its parent nexus via gRPC.
    async fn fault_child(&self, request: &FaultNexusChild) -> Result<Flt, SvcError>;
}

#[async_trait]
pub(crate) trait HostApi {
    /// Probe node for liveness based on api version in context.
    async fn liveness_probe(&self) -> Result<Register, SvcError>;
    /// List blockdevices based on api versions.
    async fn list_blockdevices(&self, request: &GetBlockDevices) -> Result<BlockDevices, SvcError>;
}
