pub(crate) mod client;
/// Message translation to agent types from rpc v0,v1 types.
mod translation;
pub(crate) mod types;
pub(crate) mod v0;
pub(crate) mod v1;

pub(crate) use client::*;

use crate::controller::io_engine::types::{
    CreateNexusSnapshot, CreateNexusSnapshotResp, RebuildHistoryResp,
};
use agents::errors::SvcError;
use stor_port::{
    transport_api::v0::BlockDevices,
    types::v0::transport::{
        AddNexusChild, ApiVersion, CreateNexus, CreatePool, CreateReplica, CreateReplicaSnapshot,
        DestroyNexus, DestroyPool, DestroyReplica, DestroyReplicaSnapshot, FaultNexusChild,
        GetBlockDevices, GetRebuildRecord, ImportPool, IoEngCreateSnapshotClone, ListRebuildRecord,
        ListReplicaSnapshots, ListSnapshotClones, Nexus, NexusChildAction, NexusChildActionContext,
        NexusChildActionKind, NexusId, PoolState, RebuildHistory, Register, RemoveNexusChild,
        Replica, ReplicaId, ReplicaSnapshot, ResizeNexus, ResizeReplica, SetReplicaEntityId,
        ShareNexus, ShareReplica, ShutdownNexus, UnshareNexus, UnshareReplica,
    },
};

use async_trait::async_trait;

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
    + NexusChildActionApi<NexusChildActionContext>
    + HostApi
    + NexusSnapshotApi
    + NexusChildRebuildApi
    + ReplicaSnapshotApi
    + Sync
    + Send
    + Clone
{
    fn api_version(&self) -> ApiVersion;
}

#[async_trait]
pub(crate) trait PoolListApi {
    /// List pools based on api version in context.
    async fn list_pools(&self) -> Result<Vec<PoolState>, SvcError>;
}

#[async_trait]
pub(crate) trait PoolApi {
    /// Create a pool on the node via gRPC.
    async fn create_pool(&self, request: &CreatePool) -> Result<PoolState, SvcError>;
    /// Destroy a pool on the node via gRPC.
    async fn destroy_pool(&self, request: &DestroyPool) -> Result<(), SvcError>;
    /// Import a pool on the node via gRPC.
    async fn import_pool(&self, request: &ImportPool) -> Result<PoolState, SvcError>;
}

#[async_trait]
pub(crate) trait ReplicaListApi {
    /// List all replicas from the node.
    async fn list_replicas(&self) -> Result<Vec<Replica>, SvcError>;

    /// Get the specified replica from the node.
    async fn get_replica(&self, replica_id: &ReplicaId) -> Result<Replica, SvcError>;
}

#[async_trait]
pub(crate) trait ReplicaApi {
    /// Create a replica on the pool via gRPC.
    async fn create_replica(&self, request: &CreateReplica) -> Result<Replica, SvcError>;
    /// Destroy a replica on the pool via gRPC.
    async fn destroy_replica(&self, request: &DestroyReplica) -> Result<(), SvcError>;
    /// Resize a replica on the pool via gRPC.
    async fn resize_replica(&self, request: &ResizeReplica) -> Result<Replica, SvcError>;

    /// Share a replica on the pool via gRPC.
    async fn share_replica(&self, request: &ShareReplica) -> Result<String, SvcError>;
    /// Unshare a replica on the pool via gRPC.
    async fn unshare_replica(&self, request: &UnshareReplica) -> Result<String, SvcError>;

    /// Set replica entity_id via gRPC.
    async fn set_replica_entity_id(
        &self,
        request: &SetReplicaEntityId,
    ) -> Result<Replica, SvcError>;
}

#[async_trait]
pub(crate) trait NexusListApi {
    /// List all nexuses from the node.
    async fn list_nexuses(&self) -> Result<Vec<Nexus>, SvcError>;
    /// Get the specified nexus from the node.
    async fn get_nexus(&self, nexus_id: &NexusId) -> Result<Nexus, SvcError>;
}

#[async_trait]
pub(crate) trait NexusApi<Sht> {
    /// Create a nexus on a node via gRPC.
    async fn create_nexus(&self, request: &CreateNexus) -> Result<Nexus, SvcError>;
    /// Destroy a nexus on a node via gRPC.
    async fn destroy_nexus(&self, request: &DestroyNexus) -> Result<(), SvcError>;
    /// Resize a nexus via gRPC.
    async fn resize_nexus(&self, request: &ResizeNexus) -> Result<Nexus, SvcError>;
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
pub(crate) trait NexusChildActionApi<Ctx>
where
    for<'async_trait> Ctx: Send + Sync + 'async_trait,
{
    /// Execute a child action within its parent nexus via gRPC.
    async fn child_action(&self, request: NexusChildAction<Ctx>) -> Result<Nexus, SvcError>;

    /// Online a child within its parent nexus via gRPC.
    async fn online_child(&self, request: Ctx) -> Result<Nexus, SvcError> {
        self.child_action(NexusChildAction::new(request, NexusChildActionKind::Online))
            .await
    }

    /// Offline a child within its parent nexus via gRPC.
    async fn offline_child(&self, request: Ctx) -> Result<Nexus, SvcError> {
        self.child_action(NexusChildAction::new(
            request,
            NexusChildActionKind::Offline,
        ))
        .await
    }
}

/// The trait for nexus snapshot operations like create, remove, list.
#[async_trait]
pub(crate) trait NexusSnapshotApi {
    /// Create a snapshot using the incoming request.
    async fn create_nexus_snapshot(
        &self,
        request: &CreateNexusSnapshot,
    ) -> Result<CreateNexusSnapshotResp, SvcError>;
}

#[async_trait]
pub(crate) trait NexusChildRebuildApi {
    /// Lists rebuild history for a given nexus.
    async fn get_rebuild_history(
        &self,
        request: &GetRebuildRecord,
    ) -> Result<RebuildHistory, SvcError>;

    async fn list_rebuild_record(
        &self,
        request: &ListRebuildRecord,
    ) -> Result<RebuildHistoryResp, SvcError>;
}

/// The trait for replica snapshot operations like create, remove, list.
#[async_trait]
pub(crate) trait ReplicaSnapshotApi {
    /// Create a snapshot using the incoming request.
    async fn create_repl_snapshot(
        &self,
        request: &CreateReplicaSnapshot,
    ) -> Result<ReplicaSnapshot, SvcError>;

    /// Remove a snapshot using the incoming id.
    async fn destroy_repl_snapshot(&self, request: &DestroyReplicaSnapshot)
        -> Result<(), SvcError>;

    /// List snapshots for a replica.
    async fn list_repl_snapshots(
        &self,
        request: &ListReplicaSnapshots,
    ) -> Result<Vec<ReplicaSnapshot>, SvcError>;

    /// Create replica clone from the snapshot.
    async fn create_snapshot_clone(
        &self,
        request: &IoEngCreateSnapshotClone,
    ) -> Result<Replica, SvcError>;

    /// List snapshot clones.
    async fn list_snapshot_clones(
        &self,
        request: &ListSnapshotClones,
    ) -> Result<Vec<Replica>, SvcError>;
}

#[async_trait]
pub(crate) trait HostApi {
    /// Probe node for liveness based on api version in context.
    async fn liveness_probe(&self) -> Result<Register, SvcError>;
    /// List blockdevices based on api versions.
    async fn list_blockdevices(&self, request: &GetBlockDevices) -> Result<BlockDevices, SvcError>;
}
