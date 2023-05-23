use super::translation::{rpc_replica_to_agent, AgentToIoEngine};
use crate::controller::io_engine::translation::TryIoEngineToAgent;
use agents::errors::{GrpcRequest as GrpcRequestError, SvcError};
use rpc::v1::replica::{DeleteReplicaSnapshotRequest, ListReplicaOptions};
use stor_port::{
    transport_api::ResourceKind,
    types::v0::transport::{
        CreateReplica, CreateReplicaSnapshot, DestroyReplica, DestroyReplicaSnapshot, NodeId,
        Replica, ReplicaSnapshot, ShareReplica, UnshareReplica,
    },
};

use snafu::ResultExt;

#[async_trait::async_trait]
impl crate::controller::io_engine::ReplicaListApi for super::RpcClient {
    async fn list_replicas(&self, id: &NodeId) -> Result<Vec<Replica>, SvcError> {
        let rpc_replicas = self
            .replica()
            .list_replicas(ListReplicaOptions::default())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Replica,
                request: "list_replicas",
            })?;

        let rpc_replicas = &rpc_replicas.get_ref().replicas;

        let replicas = rpc_replicas
            .iter()
            .filter_map(|p| match rpc_replica_to_agent(p, id) {
                Ok(r) => Some(r),
                Err(error) => {
                    tracing::error!(error=%error, "Could not convert rpc replica");
                    None
                }
            })
            .collect();

        Ok(replicas)
    }
}

#[async_trait::async_trait]
impl crate::controller::io_engine::ReplicaApi for super::RpcClient {
    async fn create_replica(&self, request: &CreateReplica) -> Result<Replica, SvcError> {
        let rpc_replica = self
            .replica()
            .create_replica(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Replica,
                request: "create_replica",
            })?;
        let replica = rpc_replica_to_agent(&rpc_replica.into_inner(), &request.node)?;
        Ok(replica)
    }

    #[tracing::instrument(name = "rpc::v1::replica::destroy", level = "debug", skip(self), err)]
    async fn destroy_replica(&self, request: &DestroyReplica) -> Result<(), SvcError> {
        let _ = self
            .replica()
            .destroy_replica(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Replica,
                request: "destroy_replica",
            })?;
        Ok(())
    }

    async fn share_replica(&self, request: &ShareReplica) -> Result<String, SvcError> {
        let uri = self
            .replica()
            .share_replica(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Replica,
                request: "share_replica",
            })?
            .into_inner()
            .uri;
        Ok(uri)
    }

    async fn unshare_replica(&self, request: &UnshareReplica) -> Result<String, SvcError> {
        let uri = self
            .replica()
            .unshare_replica(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Replica,
                request: "unshare_replica",
            })?
            .into_inner()
            .uri;
        Ok(uri)
    }
}

#[async_trait::async_trait]
impl crate::controller::io_engine::ReplicaSnapshotApi for super::RpcClient {
    async fn create_repl_snapshot(
        &self,
        request: &CreateReplicaSnapshot,
    ) -> Result<ReplicaSnapshot, SvcError> {
        let response = self
            .replica()
            .create_replica_snapshot(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Replica,
                request: "create_snapshot",
            })?;
        response.into_inner().try_to_agent()
    }

    async fn destroy_repl_snapshot(
        &self,
        request: &DestroyReplicaSnapshot,
    ) -> Result<(), SvcError> {
        self.replica()
            .delete_replica_snapshot(DeleteReplicaSnapshotRequest {
                snapshot_uuid: request.snap_id.to_string(),
            })
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Replica,
                request: "delete_snapshot",
            })?;

        Ok(())
    }
}
