use super::translation::{rpc_replica_to_agent, AgentToIoEngine};
use agents::errors::{GrpcRequest as GrpcRequestError, SvcError};
use rpc::io_engine::Null;
use stor_port::{
    transport_api::ResourceKind,
    types::v0::transport::{
        CreateReplica, CreateReplicaSnapshot, DestroyReplica, NodeId, Replica, ReplicaSnapshot,
        ShareReplica, UnshareReplica,
    },
};

use snafu::ResultExt;

#[async_trait::async_trait]
impl crate::controller::io_engine::ReplicaListApi for super::RpcClient {
    async fn list_replicas(&self, node_id: &NodeId) -> Result<Vec<Replica>, SvcError> {
        let rpc_replicas =
            self.client()
                .list_replicas_v2(Null {})
                .await
                .context(GrpcRequestError {
                    resource: ResourceKind::Replica,
                    request: "list_replicas",
                })?;

        let rpc_replicas = &rpc_replicas.get_ref().replicas;

        let replicas = rpc_replicas
            .iter()
            .filter_map(|p| match rpc_replica_to_agent(p, node_id) {
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
            .client()
            .create_replica_v2(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Replica,
                request: "create_replica",
            })?;
        let replica = rpc_replica_to_agent(&rpc_replica.into_inner(), &request.node)?;
        Ok(replica)
    }

    async fn destroy_replica(&self, request: &DestroyReplica) -> Result<(), SvcError> {
        let _ = self
            .client()
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
            .client()
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
            .client()
            .share_replica(request.to_rpc())
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
        _request: &CreateReplicaSnapshot,
    ) -> Result<ReplicaSnapshot, SvcError> {
        Err(SvcError::GrpcRequestError {
            resource: ResourceKind::Replica,
            request: "create_snapshot".to_string(),
            source: tonic::Status::unimplemented(""),
        })
    }
}
