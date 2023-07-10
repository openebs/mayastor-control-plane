use super::translation::{rpc_replica_to_agent, AgentToIoEngine};
use crate::controller::io_engine::translation::TryIoEngineToAgent;
use agents::errors::{GrpcRequest as GrpcRequestError, SvcError};
use rpc::v1::{
    replica::ListReplicaOptions,
    snapshot::{destroy_snapshot_request, DestroySnapshotRequest},
};
use stor_port::{
    transport_api::ResourceKind,
    types::v0::transport::{
        CreateReplica, CreateReplicaSnapshot, DestroyReplica, DestroyReplicaSnapshot,
        IoEngCreateSnapshotClone, ListReplicaSnapshots, ListSnapshotClones, Replica, ReplicaId,
        ReplicaSnapshot, ShareReplica, UnshareReplica,
    },
};

use snafu::ResultExt;

#[async_trait::async_trait]
impl crate::controller::io_engine::ReplicaListApi for super::RpcClient {
    async fn list_replicas(&self) -> Result<Vec<Replica>, SvcError> {
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
            .filter_map(|p| match rpc_replica_to_agent(p, self.context.node()) {
                Ok(r) => Some(r),
                Err(error) => {
                    tracing::error!(error=%error, "Could not convert rpc replica");
                    None
                }
            })
            .collect();

        Ok(replicas)
    }

    async fn get_replica(&self, replica_id: &ReplicaId) -> Result<Replica, SvcError> {
        let rpc_replicas = self
            .replica()
            .list_replicas(ListReplicaOptions {
                name: None,
                poolname: None,
                uuid: Some(replica_id.to_string()),
                pooluuid: None,
            })
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Replica,
                request: "list_replicas (uuid)",
            })?;

        match rpc_replicas.get_ref().replicas.get(0) {
            Some(replica) if replica.uuid == replica_id.as_str() => {
                rpc_replica_to_agent(replica, self.context.node())
            }
            _ => Err(SvcError::GrpcRequestError {
                resource: ResourceKind::Replica,
                request: "get_replica".to_string(),
                source: tonic::Status::not_found(""),
            }),
        }
    }
}

#[async_trait::async_trait]
impl crate::controller::io_engine::ReplicaApi for super::RpcClient {
    #[tracing::instrument(name = "rpc::v1::replica::create", level = "debug", skip(self))]
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
    #[tracing::instrument(name = "rpc::v1::replica::snapshot", level = "debug", skip(self))]
    async fn create_repl_snapshot(
        &self,
        request: &CreateReplicaSnapshot,
    ) -> Result<ReplicaSnapshot, SvcError> {
        let response = self
            .snapshot()
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
        self.snapshot()
            .destroy_snapshot(DestroySnapshotRequest {
                snapshot_uuid: request.snap_id.to_string(),
                pool: Some(destroy_snapshot_request::Pool::PoolUuid(
                    request.pool_uuid.to_string(),
                )),
            })
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Replica,
                request: "delete_snapshot",
            })?;

        Ok(())
    }

    async fn list_repl_snapshots(
        &self,
        request: &ListReplicaSnapshots,
    ) -> Result<Vec<ReplicaSnapshot>, SvcError> {
        let response = self
            .snapshot()
            .list_snapshot(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Replica,
                request: "list_snapshots",
            })?;

        let ret = response.into_inner();
        ret.snapshots.iter().map(|s| s.try_to_agent()).collect()
    }

    async fn create_snapshot_clone(
        &self,
        request: &IoEngCreateSnapshotClone,
    ) -> Result<Replica, SvcError> {
        let response = self
            .snapshot()
            .create_snapshot_clone(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::ReplicaSnapshotClone,
                request: "create_snapshot_clone",
            })?;
        rpc_replica_to_agent(response.get_ref(), self.context.node())
    }

    async fn list_snapshot_clones(
        &self,
        request: &ListSnapshotClones,
    ) -> Result<Vec<Replica>, SvcError> {
        let response = self
            .snapshot()
            .list_snapshot_clone(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Replica,
                request: "list_snapshot_clones",
            })?;
        let rpc_replicas = &response.get_ref().replicas;

        let replicas = rpc_replicas
            .iter()
            .filter_map(|p| match rpc_replica_to_agent(p, self.context.node()) {
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
