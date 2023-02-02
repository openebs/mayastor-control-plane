use common_lib::types::v0::transport::{
    CreateReplica, DestroyReplica, NodeId, Replica, ShareReplica, UnshareReplica,
};
use snafu::ResultExt;

use agents::{
    errors::{GrpcRequest as GrpcRequestError, SvcError},
    msg_translation::v1::{
        rpc_replica_to_agent as v1_rpc_replica_to_agent, AgentToIoEngine as v1_conversion,
    },
};
use common_lib::transport_api::ResourceKind;

use rpc::v1::replica::ListReplicaOptions;

#[async_trait::async_trait]
impl crate::controller::io_engine::ReplicaListApi for super::RpcClient {
    async fn list_replicas(&self, id: &NodeId) -> Result<Vec<Replica>, SvcError> {
        let rpc_replicas = self
            .replica()
            .list_replicas(ListReplicaOptions {
                name: None,
                poolname: None,
            })
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Replica,
                request: "list_replicas",
            })?;

        let rpc_replicas = &rpc_replicas.get_ref().replicas;

        let replicas = rpc_replicas
            .iter()
            .filter_map(|p| match v1_rpc_replica_to_agent(p, id) {
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
            .create_replica(v1_conversion::to_rpc(request))
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Replica,
                request: "create_replica",
            })?;
        let replica = v1_rpc_replica_to_agent(&rpc_replica.into_inner(), &request.node)?;
        Ok(replica)
    }

    async fn destroy_replica(&self, request: &DestroyReplica) -> Result<(), SvcError> {
        let _ = self
            .replica()
            .destroy_replica(v1_conversion::to_rpc(request))
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
            .share_replica(v1_conversion::to_rpc(request))
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
            .unshare_replica(v1_conversion::to_rpc(request))
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
