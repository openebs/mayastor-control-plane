use super::translation::{rpc_nexus_to_agent, rpc_nexus_v2_to_agent, AgentToIoEngine};
use crate::controller::io_engine::translation::IoEngineToAgent;
use agents::errors::{GrpcRequest as GrpcRequestError, SvcError};
use common_lib::{
    transport_api::ResourceKind,
    types::v0::transport::{
        AddNexusChild, Child, CreateNexus, DestroyNexus, FaultNexusChild, Nexus, NodeId,
        RemoveNexusChild, ShareNexus, ShutdownNexus, UnshareNexus,
    },
};
use rpc::io_engine::Null;

use snafu::ResultExt;

#[async_trait::async_trait]
impl crate::controller::io_engine::NexusListApi for super::RpcClient {
    async fn list_nexus(&self, id: &NodeId) -> Result<Vec<Nexus>, SvcError> {
        let rpc_nexuses = self
            .client()
            .list_nexus_v2(Null {})
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Nexus,
                request: "list_nexus",
            })?;

        let rpc_nexuses = &rpc_nexuses.get_ref().nexus_list;
        let nexuses = rpc_nexuses
            .iter()
            .filter_map(|n| match rpc_nexus_v2_to_agent(n, id) {
                Ok(n) => Some(n),
                Err(error) => {
                    tracing::error!(error=%error, "Could not convert rpc nexus");
                    None
                }
            })
            .collect();

        Ok(nexuses)
    }
}
#[async_trait::async_trait]
impl crate::controller::io_engine::NexusApi for super::RpcClient {
    async fn create_nexus(&self, request: &CreateNexus) -> Result<Nexus, SvcError> {
        let rpc_nexus = self
            .client()
            .create_nexus_v2(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Nexus,
                request: "create_nexus",
            })?;
        let mut nexus = rpc_nexus_to_agent(&rpc_nexus.into_inner(), &request.node)?;
        // CAS-1107 - create_nexus_v2 returns NexusV1...
        nexus.name = request.name();
        nexus.uuid = request.uuid.clone();
        Ok(nexus)
    }

    async fn destroy_nexus(&self, request: &DestroyNexus) -> Result<(), SvcError> {
        let _ = self
            .client()
            .destroy_nexus(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Nexus,
                request: "destroy_nexus",
            })?;
        Ok(())
    }

    async fn share_nexus(&self, request: &ShareNexus) -> Result<String, SvcError> {
        let share = self
            .client()
            .publish_nexus(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Nexus,
                request: "publish_nexus",
            })?;
        let share = share.into_inner().device_uri;
        Ok(share)
    }

    async fn unshare_nexus(&self, request: &UnshareNexus) -> Result<(), SvcError> {
        let _ = self
            .client()
            .unpublish_nexus(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Nexus,
                request: "unpublish_nexus",
            })?;
        Ok(())
    }

    async fn add_child(&self, request: &AddNexusChild) -> Result<Child, SvcError> {
        let rpc_child = self
            .client()
            .add_child_nexus(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Child,
                request: "add_child_nexus",
            })?;
        Ok(rpc_child.into_inner().to_agent())
    }

    async fn remove_child(&self, request: &RemoveNexusChild) -> Result<(), SvcError> {
        let _ = self
            .client()
            .remove_child_nexus(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Child,
                request: "remove_child_nexus",
            })?;
        Ok(())
    }

    async fn fault_child(&self, request: &FaultNexusChild) -> Result<(), SvcError> {
        let _ = self
            .client()
            .fault_nexus_child(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Child,
                request: "fault_child_nexus",
            })?;
        Ok(())
    }

    async fn shutdown_nexus(&self, request: &ShutdownNexus) -> Result<(), SvcError> {
        let _ = self
            .client()
            .shutdown_nexus(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Nexus,
                request: "shutdown_nexus",
            })?;
        Ok(())
    }
}
