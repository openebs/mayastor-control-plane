use super::translation::{rpc_nexus_to_agent, rpc_nexus_v2_to_agent, AgentToIoEngine};
use crate::controller::io_engine::{translation::IoEngineToAgent, NexusListApi};
use agents::errors::{GrpcRequest as GrpcRequestError, SvcError};
use rpc::io_engine::Null;
use stor_port::{
    transport_api::ResourceKind,
    types::v0::transport::{
        AddNexusChild, Child, CreateNexus, DestroyNexus, FaultNexusChild, Nexus, NexusChildAction,
        NexusId, NodeId, RemoveNexusChild, ShareNexus, ShutdownNexus, UnshareNexus,
    },
};

use snafu::ResultExt;

#[async_trait::async_trait]
impl crate::controller::io_engine::NexusListApi for super::RpcClient {
    async fn list_nexuses(&self, node_id: &NodeId) -> Result<Vec<Nexus>, SvcError> {
        let rpc_nexuses = self
            .client()
            .list_nexus_v2(Null {})
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Nexus,
                request: "list_nexuses",
            })?;

        let rpc_nexuses = &rpc_nexuses.get_ref().nexus_list;
        let nexuses = rpc_nexuses
            .iter()
            .filter_map(|n| match rpc_nexus_v2_to_agent(n, node_id) {
                Ok(n) => Some(n),
                Err(error) => {
                    tracing::error!(error=%error, "Could not convert rpc nexus");
                    None
                }
            })
            .collect();

        Ok(nexuses)
    }

    async fn get_nexus(&self, node_id: &NodeId, nexus_id: &NexusId) -> Result<Nexus, SvcError> {
        let nexuses = self.list_nexuses(node_id).await?;
        nexuses
            .into_iter()
            .find(|nexus| &nexus.uuid == nexus_id)
            .ok_or(SvcError::NexusNotFound {
                nexus_id: nexus_id.to_string(),
            })
    }
}

#[async_trait::async_trait]
impl crate::controller::io_engine::NexusApi<()> for super::RpcClient {
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

#[async_trait::async_trait]
impl crate::controller::io_engine::NexusShareApi<String, ()> for super::RpcClient {
    async fn share_nexus(&self, request: &ShareNexus) -> Result<String, SvcError> {
        let share = self
            .client()
            .publish_nexus(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Nexus,
                request: "share_nexus",
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
                request: "unshare_nexus",
            })?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl crate::controller::io_engine::NexusShareApi<Nexus, Nexus> for super::RpcClient {
    async fn share_nexus(&self, request: &ShareNexus) -> Result<Nexus, SvcError> {
        let _ =
            crate::controller::io_engine::NexusShareApi::<String, ()>::share_nexus(self, request)
                .await?;
        self.fetch_nexus(&request.node, &request.uuid).await
    }

    async fn unshare_nexus(&self, request: &UnshareNexus) -> Result<Nexus, SvcError> {
        crate::controller::io_engine::NexusShareApi::<String, ()>::unshare_nexus(self, request)
            .await?;
        self.fetch_nexus(&request.node, &request.uuid).await
    }
}

#[async_trait::async_trait]
impl crate::controller::io_engine::NexusChildApi<Child, (), ()> for super::RpcClient {
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
}

#[async_trait::async_trait]
impl crate::controller::io_engine::NexusChildApi<Nexus, Nexus, ()> for super::RpcClient {
    async fn add_child(&self, request: &AddNexusChild) -> Result<Nexus, SvcError> {
        let child =
            crate::controller::io_engine::NexusChildApi::<Child, (), ()>::add_child(self, request)
                .await;
        let nexus = self.fetch_nexus(&request.node, &request.nexus).await?;
        match child {
            Ok(_) => Ok(nexus),
            Err(_) if nexus.contains_child(&request.uri) => {
                tracing::warn!(
                    child.uri=%request.uri,
                    nexus=%request.nexus,
                    "Child is already part of nexus"
                );
                Ok(nexus)
            }
            Err(error) => Err(error),
        }
    }

    async fn remove_child(&self, request: &RemoveNexusChild) -> Result<Nexus, SvcError> {
        let removed = crate::controller::io_engine::NexusChildApi::<Child, (), ()>::remove_child(
            self, request,
        )
        .await;
        let nexus = self.fetch_nexus(&request.node, &request.nexus).await?;
        match removed {
            Ok(_) => Ok(nexus),
            Err(_) if !nexus.contains_child(&request.uri) => {
                tracing::warn!(
                    child.uri=%request.uri,
                    nexus=%request.nexus,
                    "Child was already removed from nexus"
                );
                Ok(nexus)
            }
            Err(error) => Err(error),
        }
    }

    async fn fault_child(&self, request: &FaultNexusChild) -> Result<(), SvcError> {
        crate::controller::io_engine::NexusChildApi::<Child, (), ()>::fault_child(self, request)
            .await
    }
}

#[async_trait::async_trait]
impl crate::controller::io_engine::NexusChildActionApi for super::RpcClient {
    async fn child_action(&self, _: &NexusChildAction) -> Result<Nexus, SvcError> {
        Err(SvcError::GrpcRequestError {
            resource: ResourceKind::Child,
            request: "child_action".to_string(),
            source: tonic::Status::unimplemented(""),
        })
    }
}

impl super::RpcClient {
    async fn fetch_nexus(&self, node: &NodeId, nexus: &NexusId) -> Result<Nexus, SvcError> {
        let fetcher = self.fetcher_client().await?;
        let nexuses = fetcher.list_nexuses(node).await?;
        match nexuses.into_iter().find(|n| &n.uuid == nexus) {
            Some(nexus) => Ok(nexus),
            None => Err(SvcError::NotFound {
                kind: ResourceKind::Nexus,
                id: nexus.to_string(),
            }),
        }
    }
}
