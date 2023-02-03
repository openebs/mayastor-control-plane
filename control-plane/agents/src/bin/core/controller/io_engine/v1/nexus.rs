use super::translation::{rpc_nexus_to_agent, AgentToIoEngine};
use crate::controller::io_engine::NexusListApi;
use agents::errors::{GrpcRequest as GrpcRequestError, SvcError};
use common_lib::{
    transport_api::ResourceKind,
    types::v0::transport::{
        AddNexusChild, CreateNexus, DestroyNexus, FaultNexusChild, Nexus, NexusId, NodeId,
        RemoveNexusChild, ShareNexus, ShutdownNexus, UnshareNexus,
    },
};
use rpc::v1::nexus::ListNexusOptions;

use snafu::ResultExt;

#[async_trait::async_trait]
impl crate::controller::io_engine::NexusListApi for super::RpcClient {
    async fn list_nexuses(&self, node_id: &NodeId) -> Result<Vec<Nexus>, SvcError> {
        let rpc_nexuses = self
            .nexus()
            .list_nexus(ListNexusOptions {
                name: None,
                uuid: None,
            })
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Nexus,
                request: "list_nexuses",
            })?;

        let rpc_nexuses = &rpc_nexuses.get_ref().nexus_list;

        let nexuses = rpc_nexuses
            .iter()
            .filter_map(|n| match rpc_nexus_to_agent(n, node_id) {
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
        let response = self
            .nexus()
            .list_nexus(ListNexusOptions {
                name: None,
                uuid: Some(nexus_id.to_string()),
            })
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Nexus,
                request: "list_nexus_uuid",
            })?;

        let response = response.into_inner().nexus_list;
        match response.first() {
            Some(nexus) => match rpc_nexus_to_agent(nexus, node_id) {
                Ok(nexus) => Ok(nexus),
                Err(error) => {
                    tracing::error!(error=%error, "Could not convert rpc nexus");
                    Err(SvcError::NotFound {
                        kind: ResourceKind::Nexus,
                        id: nexus_id.to_string(),
                    })
                }
            },
            None => Err(SvcError::NotFound {
                kind: ResourceKind::Nexus,
                id: nexus_id.to_string(),
            }),
        }
    }
}

#[async_trait::async_trait]
impl crate::controller::io_engine::NexusApi<()> for super::RpcClient {
    async fn create_nexus(&self, request: &CreateNexus) -> Result<Nexus, SvcError> {
        let response =
            self.nexus()
                .create_nexus(request.to_rpc())
                .await
                .context(GrpcRequestError {
                    resource: ResourceKind::Nexus,
                    request: "create_nexus",
                })?;
        Self::nexus_opt(response.into_inner().nexus, &request.node, "create_nexus")
    }

    async fn destroy_nexus(&self, request: &DestroyNexus) -> Result<(), SvcError> {
        let result = self.nexus().destroy_nexus(request.to_rpc()).await;
        match result {
            Ok(_) => Ok(()),
            Err(status) if status.code() == tonic::Code::NotFound => {
                tracing::warn!(
                    nexus.uuid = %request.uuid,
                    "Trying to destroy nexus which is already destroyed",
                );
                Ok(())
            }
            error => error.map(|_| ()).context(GrpcRequestError {
                resource: ResourceKind::Nexus,
                request: "destroy_nexus",
            }),
        }
    }

    async fn shutdown_nexus(&self, request: &ShutdownNexus) -> Result<(), SvcError> {
        let _ = self
            .nexus()
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
        let rpc_nexus =
            self.nexus()
                .publish_nexus(request.to_rpc())
                .await
                .context(GrpcRequestError {
                    resource: ResourceKind::Nexus,
                    request: "publish_nexus",
                })?;
        match rpc_nexus.into_inner().nexus {
            Some(nexus) => Ok(nexus.device_uri),
            None => Err(SvcError::Internal {
                details: format!(
                    "resource: {}, request: {}, err: {}",
                    "Nexus", "publish_nexus", "no nexus returned"
                ),
            }),
        }
    }

    async fn unshare_nexus(&self, request: &UnshareNexus) -> Result<(), SvcError> {
        let _ = self
            .nexus()
            .unpublish_nexus(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Nexus,
                request: "unpublish_nexus",
            })?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl crate::controller::io_engine::NexusShareApi<Nexus, Nexus> for super::RpcClient {
    async fn share_nexus(&self, request: &ShareNexus) -> Result<Nexus, SvcError> {
        let response =
            self.nexus()
                .publish_nexus(request.to_rpc())
                .await
                .context(GrpcRequestError {
                    resource: ResourceKind::Nexus,
                    request: "publish_nexus",
                })?;
        Self::nexus_opt(response.into_inner().nexus, &request.node, "share_nexus")
    }

    async fn unshare_nexus(&self, request: &UnshareNexus) -> Result<Nexus, SvcError> {
        let response = self
            .nexus()
            .unpublish_nexus(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Nexus,
                request: "unpublish_nexus",
            })?;
        Self::nexus_opt(response.into_inner().nexus, &request.node, "unshare_nexus")
    }
}

#[async_trait::async_trait]
impl crate::controller::io_engine::NexusChildApi<Nexus, Nexus, ()> for super::RpcClient {
    async fn add_child(&self, request: &AddNexusChild) -> Result<Nexus, SvcError> {
        let rpc_nexus = self
            .nexus()
            .add_child_nexus(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Child,
                request: "add_child_nexus",
            })?;
        match rpc_nexus.into_inner().nexus {
            None => Err(SvcError::Internal {
                details: format!(
                    "resource: {}, request: {}, err: {}",
                    "Nexus", "add_child", "no nexus returned"
                ),
            }),
            Some(nexus) => Ok(rpc_nexus_to_agent(&nexus, &request.node)?),
        }
    }

    async fn remove_child(&self, request: &RemoveNexusChild) -> Result<Nexus, SvcError> {
        let result = self
            .nexus()
            .remove_child_nexus(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Child,
                request: "remove_child_nexus",
            });
        match result.map(|r| r.into_inner().nexus) {
            Ok(Some(nexus)) => Ok(rpc_nexus_to_agent(&nexus, &request.node)?),
            Ok(None) => Err(SvcError::Internal {
                details: format!(
                    "resource: {}, request: {}, err: {}",
                    "Nexus", "remove_child", "no nexus returned"
                ),
            }),
            Err(error) => {
                let nexus = self.fetch_nexus(&request.node, &request.nexus).await?;
                if !nexus.contains_child(&request.uri) {
                    tracing::warn!(
                        child.uri=%request.uri,
                        nexus=%request.nexus,
                        "Child was already removed from nexus"
                    );
                    Ok(nexus)
                } else {
                    Err(error)
                }
            }
        }
    }

    async fn fault_child(&self, request: &FaultNexusChild) -> Result<(), SvcError> {
        let _ = self
            .nexus()
            .fault_nexus_child(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Child,
                request: "fault_child_nexus",
            })?;
        Ok(())
    }
}

impl super::RpcClient {
    fn nexus_opt(
        nexus: Option<rpc::v1::nexus::Nexus>,
        node: &NodeId,
        op: &str,
    ) -> Result<Nexus, SvcError> {
        match nexus {
            Some(nexus) => Ok(rpc_nexus_to_agent(&nexus, node)?),
            None => Err(SvcError::Internal {
                details: format!(
                    "resource: {}, request: {}, err: {}",
                    "Nexus", op, "no nexus returned"
                ),
            }),
        }
    }
    async fn fetch_nexus(&self, node_id: &NodeId, nexus_id: &NexusId) -> Result<Nexus, SvcError> {
        let fetcher = self.fetcher_client().await?;
        fetcher.get_nexus(node_id, nexus_id).await
    }
}
