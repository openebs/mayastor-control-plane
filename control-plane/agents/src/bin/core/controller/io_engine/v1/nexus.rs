use super::translation::{rpc_nexus_to_agent, rpc_nexus_to_child_agent, AgentToIoEngine};
use agents::errors::{GrpcRequest as GrpcRequestError, SvcError};
use common_lib::{
    transport_api::ResourceKind,
    types::v0::transport::{
        AddNexusChild, Child, CreateNexus, DestroyNexus, FaultNexusChild, Nexus, NodeId,
        RemoveNexusChild, ShareNexus, ShutdownNexus, UnshareNexus,
    },
};
use rpc::v1::nexus::ListNexusOptions;

use snafu::ResultExt;

#[async_trait::async_trait]
impl crate::controller::io_engine::NexusListApi for super::RpcClient {
    async fn list_nexus(&self, id: &NodeId) -> Result<Vec<Nexus>, SvcError> {
        let rpc_nexuses = self
            .nexus()
            .list_nexus(ListNexusOptions { name: None })
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Nexus,
                request: "list_nexus",
            })?;

        let rpc_nexuses = &rpc_nexuses.get_ref().nexus_list;

        let nexuses = rpc_nexuses
            .iter()
            .filter_map(|n| match rpc_nexus_to_agent(n, id) {
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
        let rpc_nexus =
            self.nexus()
                .create_nexus(request.to_rpc())
                .await
                .context(GrpcRequestError {
                    resource: ResourceKind::Nexus,
                    request: "create_nexus",
                })?;
        if let Some(nexus) = rpc_nexus.into_inner().nexus {
            let nexus = rpc_nexus_to_agent(&nexus, &request.node)?;
            Ok(nexus)
        } else {
            Err(SvcError::Internal {
                details: format!(
                    "resource: {}, request: {}, err: {}",
                    "Nexus", "create_nexus", "no nexus returned"
                ),
            })
        }
    }

    async fn destroy_nexus(&self, request: &DestroyNexus) -> Result<(), SvcError> {
        let _ = self
            .nexus()
            .destroy_nexus(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Nexus,
                request: "destroy_nexus",
            })?;
        Ok(())
    }

    async fn share_nexus(&self, request: &ShareNexus) -> Result<String, SvcError> {
        let rpc_nexus =
            self.nexus()
                .publish_nexus(request.to_rpc())
                .await
                .context(GrpcRequestError {
                    resource: ResourceKind::Nexus,
                    request: "publish_nexus",
                })?;
        if let Some(nexus) = rpc_nexus.into_inner().nexus {
            Ok(nexus.device_uri)
        } else {
            Err(SvcError::Internal {
                details: format!(
                    "resource: {}, request: {}, err: {}",
                    "Nexus", "publish_nexus", "no nexus returned"
                ),
            })
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

    async fn add_child(&self, request: &AddNexusChild) -> Result<Child, SvcError> {
        let rpc_nexus = self
            .nexus()
            .add_child_nexus(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Child,
                request: "add_child_nexus",
            })?;
        if let Some(nexus) = rpc_nexus.into_inner().nexus {
            let child = rpc_nexus_to_child_agent(&nexus, request.uri.clone().into())?;
            Ok(child)
        } else {
            Err(SvcError::Internal {
                details: format!(
                    "resource: {}, request: {}, err: {}",
                    "Nexus", "add_child", "no nexus returned"
                ),
            })
        }
    }

    async fn remove_child(&self, request: &RemoveNexusChild) -> Result<(), SvcError> {
        let _ = self
            .nexus()
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
            .nexus()
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
