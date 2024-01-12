use super::{
    super::NexusListApi,
    translation::{rpc_nexus_to_agent, AgentToIoEngine},
};
use crate::controller::io_engine::{
    translation::TryIoEngineToAgent,
    types::{CreateNexusSnapshot, CreateNexusSnapshotResp, RebuildHistoryResp},
};
use agents::errors::{GrpcRequest as GrpcRequestError, SvcError};
use rpc::v1::nexus::ListNexusOptions;
use stor_port::{
    transport_api::ResourceKind,
    types::v0::transport::{
        AddNexusChild, CreateNexus, DestroyNexus, FaultNexusChild, GetRebuildRecord,
        ListRebuildRecord, Nexus, NexusChildAction, NexusChildActionContext, NexusId, NodeId,
        RebuildHistory, RemoveNexusChild, ResizeNexus, ShareNexus, ShutdownNexus, UnshareNexus,
    },
};

use snafu::ResultExt;
use std::collections::HashMap;

#[async_trait::async_trait]
impl crate::controller::io_engine::NexusListApi for super::RpcClient {
    async fn list_nexuses(&self) -> Result<Vec<Nexus>, SvcError> {
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
            .filter_map(|n| match rpc_nexus_to_agent(n, self.context.node()) {
                Ok(n) => Some(n),
                Err(error) => {
                    tracing::error!(error=%error, "Could not convert rpc nexus");
                    None
                }
            })
            .collect();

        Ok(nexuses)
    }

    async fn get_nexus(&self, nexus_id: &NexusId) -> Result<Nexus, SvcError> {
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
            Some(nexus) => match rpc_nexus_to_agent(nexus, self.context.node()) {
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
    #[tracing::instrument(name = "rpc::v1::nexus::create", level = "debug", skip(self), err)]
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

    #[tracing::instrument(name = "rpc::v1::nexus::destroy", level = "debug", skip(self), err)]
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

    #[tracing::instrument(name = "rpc::v1::nexus::resize", level = "debug", skip(self), err)]
    async fn resize_nexus(&self, request: &ResizeNexus) -> Result<Nexus, SvcError> {
        let result = self
            .nexus()
            .resize_nexus(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Nexus,
                request: "resize_nexus",
            });

        // We map certain tonic gRPC error codes to NexusResizeStatusUnknown, assuming that
        // we don't know where the nexus resize request went wrong.
        let rpc_nexus = match result {
            Err(status)
                if matches!(
                    status.tonic_code(),
                    tonic::Code::Unknown
                        | tonic::Code::DeadlineExceeded
                        | tonic::Code::Internal
                        | tonic::Code::Unavailable
                ) =>
            {
                tracing::warn!(nexus.uuid=%request.uuid, status=%status, "Resize status unclear");
                Err(SvcError::NexusResizeStatusUnknown {
                    nexus_id: request.uuid.to_string(),
                    requested_size: request.requested_size,
                })
            }
            _else => _else,
        }?;

        match rpc_nexus.into_inner().nexus {
            Some(nexus) => Ok(rpc_nexus_to_agent(&nexus, &request.node)?),
            None => Err(SvcError::Internal {
                details: "resource: Nexus, request: resize_nexus, error: no nexus returned".into(),
            }),
        }
    }

    #[tracing::instrument(name = "rpc::v1::nexus::shutdown", level = "debug", skip(self), err)]
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
    #[tracing::instrument(
        name = "rpc::v1::nexus::add_child",
        level = "debug",
        skip(self),
        fields(rpc.io_engine = true),
        err
    )]
    async fn add_child(&self, request: &AddNexusChild) -> Result<Nexus, SvcError> {
        let result = self
            .nexus()
            .add_child_nexus(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Child,
                request: "add_child_nexus",
            });
        match result {
            Ok(rpc_nexus) => match rpc_nexus.into_inner().nexus {
                None => Err(SvcError::Internal {
                    details: format!(
                        "resource: {}, request: {}, err: {}",
                        "Nexus", "add_child", "no nexus returned"
                    ),
                }),
                Some(nexus) => Ok(rpc_nexus_to_agent(&nexus, &request.node)?),
            },
            Err(error) if error.tonic_code() == tonic::Code::AlreadyExists => {
                let nexus = self.fetch_nexus(&request.nexus).await?;
                if let Some(child) = nexus.child(request.uri.as_str()) {
                    // todo: Should we do anything here depending on the state?
                    tracing::warn!(
                        ?child,
                        nexus=%request.nexus,
                        "Child is already part of the nexus"
                    );
                    Ok(nexus)
                } else {
                    Err(error)
                }
            }
            Err(error) => Err(error),
        }
    }

    #[tracing::instrument(
        name = "rpc::v1::nexus::remove_child",
        level = "debug",
        skip(self),
        fields(rpc.io_engine = true),
        err
    )]
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
                let nexus = self.fetch_nexus(&request.nexus).await?;
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

    #[tracing::instrument(name = "rpc::v1::nexus::fault_child", level = "debug", skip(self), fields(rpc.io_engine = true), err)]
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

#[async_trait::async_trait]
impl crate::controller::io_engine::NexusChildActionApi<NexusChildActionContext>
    for super::RpcClient
{
    #[tracing::instrument(name = "rpc::v1::nexus::child_action", level = "debug", skip(self), fields(rpc.io_engine = true), err)]
    async fn child_action(
        &self,
        request: NexusChildAction<NexusChildActionContext>,
    ) -> Result<Nexus, SvcError> {
        let response = self
            .nexus()
            .child_operation(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Child,
                request: "child_action",
            })?;
        Self::nexus_opt(response.into_inner().nexus, request.node(), "child_action")
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
    async fn fetch_nexus(&self, nexus_id: &NexusId) -> Result<Nexus, SvcError> {
        let fetcher = self.fetcher_client().await?;
        fetcher.get_nexus(nexus_id).await
    }
}

#[async_trait::async_trait]
impl crate::controller::io_engine::NexusSnapshotApi for super::RpcClient {
    #[tracing::instrument(name = "rpc::v1::nexus::snapshot", level = "debug", skip(self))]
    async fn create_nexus_snapshot(
        &self,
        request: &CreateNexusSnapshot,
    ) -> Result<CreateNexusSnapshotResp, SvcError> {
        let response = self
            .snapshot()
            .create_nexus_snapshot(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Nexus,
                request: "create_snapshot",
            })?;
        response.into_inner().try_to_agent()
    }
}

#[async_trait::async_trait]
impl crate::controller::io_engine::NexusChildRebuildApi for super::RpcClient {
    async fn get_rebuild_history(
        &self,
        request: &GetRebuildRecord,
    ) -> Result<RebuildHistory, SvcError> {
        let response = self
            .nexus()
            .get_rebuild_history(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Nexus,
                request: "get_rebuild_history",
            })?;
        response.into_inner().try_to_agent()
    }

    async fn list_rebuild_record(
        &self,
        request: &ListRebuildRecord,
    ) -> Result<RebuildHistoryResp, SvcError> {
        let response = self
            .nexus()
            .list_rebuild_history(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Nexus,
                request: "get_rebuild_history",
            })?;
        let mut histories: HashMap<NexusId, RebuildHistory> = HashMap::new();
        let response = response.into_inner();
        let end_time = response.end_time;
        for (nexus, rebuild_record) in response.histories.iter() {
            let nex = NexusId::try_from(nexus.as_str()).map_err(|_| SvcError::InvalidUuid {
                uuid: nexus.to_owned(),
                kind: ResourceKind::Nexus,
            })?;
            let history = rebuild_record.try_to_agent()?;
            let _ = histories.insert(nex, history);
        }
        Ok(RebuildHistoryResp {
            end_time,
            histories,
        })
    }
}
