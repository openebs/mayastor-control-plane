mod host;
mod nexus;
mod pool;
mod replica;
mod translation;

use crate::controller::io_engine::{ApiVersion, GrpcContext, NexusChildRebuildApi};
use agents::errors::{GrpcConnect, SvcError};
use rpc::io_engine::IoEngineClientV0;
use std::collections::HashMap;
use stor_port::types::v0::transport::{
    GetRebuildRecord, ListRebuildRecord, NexusId, RebuildHistory,
};

use snafu::ResultExt;
use stor_port::transport_api::ResourceKind;
use tonic::{transport::Channel, Status};

/// Io-Engine client v0.
#[derive(Clone)]
pub(crate) struct RpcClient {
    client: IoEngineClientV0<Channel>,
    context: GrpcContext,
}

impl RpcClient {
    pub(crate) async fn new(context: &GrpcContext) -> Result<Self, SvcError> {
        let client = Self::make_client(context).await?;
        Ok(Self {
            client,
            context: context.clone(),
        })
    }
    async fn make_client(context: &GrpcContext) -> Result<IoEngineClientV0<Channel>, SvcError> {
        IoEngineClientV0::connect(context.tonic_endpoint())
            .await
            .context(GrpcConnect {
                node_id: context.node().to_owned(),
                endpoint: context.endpoint().to_string(),
            })
    }
    async fn fetcher_client(&self) -> Result<Self, SvcError> {
        let mut context = self.context.clone();
        context.override_timeout(None);
        Self::new(&context).await
    }
    fn client(&self) -> IoEngineClientV0<Channel> {
        self.client.clone()
    }
}

#[async_trait::async_trait]
impl NexusChildRebuildApi for RpcClient {
    async fn get_rebuild_history(
        &self,
        _request: &GetRebuildRecord,
    ) -> Result<RebuildHistory, SvcError> {
        Err(SvcError::Unimplemented {
            resource: ResourceKind::Nexus,
            request: "get_rebuild_history".to_string(),
            source: Status::unimplemented("api still unimplemented"),
        })
    }

    async fn list_rebuild_record(
        &self,
        _request: &ListRebuildRecord,
    ) -> Result<HashMap<NexusId, RebuildHistory>, SvcError> {
        Err(SvcError::Unimplemented {
            resource: ResourceKind::Nexus,
            request: "get_rebuild_history".to_string(),
            source: Status::unimplemented("api still unimplemented"),
        })
    }
}

#[async_trait::async_trait]
impl super::NodeApi for RpcClient {
    fn api_version(&self) -> ApiVersion {
        ApiVersion::V0
    }
}
