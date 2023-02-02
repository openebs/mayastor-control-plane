mod host;
mod nexus;
mod pool;
mod replica;
mod translation;

use crate::controller::io_engine::GrpcContext;
use agents::errors::{GrpcConnect, SvcError};
use rpc::io_engine::IoEngineClientV0;

use snafu::ResultExt;
use tonic::transport::Channel;

/// Io-Engine client v0.
#[derive(Clone)]
pub(crate) struct RpcClient {
    client: IoEngineClientV0<Channel>,
    context: GrpcContext,
}

impl RpcClient {
    pub(crate) async fn new(context: &GrpcContext) -> Result<Self, SvcError> {
        let client = IoEngineClientV0::connect(context.endpoint.clone())
            .await
            .context(GrpcConnect {
                node_id: context.node.to_owned(),
                endpoint: context.endpoint().to_string(),
            })?;
        Ok(Self {
            client,
            context: context.clone(),
        })
    }
    fn client(&self) -> IoEngineClientV0<Channel> {
        self.client.clone()
    }
}

#[async_trait::async_trait]
impl super::NodeApi for RpcClient {}
