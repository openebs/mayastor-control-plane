use crate::{
    context::{Client, Context, TracedChannel},
    dr_pair,
    dr_pair::{dr_cluster_pair_reply, dr_cluster_pairs_reply, dr_pair_grpc_client},
    operations::dr_pair::traits::DrPairOperations,
};
use std::ops::Deref;
use stor_port::{
    transport_api::{ReplyError, ResourceKind, TimeoutOptions},
    types::v0::transport::{
        CreateDrClusterPair, DestroyDrClusterPair, DrClusterPair, DrClusterPairs, GetDrClusterPair,
    },
};
use tonic::transport::Uri;

/// Unwraps a single-pair reply, which carries either the pair or why there is none.
fn pair(reply: dr_pair::DrClusterPairReply) -> Result<DrClusterPair, ReplyError> {
    match reply.reply {
        Some(dr_cluster_pair_reply::Reply::Pair(pair)) => pair.try_into(),
        Some(dr_cluster_pair_reply::Reply::Error(error)) => Err(error.into()),
        None => Err(ReplyError::invalid_response(ResourceKind::DrClusterPair)),
    }
}

/// RPC Dr cluster pair Client
#[derive(Clone)]
pub struct DrPairClient {
    inner: Client<dr_pair_grpc_client::DrPairGrpcClient<TracedChannel>>,
}

impl DrPairClient {
    /// Creates a new base tonic endpoint with the timeout options and the address.
    pub async fn new<O: Into<Option<TimeoutOptions>>>(addr: Uri, opts: O) -> Self {
        let client = Client::new(addr, opts, dr_pair_grpc_client::DrPairGrpcClient::new)
            .await
            .expect("a plaintext gRPC channel cannot fail to initialise");
        Self { inner: client }
    }
    /// Creates a TLS-enabled client whose certificate files are reloaded after rotation.
    pub async fn new_with_tls<O: Into<Option<TimeoutOptions>> + Clone>(
        addr: Uri,
        opts: O,
        tls: crate::tls::TlsConfig,
    ) -> anyhow::Result<Self> {
        let client = Client::new_with_tls(
            addr,
            opts,
            Some(tls),
            dr_pair_grpc_client::DrPairGrpcClient::new,
        )
        .await?;
        Ok(Self { inner: client })
    }
}

impl Deref for DrPairClient {
    type Target = Client<dr_pair_grpc_client::DrPairGrpcClient<TracedChannel>>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[tonic::async_trait]
impl DrPairOperations for DrPairClient {
    #[tracing::instrument(name = "DrPairClient::create", level = "debug", skip(self), err)]
    async fn create(
        &self,
        req: &CreateDrClusterPair,
        _ctx: Option<Context>,
    ) -> Result<DrClusterPair, ReplyError> {
        let request: dr_pair::CreateDrClusterPairRequest = req.into();
        let reply = self
            .client()
            .create_dr_cluster_pair(request)
            .await?
            .into_inner();
        pair(reply)
    }

    #[tracing::instrument(name = "DrPairClient::get", level = "debug", skip(self), err)]
    async fn get(
        &self,
        req: &GetDrClusterPair,
        _ctx: Option<Context>,
    ) -> Result<DrClusterPair, ReplyError> {
        let reply = self
            .client()
            .get_dr_cluster_pair(dr_pair::GetDrClusterPairRequest {
                id: req.id.to_string(),
            })
            .await?
            .into_inner();
        pair(reply)
    }

    #[tracing::instrument(name = "DrPairClient::list", level = "debug", skip(self), err)]
    async fn list(&self, _ctx: Option<Context>) -> Result<DrClusterPairs, ReplyError> {
        let reply = self
            .client()
            .list_dr_cluster_pairs(dr_pair::ListDrClusterPairsRequest {})
            .await?
            .into_inner();
        match reply.reply {
            Some(dr_cluster_pairs_reply::Reply::Pairs(pairs)) => pairs.try_into(),
            Some(dr_cluster_pairs_reply::Reply::Error(error)) => Err(error.into()),
            None => Err(ReplyError::invalid_response(ResourceKind::DrClusterPair)),
        }
    }

    #[tracing::instrument(name = "DrPairClient::destroy", level = "debug", skip(self), err)]
    async fn destroy(
        &self,
        req: &DestroyDrClusterPair,
        _ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        let reply = self
            .client()
            .destroy_dr_cluster_pair(dr_pair::DestroyDrClusterPairRequest {
                id: req.id.to_string(),
            })
            .await?
            .into_inner();
        match reply.error {
            Some(error) => Err(error.into()),
            None => Ok(()),
        }
    }
}
