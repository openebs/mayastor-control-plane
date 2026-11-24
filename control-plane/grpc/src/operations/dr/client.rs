use crate::{
    context::{Client, Context, TracedChannel},
    dr,
    dr::{cluster_pair_reply, cluster_pairs_reply, dr_grpc_client, ProbeRequest},
    operations::dr::traits::DrOperations,
};
use std::{ops::Deref, time::Duration};
use stor_port::{
    transport_api::{ReplyError, ResourceKind, TimeoutOptions},
    types::v0::transport::{
        CreateDrClusterPair, DestroyDrClusterPair, DrClusterPair, DrClusterPairs, GetDrClusterPair,
    },
};
use tonic::transport::Uri;

/// RPC Dr Client
#[derive(Clone)]
pub struct DrClient {
    inner: Client<dr_grpc_client::DrGrpcClient<TracedChannel>>,
}

impl DrClient {
    /// Creates a new base tonic endpoint with the timeout options and the address.
    pub async fn new<O: Into<Option<TimeoutOptions>>>(addr: Uri, opts: O) -> Self {
        let client = Client::new(addr, opts, dr_grpc_client::DrGrpcClient::new)
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
        let client =
            Client::new_with_tls(addr, opts, Some(tls), dr_grpc_client::DrGrpcClient::new).await?;
        Ok(Self { inner: client })
    }
    /// Try to wait until the Dr service is ready, up to a timeout, by using the Probe method.
    pub async fn wait_ready(&self, timeout_opts: Option<TimeoutOptions>) -> Result<(), ()> {
        let timeout_opts = match timeout_opts {
            Some(opts) => opts,
            None => TimeoutOptions::new()
                .with_req_timeout(Duration::from_millis(250))
                .with_max_retries(10),
        };
        for attempt in 1..=timeout_opts.max_retries().unwrap_or_default() {
            match self
                .probe(Some(Context::new(Some(timeout_opts.clone()))))
                .await
            {
                Ok(true) => return Ok(()),
                _ => {
                    let delay = Duration::from_millis(100);
                    if attempt < timeout_opts.max_retries().unwrap_or_default() {
                        tracing::trace!(%attempt, delay=?delay, "Not available, retrying after...");
                    }
                    tokio::time::sleep(delay).await;
                }
            }
        }
        tracing::error!("Timed out");
        Err(())
    }
}

impl Deref for DrClient {
    type Target = Client<dr_grpc_client::DrGrpcClient<TracedChannel>>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Unwraps a single-pair reply, which carries either the pair or why there is none.
fn pair(reply: dr::ClusterPairReply) -> Result<DrClusterPair, ReplyError> {
    match reply.reply {
        Some(cluster_pair_reply::Reply::Pair(pair)) => pair.try_into(),
        Some(cluster_pair_reply::Reply::Error(error)) => Err(error.into()),
        None => Err(ReplyError::invalid_response(ResourceKind::DrClusterPair)),
    }
}

#[tonic::async_trait]
impl DrOperations for DrClient {
    #[tracing::instrument(name = "DrClient::probe", level = "debug", skip(self))]
    async fn probe(&self, _ctx: Option<Context>) -> Result<bool, ReplyError> {
        match self.client().probe(ProbeRequest {}).await {
            Ok(resp) => Ok(resp.into_inner().ready),
            Err(e) => Err(e.into()),
        }
    }

    // Not `skip(self)`: the request carries the store credentials, and `err` logs it.
    #[tracing::instrument(name = "DrClient::create_pair", level = "debug", skip_all, fields(pair.id = %req.id), err)]
    async fn create_pair(
        &self,
        req: &CreateDrClusterPair,
        _ctx: Option<Context>,
    ) -> Result<DrClusterPair, ReplyError> {
        let request: dr::CreateClusterPairRequest = req.into();
        let reply = self
            .client()
            .create_cluster_pair(request)
            .await?
            .into_inner();
        pair(reply)
    }

    #[tracing::instrument(name = "DrClient::get_pair", level = "debug", skip(self), err)]
    async fn get_pair(
        &self,
        req: &GetDrClusterPair,
        _ctx: Option<Context>,
    ) -> Result<DrClusterPair, ReplyError> {
        let reply = self
            .client()
            .get_cluster_pair(dr::GetClusterPairRequest {
                id: req.id.to_string(),
            })
            .await?
            .into_inner();
        pair(reply)
    }

    #[tracing::instrument(name = "DrClient::list_pairs", level = "debug", skip(self), err)]
    async fn list_pairs(&self, _ctx: Option<Context>) -> Result<DrClusterPairs, ReplyError> {
        let reply = self
            .client()
            .list_cluster_pairs(dr::ListClusterPairsRequest {})
            .await?
            .into_inner();
        match reply.reply {
            Some(cluster_pairs_reply::Reply::Pairs(pairs)) => pairs.try_into(),
            Some(cluster_pairs_reply::Reply::Error(error)) => Err(error.into()),
            None => Err(ReplyError::invalid_response(ResourceKind::DrClusterPair)),
        }
    }

    #[tracing::instrument(name = "DrClient::destroy_pair", level = "debug", skip(self), err)]
    async fn destroy_pair(
        &self,
        req: &DestroyDrClusterPair,
        _ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        let reply = self
            .client()
            .destroy_cluster_pair(dr::DestroyClusterPairRequest {
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
