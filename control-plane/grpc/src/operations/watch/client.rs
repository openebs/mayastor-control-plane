use crate::{
    context::{Client, Context, TracedChannel},
    operations::watch::traits::{GetWatchInfo, WatchInfo, WatchOperations},
    watch::{get_watches_reply, watch_grpc_client::WatchGrpcClient},
};
use common_lib::{
    transport_api::{v0::Watches, ReplyError, ResourceKind, TimeoutOptions},
    types::v0::transport::MessageIdVs,
};
use std::{convert::TryFrom, ops::Deref};
use tonic::transport::Uri;

type WatchClientInner = Client<WatchGrpcClient<TracedChannel>>;

/// RPC Watch Client
#[derive(Clone)]
pub struct WatchClient {
    inner: WatchClientInner,
}
impl Deref for WatchClient {
    type Target = WatchClientInner;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl WatchClient {
    /// creates a new base tonic endpoint with the timeout options and the address
    pub async fn new<O: Into<Option<TimeoutOptions>>>(addr: Uri, opts: O) -> Self {
        let client = Client::new(addr, opts, WatchGrpcClient::new).await;
        Self { inner: client }
    }
}

#[tonic::async_trait]
impl WatchOperations for WatchClient {
    async fn create(
        &self,
        request: &dyn WatchInfo,
        ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        let req = self.request(request, ctx, MessageIdVs::CreateWatch);
        let response = self.client().create_watch(req).await?.into_inner();
        match response.error {
            None => Ok(()),
            Some(err) => Err(err.into()),
        }
    }

    async fn get(
        &self,
        request: &dyn GetWatchInfo,
        ctx: Option<Context>,
    ) -> Result<Watches, ReplyError> {
        let req = self.request(request, ctx, MessageIdVs::GetWatches);
        let response = self.client().get_watches(req).await?.into_inner();
        match response.reply {
            Some(get_watches_reply) => match get_watches_reply {
                get_watches_reply::Reply::Watches(watches) => Ok(Watches::try_from(watches)?),
                get_watches_reply::Reply::Error(err) => Err(err.into()),
            },
            None => Err(ReplyError::invalid_response(ResourceKind::Watch)),
        }
    }

    async fn destroy(
        &self,
        request: &dyn WatchInfo,
        ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        let req = self.request(request, ctx, MessageIdVs::DeleteWatch);
        let response = self.client().delete_watch(req).await?.into_inner();
        match response.error {
            None => Ok(()),
            Some(err) => Err(err.into()),
        }
    }
}
