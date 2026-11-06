use crate::{
    context::{Client, Context, TracedChannel},
    dr::{dr_grpc_client, ProbeRequest},
    operations::dr::traits::DrOperations,
};
use std::{ops::Deref, time::Duration};
use stor_port::transport_api::{ReplyError, TimeoutOptions};
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

#[tonic::async_trait]
impl DrOperations for DrClient {
    #[tracing::instrument(name = "DrClient::probe", level = "debug", skip(self))]
    async fn probe(&self, _ctx: Option<Context>) -> Result<bool, ReplyError> {
        match self.client().probe(ProbeRequest {}).await {
            Ok(resp) => Ok(resp.into_inner().ready),
            Err(e) => Err(e.into()),
        }
    }
}
