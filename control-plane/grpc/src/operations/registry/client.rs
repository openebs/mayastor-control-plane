use crate::{
    context::{Client, Context, TracedChannel},
    operations::registry::traits::{GetSpecsInfo, GetStatesInfo, RegistryOperations},
    registry::{get_specs_reply, get_states_reply, registry_grpc_client::RegistryGrpcClient},
};
use std::{convert::TryFrom, ops::Deref};
use stor_port::{
    transport_api::{ReplyError, ResourceKind, TimeoutOptions},
    types::v0::transport::{MessageIdVs, Specs, States},
};
use tonic::transport::Uri;

/// RPC Registry Client
#[derive(Clone)]
pub struct RegistryClient {
    inner: Client<RegistryGrpcClient<TracedChannel>>,
}
impl Deref for RegistryClient {
    type Target = Client<RegistryGrpcClient<TracedChannel>>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl RegistryClient {
    /// creates a new base tonic endpoint with the timeout options and the address
    pub async fn new<O: Into<Option<TimeoutOptions>>>(addr: Uri, opts: O) -> Self {
        let client = Client::new(addr, opts, RegistryGrpcClient::new).await;
        Self { inner: client }
    }
}
/// Implement registry operations supported by the Registry RPC client.
/// This converts the client side data into a RPC request.
#[tonic::async_trait]
impl RegistryOperations for RegistryClient {
    async fn get_specs(
        &self,
        request: &dyn GetSpecsInfo,
        ctx: Option<Context>,
    ) -> Result<Specs, ReplyError> {
        let req = self.request(request, ctx, MessageIdVs::GetSpecs);
        let response = self.client().get_specs(req).await?.into_inner();
        match response.reply {
            Some(get_specs_reply) => match get_specs_reply {
                get_specs_reply::Reply::Specs(specs) => Ok(Specs::try_from(specs)?),
                get_specs_reply::Reply::Error(err) => Err(err.into()),
            },
            None => Err(ReplyError::invalid_response(ResourceKind::Spec)),
        }
    }

    async fn get_states(
        &self,
        request: &dyn GetStatesInfo,
        ctx: Option<Context>,
    ) -> Result<States, ReplyError> {
        let req = self.request(request, ctx, MessageIdVs::GetStates);
        let response = self.client().get_states(req).await?.into_inner();
        match response.reply {
            Some(get_states_reply) => match get_states_reply {
                get_states_reply::Reply::States(states) => Ok(States::try_from(states)?),
                get_states_reply::Reply::Error(err) => Err(err.into()),
            },
            None => Err(ReplyError::invalid_response(ResourceKind::State)),
        }
    }
}
