use crate::{
    common::{NexusFilter, NodeFilter, NodeNexusFilter},
    context::{Client, Context, TracedChannel},
    nexus::{
        add_nexus_child_reply, create_nexus_reply, get_nexuses_reply, get_nexuses_request,
        nexus_grpc_client::NexusGrpcClient, share_nexus_reply, GetNexusesRequest,
    },
    operations::nexus::traits::{
        AddNexusChildInfo, CreateNexusInfo, DestroyNexusInfo, NexusOperations,
        RemoveNexusChildInfo, ShareNexusInfo, UnshareNexusInfo,
    },
};
use common_lib::{
    mbus_api::{v0::Nexuses, ReplyError, ResourceKind, TimeoutOptions},
    types::v0::message_bus::{Child, Filter, MessageIdVs, Nexus},
};
use std::{convert::TryFrom, ops::Deref};
use tonic::transport::Uri;

/// RPC Nexus Client
#[derive(Clone)]
pub struct NexusClient {
    inner: Client<NexusGrpcClient<TracedChannel>>,
}

impl Deref for NexusClient {
    type Target = Client<NexusGrpcClient<TracedChannel>>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl NexusClient {
    /// creates a new base tonic endpoint with the timeout options and the address
    pub async fn new<O: Into<Option<TimeoutOptions>>>(addr: Uri, opts: O) -> Self {
        let client = Client::new(addr, opts, NexusGrpcClient::new).await;
        Self { inner: client }
    }
}

#[tonic::async_trait]
impl NexusOperations for NexusClient {
    #[tracing::instrument(name = "NexusClient::create", level = "debug", skip(self), err)]
    async fn create(
        &self,
        request: &dyn CreateNexusInfo,
        ctx: Option<Context>,
    ) -> Result<Nexus, ReplyError> {
        let req = self.request(request, ctx, MessageIdVs::CreateNexus);
        let response = self.client().create_nexus(req).await?.into_inner();
        match response.reply {
            Some(create_nexus_reply) => match create_nexus_reply {
                create_nexus_reply::Reply::Nexus(nexus) => Ok(Nexus::try_from(nexus)?),
                create_nexus_reply::Reply::Error(err) => Err(err.into()),
            },
            None => Err(ReplyError::invalid_response(ResourceKind::Nexus)),
        }
    }

    #[tracing::instrument(name = "NexusClient::get", level = "debug", skip(self), err)]
    async fn get(&self, filter: Filter, ctx: Option<Context>) -> Result<Nexuses, ReplyError> {
        let req: GetNexusesRequest = match filter {
            Filter::Node(id) => GetNexusesRequest {
                filter: Some(get_nexuses_request::Filter::Node(NodeFilter {
                    node_id: id.into(),
                })),
            },
            Filter::NodeNexus(node_id, nexus_id) => GetNexusesRequest {
                filter: Some(get_nexuses_request::Filter::NodeNexus(NodeNexusFilter {
                    node_id: node_id.into(),
                    nexus_id: nexus_id.to_string(),
                })),
            },
            Filter::Nexus(nexus_id) => GetNexusesRequest {
                filter: Some(get_nexuses_request::Filter::Nexus(NexusFilter {
                    nexus_id: nexus_id.to_string(),
                })),
            },
            _ => GetNexusesRequest { filter: None },
        };
        let req = self.request(req, ctx, MessageIdVs::GetNexuses);
        let response = self.client().get_nexuses(req).await?.into_inner();
        match response.reply {
            Some(get_nexuses_reply) => match get_nexuses_reply {
                get_nexuses_reply::Reply::Nexuses(nexuses) => Ok(Nexuses::try_from(nexuses)?),
                get_nexuses_reply::Reply::Error(err) => Err(err.into()),
            },
            None => Err(ReplyError::invalid_response(ResourceKind::Nexus)),
        }
    }

    #[tracing::instrument(name = "NexusClient::destroy", level = "debug", skip(self), err)]
    async fn destroy(
        &self,
        request: &dyn DestroyNexusInfo,
        ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        let req = self.request(request, ctx, MessageIdVs::DestroyNexus);
        let response = self.client().destroy_nexus(req).await?.into_inner();
        match response.error {
            None => Ok(()),
            Some(err) => Err(err.into()),
        }
    }

    #[tracing::instrument(name = "NexusClient::share", level = "debug", skip(self), err)]
    async fn share(
        &self,
        request: &dyn ShareNexusInfo,
        ctx: Option<Context>,
    ) -> Result<String, ReplyError> {
        let req = self.request(request, ctx, MessageIdVs::ShareNexus);
        let response = self.client().share_nexus(req).await?.into_inner();
        match response.reply {
            Some(share_nexus_reply) => match share_nexus_reply {
                share_nexus_reply::Reply::Response(message) => Ok(message),
                share_nexus_reply::Reply::Error(err) => Err(err.into()),
            },
            None => Err(ReplyError::invalid_response(ResourceKind::Nexus)),
        }
    }

    #[tracing::instrument(name = "NexusClient::unshare", level = "debug", skip(self), err)]
    async fn unshare(
        &self,
        request: &dyn UnshareNexusInfo,
        ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        let req = self.request(request, ctx, MessageIdVs::UnshareNexus);
        let response = self.client().unshare_nexus(req).await?.into_inner();
        match response.error {
            None => Ok(()),
            Some(err) => Err(err.into()),
        }
    }

    #[tracing::instrument(
        name = "NexusClient::add_nexus_child",
        level = "debug",
        skip(self),
        err
    )]
    async fn add_nexus_child(
        &self,
        request: &dyn AddNexusChildInfo,
        ctx: Option<Context>,
    ) -> Result<Child, ReplyError> {
        let req = self.request(request, ctx, MessageIdVs::AddNexusChild);
        let response = self.client().add_nexus_child(req).await?.into_inner();
        match response.reply {
            Some(add_nexus_child_reply) => match add_nexus_child_reply {
                add_nexus_child_reply::Reply::Child(child) => Ok(Child::try_from(child)?),
                add_nexus_child_reply::Reply::Error(err) => Err(err.into()),
            },
            None => Err(ReplyError::invalid_response(ResourceKind::Child)),
        }
    }

    #[tracing::instrument(
        name = "NexusClient::remove_nexus_child",
        level = "debug",
        skip(self),
        err
    )]
    async fn remove_nexus_child(
        &self,
        request: &dyn RemoveNexusChildInfo,
        ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        let req = self.request(request, ctx, MessageIdVs::RemoveNexusChild);
        let response = self.client().remove_nexus_child(req).await?.into_inner();
        match response.error {
            None => Ok(()),
            Some(err) => Err(err.into()),
        }
    }
}
