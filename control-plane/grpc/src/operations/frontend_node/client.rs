use crate::{
    context::{Client, Context, TracedChannel},
    frontend::{
        frontend_node_grpc_client::FrontendNodeGrpcClient,
        registration::registration_client::RegistrationClient, GetFrontendNodeRequest,
    },
    operations::{
        frontend_node::traits::{
            to_list_frontend_node_request, FrontendNodeInfo, FrontendNodeOperations,
            FrontendNodeRegisterInfo,
        },
        Pagination,
    },
};
use stor_port::{
    transport_api::{v0::FrontendNodes, ReplyError, ResourceKind, TimeoutOptions},
    types::v0::{
        openapi::client::Uri,
        transport::{Filter, FrontendNode, MessageIdVs},
    },
};

/// RPC Frontend Node Client.
#[derive(Clone)]
pub struct FrontendNodeClient {
    ops: Client<FrontendNodeGrpcClient<TracedChannel>>,
    registration: Client<RegistrationClient<TracedChannel>>,
}

impl FrontendNodeClient {
    /// Creates a new base tonic endpoint with the timeout options and the address.
    pub async fn new<O: Into<Option<TimeoutOptions>> + Clone>(addr: Uri, opts: O) -> Self {
        let ops_client = Client::new(addr.clone(), opts.clone(), FrontendNodeGrpcClient::new).await;
        let registration_client = Client::new(addr, opts, RegistrationClient::new).await;
        Self {
            ops: ops_client,
            registration: registration_client,
        }
    }
}

#[tonic::async_trait]
impl FrontendNodeOperations for FrontendNodeClient {
    async fn get(&self, req: Filter, ctx: Option<Context>) -> Result<FrontendNode, ReplyError> {
        let request = self.ops.request(
            GetFrontendNodeRequest::try_from(req.clone())?,
            ctx,
            MessageIdVs::GetFrontendNodes,
        );
        let response = self
            .ops
            .client()
            .get_frontend_node(request)
            .await?
            .into_inner()
            .frontend_node;
        match response {
            None => Err(ReplyError::not_found(
                ResourceKind::FrontendNode,
                req.to_string(),
                "Frontend node was not found".to_string(),
            )),
            Some(frontend_node) => Ok(FrontendNode::try_from(frontend_node)?),
        }
    }

    async fn list(
        &self,
        req: Filter,
        pagination: Option<Pagination>,
        ctx: Option<Context>,
    ) -> Result<FrontendNodes, ReplyError> {
        let req = self.ops.request(
            to_list_frontend_node_request(pagination, req)?,
            ctx,
            MessageIdVs::GetFrontendNodes,
        );
        let response = self
            .ops
            .client()
            .list_frontend_nodes(req)
            .await?
            .into_inner()
            .frontend_nodes;
        match response {
            None => Ok(FrontendNodes {
                entries: vec![],
                next_token: None,
            }),
            Some(frontend_nodes) => Ok(FrontendNodes::try_from(frontend_nodes)?),
        }
    }

    async fn register_frontend_node(
        &self,
        req: &dyn FrontendNodeRegisterInfo,
        ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        let req = self
            .ops
            .request(req, ctx, MessageIdVs::RegisterFrontendNode);
        let response = self.registration.client().register(req).await?.into_inner();
        Ok(response)
    }

    async fn deregister_frontend_node(
        &self,
        req: &dyn FrontendNodeInfo,
        ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        let req = self
            .ops
            .request(req, ctx, MessageIdVs::DeregisterFrontendNode);
        let response = self
            .registration
            .client()
            .deregister(req)
            .await?
            .into_inner();
        Ok(response)
    }
}
