use crate::{
    app_node::{
        app_node_grpc_client::AppNodeGrpcClient,
        registration::registration_client::RegistrationClient, GetAppNodeRequest,
        ListAppNodesRequest,
    },
    context::{Client, Context, TracedChannel},
    operations::{
        app_node::traits::{AppNodeInfo, AppNodeOperations, AppNodeRegisterInfo},
        Pagination,
    },
};
use stor_port::{
    transport_api::{v0::AppNodes, ReplyError, ResourceKind, TimeoutOptions},
    types::v0::{
        openapi::client::Uri,
        transport::{AppNode, Filter, MessageIdVs},
    },
};

/// RPC App Node Client.
#[derive(Clone)]
pub struct AppNodeClient {
    ops: Client<AppNodeGrpcClient<TracedChannel>>,
    registration: Client<RegistrationClient<TracedChannel>>,
}

impl AppNodeClient {
    /// Creates a new base tonic endpoint with the timeout options and the address.
    pub async fn new<O: Into<Option<TimeoutOptions>> + Clone>(addr: Uri, opts: O) -> Self {
        let ops_client = Client::new(addr.clone(), opts.clone(), AppNodeGrpcClient::new).await;
        let registration_client = Client::new(addr, opts, RegistrationClient::new).await;
        Self {
            ops: ops_client,
            registration: registration_client,
        }
    }
}

#[tonic::async_trait]
impl AppNodeOperations for AppNodeClient {
    async fn get(&self, req: Filter, ctx: Option<Context>) -> Result<AppNode, ReplyError> {
        let request = self.ops.request(
            GetAppNodeRequest::try_from(req.clone())?,
            ctx,
            MessageIdVs::GetAppNode,
        );
        let response = self
            .ops
            .client()
            .get_app_node(request)
            .await?
            .into_inner()
            .app_node;
        match response {
            None => Err(ReplyError::not_found(
                ResourceKind::AppNode,
                req.to_string(),
                "App node was not found".to_string(),
            )),
            Some(app_node) => Ok(AppNode::try_from(app_node)?),
        }
    }

    async fn list(
        &self,
        pagination: Option<Pagination>,
        ctx: Option<Context>,
    ) -> Result<AppNodes, ReplyError> {
        let pagination = pagination.map(|p| p.into());
        let request = self.ops.request(
            ListAppNodesRequest { pagination },
            ctx,
            MessageIdVs::ListAppNodes,
        );

        let response = self
            .ops
            .client()
            .list_app_nodes(request)
            .await?
            .into_inner()
            .app_nodes;
        match response {
            None => Ok(AppNodes {
                entries: vec![],
                next_token: None,
            }),
            Some(app_nodes) => Ok(AppNodes::try_from(app_nodes)?),
        }
    }

    async fn register_app_node(
        &self,
        req: &dyn AppNodeRegisterInfo,
        ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        let req = self.ops.request(req, ctx, MessageIdVs::RegisterAppNode);
        let response = self.registration.client().register(req).await?.into_inner();
        Ok(response)
    }

    async fn deregister_app_node(
        &self,
        req: &dyn AppNodeInfo,
        ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        let req = self.ops.request(req, ctx, MessageIdVs::DeregisterAppNode);
        let response = self
            .registration
            .client()
            .deregister(req)
            .await?
            .into_inner();
        Ok(response)
    }
}
