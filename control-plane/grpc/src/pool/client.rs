use crate::{
    common::{NodeFilter, NodePoolFilter, PoolFilter},
    grpc_opts::{timeout_grpc, Context},
    pool::traits::{CreatePoolInfo, DestroyPoolInfo, PoolOperations},
    pool_grpc::{
        create_pool_reply, get_pools_reply, get_pools_request, pool_grpc_client::PoolGrpcClient,
        CreatePoolRequest, DestroyPoolRequest, GetPoolsRequest,
    },
};
use common_lib::{
    mbus_api::{v0::Pools, ReplyError, ResourceKind, TimeoutOptions},
    types::v0::message_bus::{Filter, MessageIdVs, Pool},
};
use std::{convert::TryFrom, time::Duration};
use tonic::transport::{Channel, Endpoint, Uri};
use utils::DEFAULT_REQ_TIMEOUT;

// RPC Pool Client
#[derive(Clone)]
pub struct PoolClient {
    base_timeout: Duration,
    endpoint: Endpoint,
}

impl PoolClient {
    /// creates a new base tonic endpoint with the timeout options and the address
    pub async fn new<O: Into<Option<TimeoutOptions>>>(addr: Uri, opts: O) -> Self {
        let timeout_opts = opts.into();
        let timeout = timeout_opts
            .map(|opt| opt.base_timeout())
            .unwrap_or_else(|| humantime::parse_duration(DEFAULT_REQ_TIMEOUT).unwrap());
        let endpoint = tonic::transport::Endpoint::from(addr)
            .connect_timeout(timeout)
            .timeout(timeout);
        Self {
            base_timeout: timeout,
            endpoint,
        }
    }
    /// creates a new pool grpc client on a new endpoint after altering the properties of the
    /// base endpoint according to the provided context
    pub async fn reconnect(
        &self,
        ctx: Option<Context>,
        op_id: MessageIdVs,
    ) -> Result<PoolGrpcClient<Channel>, tonic::transport::Error> {
        let ctx_timeout = ctx.map(|ctx| ctx.timeout_opts()).flatten();
        match ctx_timeout {
            None => {
                let timeout = timeout_grpc(op_id, self.base_timeout);
                let endpoint = self
                    .endpoint
                    .clone()
                    .connect_timeout(timeout)
                    .timeout(timeout);
                let client = PoolGrpcClient::connect(endpoint.clone()).await?;
                Ok(client)
            }
            Some(timeout) => {
                let timeout = timeout.base_timeout();
                let endpoint = self
                    .endpoint
                    .clone()
                    .connect_timeout(timeout)
                    .timeout(timeout);
                let client = PoolGrpcClient::connect(endpoint.clone()).await?;
                Ok(client)
            }
        }
    }
}

/// Implement pool operations supported by the Pool RPC client.
/// This converts the client side data into a RPC request.
#[tonic::async_trait]
impl PoolOperations for PoolClient {
    async fn create(
        &self,
        create_pool_req: &dyn CreatePoolInfo,
        ctx: Option<Context>,
    ) -> Result<Pool, ReplyError> {
        let client = self.reconnect(ctx, MessageIdVs::CreatePool).await?;
        let req: CreatePoolRequest = create_pool_req.into();
        let response = client.clone().create_pool(req).await?.into_inner();
        match response.reply {
            Some(create_pool_reply) => match create_pool_reply {
                create_pool_reply::Reply::Pool(pool) => Ok(Pool::try_from(pool)?),
                create_pool_reply::Reply::Error(err) => Err(err.into()),
            },
            None => Err(ReplyError::invalid_response(ResourceKind::Replica)),
        }
    }

    /// Issue the pool destroy operation over RPC.
    async fn destroy(
        &self,
        destroy_pool_req: &dyn DestroyPoolInfo,
        ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        let client = self.reconnect(ctx, MessageIdVs::DestroyPool).await?;
        let req: DestroyPoolRequest = destroy_pool_req.into();
        let response = client.clone().destroy_pool(req).await?.into_inner();
        match response.error {
            None => Ok(()),
            Some(err) => Err(err.into()),
        }
    }

    async fn get(&self, filter: Filter, ctx: Option<Context>) -> Result<Pools, ReplyError> {
        let client = self.reconnect(ctx, MessageIdVs::GetPools).await?;
        let req: GetPoolsRequest = match filter {
            Filter::Node(id) => GetPoolsRequest {
                filter: Some(get_pools_request::Filter::Node(NodeFilter {
                    node_id: id.into(),
                })),
            },
            Filter::Pool(id) => GetPoolsRequest {
                filter: Some(get_pools_request::Filter::Pool(PoolFilter {
                    pool_id: id.into(),
                })),
            },
            Filter::NodePool(node_id, pool_id) => GetPoolsRequest {
                filter: Some(get_pools_request::Filter::NodePool(NodePoolFilter {
                    node_id: node_id.into(),
                    pool_id: pool_id.into(),
                })),
            },
            _ => GetPoolsRequest { filter: None },
        };
        let response = client.clone().get_pools(req).await?.into_inner();
        match response.reply {
            Some(get_pools_reply) => match get_pools_reply {
                get_pools_reply::Reply::Pools(pools) => Ok(Pools::try_from(pools)?),
                get_pools_reply::Reply::Error(err) => Err(err.into()),
            },
            None => Err(ReplyError::invalid_response(ResourceKind::Replica)),
        }
    }
}
