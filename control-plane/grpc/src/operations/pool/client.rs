use crate::{
    common::{NodeFilter, NodePoolFilter, PoolFilter},
    grpc_opts::{Client, Context},
    operations::pool::traits::{CreatePoolInfo, DestroyPoolInfo, PoolOperations},
    pool::{
        create_pool_reply, get_pools_reply, get_pools_request, pool_grpc_client::PoolGrpcClient,
        GetPoolsRequest,
    },
};
use common_lib::{
    mbus_api::{v0::Pools, ReplyError, ResourceKind, TimeoutOptions},
    types::v0::message_bus::{Filter, MessageIdVs, Pool},
};
use std::{convert::TryFrom, ops::Deref};
use tonic::transport::{Channel, Uri};

/// RPC Pool Client
#[derive(Clone)]
pub struct PoolClient {
    inner: Client<PoolGrpcClient<Channel>>,
}
impl Deref for PoolClient {
    type Target = Client<PoolGrpcClient<Channel>>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl PoolClient {
    /// creates a new base tonic endpoint with the timeout options and the address
    pub async fn new<O: Into<Option<TimeoutOptions>>>(addr: Uri, opts: O) -> Self {
        let client = Client::new(addr, opts, PoolGrpcClient::new).await;
        Self { inner: client }
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
        let req = self.request(create_pool_req, ctx, MessageIdVs::CreatePool);
        let response = self.client().create_pool(req).await?.into_inner();
        match response.reply {
            Some(create_pool_reply) => match create_pool_reply {
                create_pool_reply::Reply::Pool(pool) => Ok(Pool::try_from(pool)?),
                create_pool_reply::Reply::Error(err) => Err(err.into()),
            },
            None => Err(ReplyError::invalid_response(ResourceKind::Pool)),
        }
    }

    /// Issue the pool destroy operation over RPC.
    async fn destroy(
        &self,
        destroy_pool_req: &dyn DestroyPoolInfo,
        ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        let req = self.request(destroy_pool_req, ctx, MessageIdVs::DestroyPool);
        let response = self.client().destroy_pool(req).await?.into_inner();
        match response.error {
            None => Ok(()),
            Some(err) => Err(err.into()),
        }
    }

    async fn get(&self, filter: Filter, ctx: Option<Context>) -> Result<Pools, ReplyError> {
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
        let req = self.request(req, ctx, MessageIdVs::GetPools);
        let response = self.client().get_pools(req).await?.into_inner();
        match response.reply {
            Some(get_pools_reply) => match get_pools_reply {
                get_pools_reply::Reply::Pools(pools) => Ok(Pools::try_from(pools)?),
                get_pools_reply::Reply::Error(err) => Err(err.into()),
            },
            None => Err(ReplyError::invalid_response(ResourceKind::Pool)),
        }
    }
}
