use crate::{
    blockdevice::get_block_devices_reply,
    common::NodeFilter,
    context::{Client, Context, TracedChannel},
    node::{
        get_nodes_reply, get_nodes_request, node_grpc_client::NodeGrpcClient, GetNodesRequest,
        ProbeRequest,
    },
    operations::node::traits::{GetBlockDeviceInfo, NodeOperations},
};
use common_lib::{
    mbus_api::{
        v0::{BlockDevices, Nodes},
        ReplyError, ResourceKind, TimeoutOptions,
    },
    types::v0::message_bus::{Filter, MessageIdVs},
};
use std::{convert::TryFrom, ops::Deref};
use tonic::transport::Uri;

/// RPC Node Client
#[derive(Clone)]
pub struct NodeClient {
    inner: Client<NodeGrpcClient<TracedChannel>>,
}
impl Deref for NodeClient {
    type Target = Client<NodeGrpcClient<TracedChannel>>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl NodeClient {
    /// creates a new base tonic endpoint with the timeout options and the address
    pub async fn new<O: Into<Option<TimeoutOptions>>>(addr: Uri, opts: O) -> Self {
        let client = Client::new(addr, opts, NodeGrpcClient::new).await;
        Self { inner: client }
    }
}

#[tonic::async_trait]
impl NodeOperations for NodeClient {
    #[tracing::instrument(name = "NodeClient::get", level = "debug", skip(self), err)]
    async fn get(&self, filter: Filter, ctx: Option<Context>) -> Result<Nodes, ReplyError> {
        let req: GetNodesRequest = match filter {
            Filter::Node(id) => GetNodesRequest {
                filter: Some(get_nodes_request::Filter::Node(NodeFilter {
                    node_id: id.into(),
                })),
            },
            _ => GetNodesRequest { filter: None },
        };
        let req = self.request(req, ctx, MessageIdVs::GetNodes);
        let response = self.client().get_nodes(req).await?.into_inner();
        match response.reply {
            Some(get_nodes_reply) => match get_nodes_reply {
                get_nodes_reply::Reply::Nodes(nodes) => Ok(Nodes::try_from(nodes)?),
                get_nodes_reply::Reply::Error(err) => Err(err.into()),
            },
            None => Err(ReplyError::invalid_response(ResourceKind::Node)),
        }
    }
    #[tracing::instrument(name = "NodeClient::probe", level = "debug", skip(self), err)]
    async fn probe(&self, _ctx: Option<Context>) -> Result<bool, ReplyError> {
        match self.client().probe(ProbeRequest {}).await {
            Ok(resp) => Ok(resp.into_inner().ready),
            Err(e) => Err(e.into()),
        }
    }
    async fn get_block_devices(
        &self,
        request: &dyn GetBlockDeviceInfo,
        ctx: Option<Context>,
    ) -> Result<BlockDevices, ReplyError> {
        let req = self.request(request, ctx, MessageIdVs::GetBlockDevices);
        let response = self.client().get_block_devices(req).await?.into_inner();
        match response.reply {
            Some(get_block_devices_reply) => match get_block_devices_reply {
                get_block_devices_reply::Reply::Blockdevices(blockdevices) => {
                    Ok(BlockDevices::try_from(blockdevices)?)
                }
                get_block_devices_reply::Reply::Error(err) => Err(err.into()),
            },
            None => Err(ReplyError::invalid_response(ResourceKind::Block)),
        }
    }
}
