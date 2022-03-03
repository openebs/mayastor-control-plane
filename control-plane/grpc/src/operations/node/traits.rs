use crate::context::Context;
use common_lib::{
    mbus_api::{v0::Nodes, ReplyError, ResourceKind},
    types::v0::{
        message_bus::{Filter, Node, NodeState, NodeStatus},
        store::node::NodeSpec,
    },
};
use std::convert::TryFrom;

use crate::{node, node::get_nodes_request};

/// Trait implemented by services which support node operations.
#[tonic::async_trait]
pub trait NodeOperations: Send + Sync {
    /// Get nodes based on the filters
    async fn get(&self, filter: Filter, ctx: Option<Context>) -> Result<Nodes, ReplyError>;
    /// Liveness probe for node service
    async fn probe(&self, ctx: Option<Context>) -> Result<bool, ReplyError>;
}

impl TryFrom<node::Node> for Node {
    type Error = ReplyError;
    fn try_from(node_grpc_type: node::Node) -> Result<Self, Self::Error> {
        let node_spec = node_grpc_type.spec.map(|spec| {
            NodeSpec::new(
                spec.node_id.into(),
                spec.endpoint,
                spec.labels.unwrap_or_default().value,
            )
        });
        let node_state = match node_grpc_type.state {
            Some(state) => {
                let status: NodeStatus = match node::NodeStatus::from_i32(state.status) {
                    Some(status) => status.into(),
                    None => {
                        return Err(ReplyError::invalid_argument(
                            ResourceKind::Node,
                            "node.state.status",
                            "".to_string(),
                        ))
                    }
                };
                Some(NodeState::new(state.node_id.into(), state.endpoint, status))
            }
            None => None,
        };
        Ok(Node::new(
            node_grpc_type.node_id.into(),
            node_spec,
            node_state,
        ))
    }
}

impl From<Node> for node::Node {
    fn from(node: Node) -> Self {
        let node_spec = node.spec().map(|spec| node::NodeSpec {
            node_id: spec.id().to_string(),
            endpoint: spec.endpoint().to_string(),
            labels: Some(crate::common::StringMapValue {
                value: spec.labels().clone(),
            }),
        });
        let node_state = match node.state() {
            None => None,
            Some(state) => {
                let status: node::NodeStatus = state.status.clone().into();
                Some(node::NodeState {
                    node_id: state.id.to_string(),
                    endpoint: state.grpc_endpoint.to_string(),
                    status: status as i32,
                })
            }
        };
        node::Node {
            node_id: node.id().to_string(),
            spec: node_spec,
            state: node_state,
        }
    }
}

impl TryFrom<node::Nodes> for Nodes {
    type Error = ReplyError;
    fn try_from(grpc_nodes: node::Nodes) -> Result<Self, Self::Error> {
        let mut nodes: Vec<Node> = vec![];
        for node in grpc_nodes.nodes {
            nodes.push(Node::try_from(node)?)
        }
        Ok(Nodes(nodes))
    }
}

impl From<Nodes> for node::Nodes {
    fn from(nodes: Nodes) -> Self {
        node::Nodes {
            nodes: nodes
                .into_inner()
                .into_iter()
                .map(|node| node.into())
                .collect(),
        }
    }
}

impl From<get_nodes_request::Filter> for Filter {
    fn from(filter: get_nodes_request::Filter) -> Self {
        match filter {
            get_nodes_request::Filter::Node(node_filter) => {
                Filter::Node(node_filter.node_id.into())
            }
        }
    }
}

impl From<node::NodeStatus> for NodeStatus {
    fn from(src: node::NodeStatus) -> Self {
        match src {
            node::NodeStatus::Unknown => Self::Unknown,
            node::NodeStatus::Online => Self::Online,
            node::NodeStatus::Offline => Self::Offline,
        }
    }
}

impl From<NodeStatus> for node::NodeStatus {
    fn from(src: NodeStatus) -> Self {
        match src {
            NodeStatus::Unknown => Self::Unknown,
            NodeStatus::Online => Self::Online,
            NodeStatus::Offline => Self::Offline,
        }
    }
}
