use std::net::SocketAddr;

use super::*;

#[derive(Debug)]
pub struct NodeAgentInfo {
    /// node name where node-agent is running
    node_name: String,
    /// endpoint of node-agent GRPC server
    endpoint: SocketAddr,
}

impl NodeAgentInfo {
    pub fn new(node_name: String, endpoint: SocketAddr) -> Self {
        NodeAgentInfo {
            node_name,
            endpoint,
        }
    }

    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    pub fn endpoint(&self) -> SocketAddr {
        self.endpoint
    }
}
