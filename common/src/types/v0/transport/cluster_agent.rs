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

/// Failed NVMe path.
#[derive(Debug, Clone)]
pub struct FailedPath {
    target_nqn: String,
}

impl FailedPath {
    /// Create a new instance of FailedPath for a given NVMe target NQN.
    pub fn new(target_nqn: String) -> Self {
        Self { target_nqn }
    }

    /// Get target NQN.
    pub fn target_nqn(&self) -> &str {
        &self.target_nqn
    }
}

/// Report failed NVMe paths.
#[derive(Debug)]
pub struct ReportFailedPaths {
    node: String,
    failed_paths: Vec<FailedPath>,
}

impl ReportFailedPaths {
    pub fn new(node: String, failed_paths: Vec<FailedPath>) -> Self {
        Self { node, failed_paths }
    }

    pub fn node_name(&self) -> &str {
        &self.node
    }

    pub fn failed_paths(&self) -> &Vec<FailedPath> {
        &self.failed_paths
    }
}
