use std::{collections::HashMap, net::SocketAddr};

use super::*;

#[derive(Debug)]
pub struct NodeAgentInfo {
    /// node name where node-agent is running
    node_name: String,
    /// endpoint of node-agent GRPC server
    endpoint: SocketAddr,
}

impl NodeAgentInfo {
    /// Creates an instance containing node name and node agent's grpc endpoint. Used for
    /// registering node agent with cluster agent.
    pub fn new(node_name: String, endpoint: SocketAddr) -> Self {
        NodeAgentInfo {
            node_name,
            endpoint,
        }
    }

    /// Get node name.
    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    /// Get node agents grpc address.
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
    /// Create a new instance with FailedPath for a given NVMe target NQN.
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
    endpoint: SocketAddr,
    failed_paths: Vec<FailedPath>,
}

impl ReportFailedPaths {
    /// Creates instance listing failed paths, reporting node id and node agent's grpc address.
    pub fn new(node: String, failed_paths: Vec<FailedPath>, endpoint: SocketAddr) -> Self {
        Self {
            node,
            failed_paths,
            endpoint,
        }
    }

    /// Get node name reporting Nvme path failures.
    pub fn node_name(&self) -> &str {
        &self.node
    }

    /// Get the grpc address of node reporting failed paths.
    pub fn endpoint(&self) -> SocketAddr {
        self.endpoint
    }

    /// Get list of all failed paths in the request.
    pub fn failed_paths(&self) -> &Vec<FailedPath> {
        &self.failed_paths
    }
}

#[derive(Debug)]
pub struct ReplacePath {
    target_nqn: String,
    new_path: String,
    publish_context: Option<HashMap<String, String>>,
}

impl ReplacePath {
    /// Creates an instance containing failed and new nexus path to be reported back to node agent
    /// for Nvme connect.
    pub fn new(
        target_nqn: String,
        new_path: String,
        publish_context: Option<HashMap<String, String>>,
    ) -> Self {
        Self {
            target_nqn,
            new_path,
            publish_context,
        }
    }

    /// Get failed nexus path.
    pub fn target(&self) -> &str {
        &self.target_nqn
    }

    /// Get newly published nexus path.
    pub fn new_path(&self) -> &str {
        &self.new_path
    }

    /// Get the publish context.
    pub fn publish_context(&self) -> Option<HashMap<String, String>> {
        self.publish_context.clone()
    }
}
