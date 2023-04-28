use super::*;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, net::SocketAddr};

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

    /// Remove path from request.
    pub fn remove_path(&mut self, path: &str) {
        self.failed_paths.retain(|p| p.target_nqn != path);
    }
}

/// Report failed NVMe paths.
#[derive(Default, Debug)]
pub struct FailedPathsResponse {
    failed_paths: Vec<FailedPathResponse>,
}
impl FailedPathsResponse {
    /// Create a new `Self`.
    pub fn new(failed_paths: Vec<FailedPathResponse>) -> Self {
        Self { failed_paths }
    }
    /// Adds a new failed path.
    pub fn push(&mut self, status_code: tonic::Code, failed_path: &str) {
        self.failed_paths
            .push(FailedPathResponse::new(status_code, failed_path.into()));
    }
    /// Check if they're no reported paths.
    pub fn is_empty(&self) -> bool {
        self.failed_paths.is_empty()
    }
    /// Check if all path reporting succeeded.
    pub fn is_all_ok(&self) -> bool {
        self.failed_paths
            .iter()
            .all(|p| p.status_code == tonic::Code::Ok)
    }
    /// Convert into a list of failed paths.
    pub fn into_failed_paths(self) -> Vec<FailedPathResponse> {
        self.failed_paths
    }
}
/// A specific failed path response.
#[derive(Debug)]
pub struct FailedPathResponse {
    /// The status code related to this report failure.
    pub status_code: tonic::Code,
    /// The failed path reported.
    pub failed_nqn: String,
}
impl FailedPathResponse {
    /// Create a new `Self`.
    pub fn new(status_code: tonic::Code, failed_nqn: String) -> Self {
        Self {
            status_code,
            failed_nqn,
        }
    }
}

/// Request struct to get Nvme subsystems registered for a given nqn.
#[derive(Debug)]
pub struct GetController {
    /// nvme_path is the device uri of the target associated with volume.
    nvme_path: String,
}

impl GetController {
    /// Constructor to create instance of the struct.
    pub fn new(nvme_path: String) -> Self {
        Self { nvme_path }
    }
    /// Get nvme path.
    pub fn nvme_path(&self) -> String {
        self.nvme_path.clone()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Represents targets address of Nvme Subsystem.
pub struct NvmeSubsystem {
    /// Address is the IP address of the Nvme Subsystem.
    address: String,
}

impl NvmeSubsystem {
    /// Creates an instance of this struct.
    pub fn new(target_addr: String) -> Self {
        Self {
            address: target_addr,
        }
    }
    /// Get IP address of the Nvme Subsystem.
    pub fn address(&self) -> &str {
        self.address.as_str()
    }
    /// Get IP address of the Nvme Subsystem.
    pub fn into_address(self) -> String {
        self.address
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
