//! Definition of node types that can be saved to the persistent store.

use crate::types::v0::{
    openapi::models,
    store::{
        definitions::{ObjectKey, StorableObject, StorableObjectType},
        ResourceUuid,
    },
    transport::{self, NodeId},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type NodeLabels = HashMap<String, String>;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Node {
    /// Node information.
    node: transport::NodeState,
    /// Node labels.
    labels: NodeLabels,
}

pub struct NodeState {
    /// Node information
    pub node: transport::NodeState,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Default, Clone)]
pub struct NodeSpec {
    /// Node identification.
    id: NodeId,
    /// Endpoint of the io-engine instance (gRPC)
    endpoint: String,
    /// Node labels.
    labels: NodeLabels,
    /// Cordon labels.
    #[serde(default)] // Ensure backwards compatibility in etcd when upgrading.
    cordon_labels: Vec<String>,
    /// Drain labels.
    #[serde(default)] // Ensure backwards compatibility in etcd when upgrading.
    drain_labels: Vec<String>,
}

impl NodeSpec {
    /// Return a new `Self`
    pub fn new(
        id: NodeId,
        endpoint: String,
        labels: NodeLabels,
        cordon_label: Option<Vec<String>>,
        drain_label: Option<Vec<String>>,
    ) -> Self {
        Self {
            id,
            endpoint,
            labels,
            cordon_labels: cordon_label.unwrap_or_default(),
            drain_labels: drain_label.unwrap_or_default(),
        }
    }
    /// Node identification
    pub fn id(&self) -> &NodeId {
        &self.id
    }
    /// Node gRPC endpoint
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
    /// Node labels
    pub fn labels(&self) -> &NodeLabels {
        &self.labels
    }
    /// Node gRPC endpoint
    pub fn set_endpoint(&mut self, endpoint: String) {
        self.endpoint = endpoint
    }
    /// Cordon node by applying the label.
    pub fn cordon(&mut self, label: String) {
        self.cordon_labels.push(label);
    }
    /// Drain node by applying the drain label.
    pub fn drain(&mut self, label: String) {
        self.drain_labels.push(label);
    }
    /// Uncordon node by removing the corresponding label.
    pub fn uncordon(&mut self, label: String) {
        if let Some(index) = self.cordon_labels.iter().position(|l| l == &label) {
            self.cordon_labels.remove(index);
        }
        if let Some(index) = self.drain_labels.iter().position(|l| l == &label) {
            self.drain_labels.remove(index);
        }
    }
    /// Returns whether or not the node is cordoned.
    pub fn cordoned(&self) -> bool {
        !self.cordon_labels.is_empty() || !self.drain_labels.is_empty()
    }
    /// Returns whether or not the node is cordoned for drain.
    pub fn cordoned_for_drain(&self) -> bool {
        !self.drain_labels.is_empty()
    }
    /// Returns the cordon labels
    pub fn cordon_labels(&self) -> Vec<String> {
        self.cordon_labels.clone()
    }
    /// Returns the drain labels
    pub fn drain_labels(&self) -> Vec<String> {
        self.drain_labels.clone()
    }
    pub fn has_cordon_label(&self, label: String) -> bool {
        if self.cordon_labels.contains(&label) {
            return true;
        }
        if self.drain_labels.contains(&label) {
            return true;
        }
        false
    }
}

impl From<NodeSpec> for models::NodeSpec {
    fn from(src: NodeSpec) -> Self {
        Self::new(src.endpoint, src.id, src.cordon_labels, src.drain_labels)
    }
}

impl ResourceUuid for NodeSpec {
    type Id = NodeId;
    fn uuid(&self) -> Self::Id {
        self.id.clone()
    }
}

/// Key used by the store to uniquely identify a NodeSpec structure.
pub struct NodeSpecKey(NodeId);

impl From<&NodeId> for NodeSpecKey {
    fn from(id: &NodeId) -> Self {
        Self(id.clone())
    }
}

impl ObjectKey for NodeSpecKey {
    fn key_type(&self) -> StorableObjectType {
        StorableObjectType::NodeSpec
    }

    fn key_uuid(&self) -> String {
        self.0.to_string()
    }
}

impl StorableObject for NodeSpec {
    type Key = NodeSpecKey;

    fn key(&self) -> Self::Key {
        NodeSpecKey(self.id.clone())
    }
}
