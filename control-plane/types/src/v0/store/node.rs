//! Definition of node types that can be saved to the persistent store.

use crate::v0::{
    message_bus::{mbus, mbus::NodeId},
    store::definitions::{ObjectKey, StorableObject, StorableObjectType},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

type NodeLabels = HashMap<String, String>;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Node {
    /// Node information.
    node: mbus::Node,
    /// Node labels.
    labels: NodeLabels,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct NodeSpec {
    /// Node identification.
    id: NodeId,
    /// Node labels.
    labels: NodeLabels,
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
