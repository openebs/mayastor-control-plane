//! Definition of node types that can be saved to the persistent store.
use crate::{
    types::v0::{
        openapi::models,
        store::definitions::{ObjectKey, StorableObject, StorableObjectType},
        transport::{self, HostNqn, NodeId},
    },
    IntoOption,
};
use pstor::ApiVersion;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Generic labels associated with the node.
pub type NodeLabels = HashMap<String, String>;

/// Data relating to the cordoning of a node.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CordonedState {
    /// Labels used to enforce a node cordon.
    pub cordonlabels: Vec<String>,
}

impl CordonedState {
    /// Create a new CordonedState object.
    pub fn new(cordonlabels: Vec<String>) -> Self {
        Self { cordonlabels }
    }
    /// Create a DrainState from a CordonedState.
    pub fn into_drain(&self, drainlabel: &str) -> DrainState {
        DrainState::new(self.cordonlabels.clone(), vec![String::from(drainlabel)])
    }
    /// Remove a cordon label from a CordonedState object.
    pub fn remove_label(&mut self, label: &str) {
        self.cordonlabels.retain(|l| l != label)
    }
}

/// Data relating to a draining or drained node, including non-drain cordon labels.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct DrainState {
    /// Labels used to enforce a node cordon.
    pub cordonlabels: Vec<String>,
    /// Labels used to cordon a node specifically for a drain.
    pub drainlabels: Vec<String>,
}

impl DrainState {
    /// Create a new DrainState object.
    pub fn new(cordonlabels: Vec<String>, drainlabels: Vec<String>) -> Self {
        Self {
            cordonlabels,
            drainlabels,
        }
    }
    /// Remove a label from a DrainState object.
    pub fn remove_label(&mut self, label: &str) {
        self.cordonlabels.retain(|l| l != label);
        self.drainlabels.retain(|l| l != label);
    }
    /// Add a drain label to a DrainState object.
    pub fn add_drain_label(&mut self, label: &str) {
        self.drainlabels.push(label.to_string());
    }
}

/// Enum variant encompassing data related to a cordoned or draining node.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub enum CordonDrainState {
    /// The node is being cordoned.
    Cordoned(CordonedState),
    /// The node is being drained.
    Draining(DrainState),
    /// The drain has completed.
    Drained(DrainState),
}

impl CordonDrainState {
    /// Add the given label to the cordon labels.
    pub fn add_cordon_label(&mut self, label: &str) {
        match self {
            CordonDrainState::Draining(state) => state.cordonlabels.push(label.to_string()),
            CordonDrainState::Drained(state) => state.cordonlabels.push(label.to_string()),
            CordonDrainState::Cordoned(state) => state.cordonlabels.push(label.to_string()),
        }
    }
    /// Returns a new Cordoned enum with the given cordon label.
    pub fn cordon(label: &str) -> Self {
        CordonDrainState::Cordoned(CordonedState::new(vec![String::from(label)]))
    }

    /// Returns whether the state has the specified cordon label.
    pub fn has_cordon_only_label(&self, label: &str) -> bool {
        match self {
            CordonDrainState::Draining(state) => state.cordonlabels.iter().any(|i| i == label),
            CordonDrainState::Drained(state) => state.cordonlabels.iter().any(|i| i == label),
            CordonDrainState::Cordoned(state) => state.cordonlabels.iter().any(|i| i == label),
        }
    }
    /// Returns whether the state has the specified drain label.
    pub fn has_drain_label(&self, label: &str) -> bool {
        match self {
            CordonDrainState::Draining(state) => state.drainlabels.iter().any(|i| i == label),
            CordonDrainState::Drained(state) => state.drainlabels.iter().any(|i| i == label),
            CordonDrainState::Cordoned(_) => false,
        }
    }
    /// Returns whether the state corresponds to draining.
    pub fn is_draining(&self) -> bool {
        match self {
            CordonDrainState::Cordoned(_) => false,
            CordonDrainState::Draining(_) => true,
            CordonDrainState::Drained(_) => false,
        }
    }
    /// Returns whether the state corresponds to drained.
    pub fn is_drained(&self) -> bool {
        match self {
            CordonDrainState::Cordoned(_) => false,
            CordonDrainState::Draining(_) => false,
            CordonDrainState::Drained(_) => true,
        }
    }
}

/// Node information.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Node {
    /// Node state information.
    node: transport::NodeState,
    /// Node labels.
    labels: NodeLabels,
}

/// Node state information.
pub struct NodeState {
    /// Node information.
    pub node: transport::NodeState,
}

/// Node spec data, including the cordon/drain state.
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct NodeSpec {
    /// Node identification.
    id: NodeId,
    /// Endpoint of the io-engine instance (gRPC).
    endpoint: std::net::SocketAddr,
    /// Node labels.
    labels: NodeLabels,
    /// Cordon/drain state.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)] // Ensure backwards compatibility in etcd when upgrading.
    cordon_drain_state: Option<CordonDrainState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)] // Ensure backwards compatibility in etcd when upgrading.
    node_nqn: Option<HostNqn>,
}

impl NodeSpec {
    /// Return a new `Self`.
    pub fn new(
        id: NodeId,
        endpoint: std::net::SocketAddr,
        labels: NodeLabels,
        cordon_drain_state: Option<CordonDrainState>,
        node_nqn: Option<HostNqn>,
    ) -> Self {
        Self {
            id,
            endpoint,
            labels,
            cordon_drain_state,
            node_nqn,
        }
    }
    /// Node Nvme HOSTNQN.
    pub fn node_nqn(&self) -> &Option<HostNqn> {
        &self.node_nqn
    }
    /// Node identification.
    pub fn id(&self) -> &NodeId {
        &self.id
    }
    /// Node gRPC endpoint.
    pub fn endpoint(&self) -> std::net::SocketAddr {
        self.endpoint
    }
    /// Node labels.
    pub fn labels(&self) -> &NodeLabels {
        &self.labels
    }
    /// Node labels.
    pub fn cordon_drain_state(&self) -> &Option<CordonDrainState> {
        &self.cordon_drain_state
    }

    /// Node gRPC endpoint.
    pub fn set_nqn(&mut self, nqn: Option<HostNqn>) {
        self.node_nqn = nqn;
    }
    /// Node gRPC endpoint.
    pub fn set_endpoint(&mut self, endpoint: std::net::SocketAddr) {
        self.endpoint = endpoint;
    }

    /// Ensure the state is consistent with the labels.
    pub fn resolve(&mut self) {
        match &mut self.cordon_drain_state {
            Some(ds) => match ds {
                CordonDrainState::Cordoned(state) => {
                    if state.cordonlabels.is_empty() {
                        self.cordon_drain_state = None;
                    }
                }
                CordonDrainState::Draining(state) | CordonDrainState::Drained(state) => {
                    // the drain labels should not be empty, if not - fix the state
                    if state.drainlabels.is_empty() {
                        if state.cordonlabels.is_empty() {
                            self.cordon_drain_state = None;
                        } else {
                            self.cordon_drain_state = Some(CordonDrainState::Cordoned(
                                CordonedState::new(state.cordonlabels.clone()),
                            ));
                        }
                    }
                }
            },
            None => {}
        }
    }

    /// Cordon node by applying the label.
    pub fn cordon(&mut self, label: String) {
        match &mut self.cordon_drain_state {
            Some(ds) => {
                ds.add_cordon_label(&label);
                self.resolve();
            }
            None => {
                //add the label and set the state to cordoned
                self.cordon_drain_state = Some(CordonDrainState::cordon(&label));
            }
        }
    }

    /// Drain node by applying the drain label.
    pub fn set_drain(&mut self, label: String) {
        // the the node has the label, return with an error
        match &mut self.cordon_drain_state {
            Some(ds) => match ds {
                CordonDrainState::Cordoned(state) => {
                    // set state to draining and add label
                    self.cordon_drain_state =
                        Some(CordonDrainState::Draining(state.into_drain(&label)))
                }
                CordonDrainState::Draining(state) => {
                    // add the label
                    state.add_drain_label(&label);
                }
                CordonDrainState::Drained(state) => {
                    // add the label
                    state.add_drain_label(&label);
                }
            },
            None => {
                //add the label and set the state to draining
                let cordonlabels = Vec::<String>::new();
                let drainlabels = vec![label];
                self.cordon_drain_state = Some(CordonDrainState::Draining(DrainState::new(
                    cordonlabels,
                    drainlabels,
                )));
            }
        }
    }

    /// Move state from Draining to Drained, no change to the labels.
    pub fn set_drained(&mut self) {
        if let Some(CordonDrainState::Draining(state)) = &mut self.cordon_drain_state {
            self.cordon_drain_state = Some(CordonDrainState::Drained(DrainState::new(
                state.cordonlabels.clone(),
                state.drainlabels.clone(),
            )));
            self.resolve();
        }
    }

    /// Uncordon node by removing the corresponding label.
    pub fn uncordon(&mut self, label: String) {
        match &mut self.cordon_drain_state {
            Some(ds) => match ds {
                CordonDrainState::Cordoned(state) => {
                    state.remove_label(&label);
                }
                CordonDrainState::Draining(state) | CordonDrainState::Drained(state) => {
                    state.remove_label(&label);
                }
            },
            None => {
                // should not be possible
            }
        }
        self.resolve();
    }

    /// Returns whether or not the node is cordoned.
    pub fn cordoned(&self) -> bool {
        self.cordon_drain_state.is_some()
    }

    /// Returns true if it has the label in either of the lists.
    pub fn has_cordon_label(&self, label: &str) -> bool {
        self.has_cordon_only_label(label) || self.has_drain_label(label)
    }

    /// Returns true if it has the label in the cordon list.
    pub fn has_cordon_only_label(&self, label: &str) -> bool {
        match &self.cordon_drain_state {
            Some(ds) => ds.has_cordon_only_label(label),
            None => false,
        }
    }

    /// Returns true if it has the label in the drain list.
    pub fn has_drain_label(&self, label: &str) -> bool {
        match &self.cordon_drain_state {
            Some(ds) => ds.has_drain_label(label),
            None => false,
        }
    }

    /// Returns true if the node is in the draining state.
    pub fn is_draining(&self) -> bool {
        match &self.cordon_drain_state {
            Some(ds) => ds.is_draining(),
            None => false,
        }
    }
    /// Returns true if the node is in the drained state.
    pub fn is_drained(&self) -> bool {
        match &self.cordon_drain_state {
            Some(ds) => ds.is_drained(),
            None => false,
        }
    }
}

impl From<NodeSpec> for models::NodeSpec {
    fn from(src: NodeSpec) -> Self {
        Self::new_all(
            src.endpoint.to_string(),
            src.id,
            src.cordon_drain_state.into_opt(),
            src.node_nqn.into_opt(),
        )
    }
}

impl From<CordonDrainState> for models::CordonDrainState {
    fn from(node_ds: CordonDrainState) -> Self {
        match node_ds {
            CordonDrainState::Cordoned(state) => {
                let cs = models::CordonedState {
                    cordonlabels: state.cordonlabels,
                };
                models::CordonDrainState::cordonedstate(cs)
            }
            CordonDrainState::Draining(state) => {
                let ds = models::DrainState {
                    cordonlabels: state.cordonlabels,
                    drainlabels: state.drainlabels,
                };
                models::CordonDrainState::drainingstate(ds)
            }
            CordonDrainState::Drained(state) => {
                let ds = models::DrainState {
                    cordonlabels: state.cordonlabels,
                    drainlabels: state.drainlabels,
                };
                models::CordonDrainState::drainedstate(ds)
            }
        }
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
    type Kind = StorableObjectType;

    fn version(&self) -> ApiVersion {
        ApiVersion::V0
    }

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
