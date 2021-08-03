use crate::types::v0::store::definitions::{ObjectKey, StorableObject, StorableObjectType};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Registry configuration loaded from/stored into the persistent store
#[derive(Serialize, Deserialize, Debug)]
pub struct CoreRegistryConfig {
    /// Key of this configuration
    id: CoreRegistryConfigKey,
    /// Node registration
    registration: NodeRegistration,
}

impl CoreRegistryConfig {
    /// Return a new `Self` with the provided id and registration type
    pub fn new(registration: NodeRegistration) -> Self {
        Self {
            id: CoreRegistryConfigKey::default(),
            registration,
        }
    }
    /// Get a reference to the `NodeRegistration`
    pub fn node_registration(&self) -> &NodeRegistration {
        &self.registration
    }
}

/// How the Node Registration is handled
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub enum NodeRegistration {
    /// Nodes have to be registered via the RestApi before they can be used.
    Manual,
    /// Nodes are automatically registered when a Register message is received from a
    /// mayastor instance.
    /// They can be explicitly removed via the RestApi.
    Automatic,
}
impl NodeRegistration {
    pub fn automatic(&self) -> bool {
        self == &Self::Automatic
    }
}

/// Key used to store core registry configuration data
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CoreRegistryConfigKey(String);

const CORE_REGISTRY_CONFIG_KEY_DFLT: &str = "db98f8bb-4afc-45d0-85b9-24c99cc443f2";
impl Default for CoreRegistryConfigKey {
    fn default() -> Self {
        Self(CORE_REGISTRY_CONFIG_KEY_DFLT.to_string())
    }
}

impl From<&str> for CoreRegistryConfigKey {
    fn from(id: &str) -> Self {
        Self(id.to_string())
    }
}

impl ObjectKey for CoreRegistryConfigKey {
    fn key_type(&self) -> StorableObjectType {
        StorableObjectType::CoreRegistryConfig
    }

    fn key_uuid(&self) -> String {
        self.0.to_string()
    }
}

impl StorableObject for CoreRegistryConfig {
    type Key = CoreRegistryConfigKey;

    fn key(&self) -> Self::Key {
        self.id.clone()
    }
}
