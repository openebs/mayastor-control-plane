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
    /// Also query healthy replicas info from etcd on the v1 path where mayastor v1 used to it, at
    /// the root of etcd, eg: "7a43f237-b2f8-4070-ac37-18df0bd7b115"
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    mayastor_compat_v1: Option<bool>,
}

impl CoreRegistryConfig {
    /// Return a new `Self` with the provided id and registration type
    pub fn new(registration: NodeRegistration) -> Self {
        Self {
            id: CoreRegistryConfigKey::default(),
            registration,
            mayastor_compat_v1: None,
        }
    }
    /// Get the `mayastor_compat_v1`.
    pub fn mayastor_compat_v1(&self) -> bool {
        self.mayastor_compat_v1.unwrap_or(false)
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
    /// Nodes are automatically registered when a Register message is received from an
    /// io-engine instance.
    /// They can be explicitly removed via the RestApi.
    Automatic,
}
impl NodeRegistration {
    pub fn automatic(&self) -> bool {
        self == &Self::Automatic
    }
}
impl Default for NodeRegistration {
    fn default() -> Self {
        Self::Automatic
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
    type Kind = StorableObjectType;

    fn version(&self) -> u64 {
        0
    }
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

/// Service Name used by the store client library
#[derive(Serialize, Deserialize, Debug, Clone, strum_macros::Display)]
pub enum ControlPlaneService {
    CoreAgent,
}

/// Key used by the store lock api to identify the lock.
/// The key is deleted when the lock is unlocked or if the lease is lost.
#[derive(Debug)]
pub struct StoreLeaseLockKey(ControlPlaneService);
impl StoreLeaseLockKey {
    /// return new `Self` with `name`
    pub fn new(name: &ControlPlaneService) -> Self {
        Self(name.clone())
    }
}
impl ObjectKey for StoreLeaseLockKey {
    type Kind = StorableObjectType;

    fn version(&self) -> u64 {
        0
    }
    fn key_type(&self) -> StorableObjectType {
        StorableObjectType::StoreLeaseLock
    }
    fn key_uuid(&self) -> String {
        self.0.to_string()
    }
}

/// Key used to store the last owner ref.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StoreLeaseOwnerKey(ControlPlaneService);
impl StoreLeaseOwnerKey {
    /// return new `Self` with `kind`
    pub fn new(kind: &ControlPlaneService) -> Self {
        Self(kind.clone())
    }
}
impl ObjectKey for StoreLeaseOwnerKey {
    type Kind = StorableObjectType;

    fn version(&self) -> u64 {
        0
    }
    fn key_type(&self) -> StorableObjectType {
        StorableObjectType::StoreLeaseOwner
    }
    fn key_uuid(&self) -> String {
        self.0.to_string()
    }
}

/// A lease owner is the service instance which owns a lease
#[derive(Serialize, Deserialize, Debug)]
pub struct StoreLeaseOwner {
    kind: ControlPlaneService,
    lease_id: String,
    instance_name: String,
}
impl StoreLeaseOwner {
    /// return new `Self` with `kind` and `lease_id`
    pub fn new(kind: &ControlPlaneService, lease_id: i64) -> Self {
        Self {
            kind: kind.clone(),
            lease_id: format!("{lease_id:x}"),
            instance_name: std::env::var("MY_POD_NAME").unwrap_or_default(),
        }
    }
    /// Get the `lease_id` as a hex string
    pub fn lease_id(&self) -> &str {
        &self.lease_id
    }
}
impl StorableObject for StoreLeaseOwner {
    type Key = StoreLeaseOwnerKey;

    fn key(&self) -> Self::Key {
        Self::Key::new(&self.kind)
    }
}
