use crate::types::v0::{
    store::definitions::{ObjectKey, StorableObject, StorableObjectType},
    transport::NexusVersion,
};
use pstor::ApiVersion;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Registry configuration loaded from/stored into the persistent store
#[derive(Serialize, Deserialize, Debug, Clone)]
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
    /// The version of the volume label.
    /// Volumes will be created with this version of the label.
    /// This should only be changed when all nodes support the new version of the label,
    /// otherwise volumes may not be accessible on all nodes.
    #[serde(default, rename = "volumeLabelVersion")]
    volume_label_version: NexusVersion,
    #[serde(skip)]
    store_dirty: bool,
}

impl CoreRegistryConfig {
    /// Return a new `Self` with the provided id and registration type
    pub fn new(registration: NodeRegistration) -> Self {
        Self {
            id: CoreRegistryConfigKey::default(),
            registration,
            mayastor_compat_v1: None,
            volume_label_version: NexusVersion::V1,
            store_dirty: false,
        }
    }
    /// Get the `mayastor_compat_v1`.
    pub fn mayastor_compat_v1(&self) -> bool {
        self.mayastor_compat_v1.unwrap_or(false)
    }
    /// Set the `mayastor_compat_v1` to true.
    pub fn set_mayastor_compat_v1(&mut self, value: bool) {
        let val = match value {
            true => Some(true),
            false => None,
        };
        self.mayastor_compat_v1 = val;
    }
    /// Get a reference to the `NodeRegistration`
    pub fn node_registration(&self) -> &NodeRegistration {
        &self.registration
    }
    /// Get the version of the volume label.
    pub fn volume_version(&self) -> NexusVersion {
        self.volume_label_version
    }
    /// Set the version of the volume label.
    ///
    /// Returns:
    /// * `Ok(None)` if the version is already set
    /// * `Ok(Some(existing))` if the existing version is lower than the provided version
    /// * `Err(existing)` if the existing version is greater than the provided version  \
    ///   This should not happen, but we want to be safe.
    pub fn set_volume_version(
        &mut self,
        version: NexusVersion,
    ) -> Result<Option<NexusVersion>, NexusVersion> {
        if version > self.volume_label_version {
            let old = self.volume_label_version;
            self.volume_label_version = version;
            Ok(Some(old))
        } else if version < self.volume_label_version {
            Err(self.volume_label_version)
        } else {
            Ok(None)
        }
    }

    /// Is the configuration dirty and needs to be stored in the persistent store?
    pub fn is_dirty(&self) -> bool {
        self.store_dirty
    }
    /// Set the dirty flag to indicate that the configuration needs to be stored in the persistent store
    pub fn set_dirty(&mut self, dirty: bool) {
        self.store_dirty = dirty;
    }
}

/// How the Node Registration is handled
#[derive(Serialize, Deserialize, Debug, Default, PartialEq, Clone)]
pub enum NodeRegistration {
    /// Nodes have to be registered via the RestApi before they can be used.
    Manual,
    /// Nodes are automatically registered when a Register message is received from an
    /// io-engine instance.
    /// They can be explicitly removed via the RestApi.
    #[default]
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
    type Kind = StorableObjectType;

    fn version(&self) -> ApiVersion {
        ApiVersion::V0
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

    fn version(&self) -> ApiVersion {
        ApiVersion::V0
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

    fn version(&self) -> ApiVersion {
        ApiVersion::V0
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
