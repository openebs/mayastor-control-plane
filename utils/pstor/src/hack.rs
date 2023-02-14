use crate::api::ObjectKey;
use strum_macros::{AsRefStr, Display};

/// All types of objects which are storable in our store.
#[derive(Display, AsRefStr, Copy, Clone, Debug)]
#[allow(dead_code)]
pub enum StorableObjectType {
    WatchConfig,
    Volume,
    Nexus,
    NexusSpec,
    NexusState,
    NexusInfo,
    Node,
    NodeSpec,
    Pool,
    PoolSpec,
    Replica,
    ReplicaState,
    ReplicaSpec,
    VolumeSpec,
    VolumeState,
    ChildSpec,
    ChildState,
    CoreRegistryConfig,
    StoreLeaseLock,
    StoreLeaseOwner,
    SwitchOver,
}

/// Prefix for all keys stored in the persistent store.
pub const ETCD_KEY_PREFIX: &str = "/openebs.io/mayastor";

/// Returns the key prefix that should is used for the keys, when running from within the cluster.
pub fn key_prefix(api_version: u64) -> String {
    build_key_prefix(platform::platform_info(), api_version)
}

/// Returns the key prefix that is used for the keys.
/// The platform info and namespace where the product is running must be specified.
pub fn build_key_prefix(cluster_uid: &dyn platform::PlatformInfo, api_version: u64) -> String {
    format!(
        "{}/apis/v{}/clusters/{}/namespaces/{}",
        ETCD_KEY_PREFIX,
        api_version,
        cluster_uid.uid(),
        cluster_uid.namespace()
    )
}
/// Returns the control plane prefix that should be used for the keys, in conjunction
/// with a `StorableObjectType` type.
pub fn key_prefix_obj<K: AsRef<str>>(key_type: K, api_version: u64) -> String {
    format!("{}/{}", key_prefix(api_version), key_type.as_ref())
}

/// Create a key based on the object's key trait.
pub fn generate_key<K: ObjectKey + ?Sized>(k: &K) -> String {
    format!(
        "{}/{}",
        key_prefix_obj(k.key_type(), k.version()),
        k.key_uuid()
    )
}
