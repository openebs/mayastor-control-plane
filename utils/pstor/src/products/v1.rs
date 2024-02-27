//! This module denotes the 1.x versions of the product and has the implementations
//! of the etcd key space management for the 1.x versions of product only. The v1 here
//! does not denote the data-plane or control-plane api version.

use crate::api::Store;

/// Returns the key prefix that is used for the keys, when running from within the cluster.
pub fn key_prefix() -> String {
    build_key_prefix(platform::platform_info())
}

/// Returns the key prefix that is used for the keys.
/// The platform info and namespace where the product is running must be specified.
fn build_key_prefix(cluster_uid: &dyn platform::PlatformInfo) -> String {
    format!("/namespace/{}/control-plane", cluster_uid.namespace())
}

/// Returns the control plane prefix that is used for the keys, in conjunction
/// with a `StorableObjectType` type.
pub fn key_prefix_obj<K: AsRef<str>>(key_type: K) -> String {
    format!("{}/{}", key_prefix(), key_type.as_ref())
}

/// Fetches the product v1 key prefix and returns true if entry is present.
pub async fn detect_product_v1_prefix<S: Store>(store: &mut S) -> Result<bool, crate::Error> {
    let prefix = store.get_values_paged_all(&key_prefix(), 3).await?;
    Ok(!prefix.is_empty())
}
