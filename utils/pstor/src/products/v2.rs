//! This module denotes the 2.x(current) versions of the product and has the implementations
//! of the etcd key space management for the 2.x(current) versions of product specifically.
//! The v2 here does not denote the data-plane or control-plane api version.

use crate::{api::ObjectKey, common::ApiVersion};

/// The api version.
pub const API_VERSION: ApiVersion = ApiVersion::V0;

/// Returns the key prefix that should is used for the keys, when running from within the cluster.
pub fn key_prefix(api_version: ApiVersion) -> String {
    build_key_prefix(platform::platform_info(), api_version)
}

/// Returns the key prefix that is used for the keys.
/// The platform info and namespace where the product is running must be specified.
pub fn build_key_prefix(
    cluster_uid: &dyn platform::PlatformInfo,
    api_version: ApiVersion,
) -> String {
    format!(
        "/{}/{}/apis/{}/clusters/{}/namespaces/{}",
        utils::PRODUCT_DOMAIN_NAME,
        utils::PRODUCT_NAME,
        api_version,
        cluster_uid.uid(),
        cluster_uid.namespace()
    )
}

/// Returns the control plane prefix that should be used for the keys, in conjunction
/// with a `StorableObjectType` type.
pub fn key_prefix_obj<K: AsRef<str>>(key_type: K, api_version: ApiVersion) -> String {
    format!("{}/{}", key_prefix(api_version), key_type.as_ref())
}

/// Create a key based on the object's key trait.
pub(crate) fn generate_key<K: ObjectKey + ?Sized>(k: &K) -> String {
    format!(
        "{}/{}",
        key_prefix_obj(k.key_type(), k.version()),
        k.key_uuid()
    )
}
