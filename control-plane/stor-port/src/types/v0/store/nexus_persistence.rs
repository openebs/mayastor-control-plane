use crate::types::v0::{
    store::definitions::{key_prefix, ObjectKey, StorableObject, StorableObjectType, StoreError},
    transport::{NexusId, ReplicaId, VolumeId},
};
use pstor::{ApiVersion, Store};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use tracing::info;
use uuid::Uuid;

/// Definition of the nexus information that gets saved in the persistent
/// store.
#[derive(Serialize, Deserialize, Debug, Default, Clone, Eq, PartialEq)]
pub struct NexusInfo {
    #[serde(skip)]
    /// uuid of the Nexus
    pub uuid: NexusId,
    #[serde(skip)]
    /// uuid of the Volume
    pub volume_uuid: Option<VolumeId>,
    /// Nexus destroyed successfully.
    pub clean_shutdown: bool,
    /// Information about children.
    pub children: Vec<ChildInfo>,
}

impl NexusInfo {
    /// Check if the provided replica is healthy or not
    pub fn is_replica_healthy(&self, replica: &ReplicaId) -> bool {
        match self.children.iter().find(|c| &c.uuid == replica) {
            Some(info) => info.healthy,
            None => false,
        }
    }

    /// Check if no replica is healthy
    pub fn no_healthy_replicas(&self) -> bool {
        self.children.iter().all(|c| !c.healthy) || self.children.is_empty()
    }
}

/// Definition of the child information that gets saved in the persistent
/// store.
#[derive(Serialize, Deserialize, Debug, Default, Clone, Eq, PartialEq)]
pub struct ChildInfo {
    /// UUID of the child.
    pub uuid: ReplicaId,
    /// Child's state of health.
    pub healthy: bool,
}

/// Key used by the store to uniquely identify a NexusInfo structure.
/// The volume is optional because a nexus can be created which is not associated with a volume.
pub struct NexusInfoKey {
    volume_id: Option<VolumeId>,
    nexus_id: NexusId,
    mayastor_compat_v1: bool,
    rm_all_nexuses: bool,
}

impl NexusInfoKey {
    /// Create a new NexusInfoKey.
    pub fn new(volume_id: &Option<VolumeId>, nexus_id: &NexusId) -> Self {
        Self {
            volume_id: volume_id.clone(),
            nexus_id: nexus_id.clone(),
            mayastor_compat_v1: false,
            rm_all_nexuses: false,
        }
    }
    /// Set the `mayastor_compat_v1`.
    pub fn with_mayastor_compat_v1(mut self, compat: bool) -> Self {
        self.mayastor_compat_v1 = compat;
        self
    }
    /// Set the `rm_all_nexuses`.
    pub fn with_rm_all_nexuses(mut self, rm_all_nexuses: bool) -> Self {
        self.rm_all_nexuses = rm_all_nexuses;
        self
    }

    /// Get the volume ID.
    pub fn volume_id(&self) -> &Option<VolumeId> {
        &self.volume_id
    }

    /// Get the nexus ID.
    pub fn nexus_id(&self) -> &NexusId {
        &self.nexus_id
    }

    fn nexus_key_mayastor_v1(&self) -> String {
        // compatibility mode, return key at the root!
        self.nexus_id.to_string()
    }

    /// Parse the nexus id out of the given key.
    pub fn parse_id(key: &str) -> Result<NexusId, String> {
        let Some((_, n)) = parse_info_key(key) else {
            return Err("Failed to match nexus info format".into());
        };

        n.try_into().map_err(|e: uuid::Error| e.to_string())
    }
}

fn parse_info_key(key: &str) -> Option<(Option<&str>, &str)> {
    // $prefix/volume/{volume_uuid}/nexus/{nexus_uuid}/info
    let values = key.split('/').collect::<Vec<_>>();
    if let [.., "volume", volume, "nexus", nexus, "info"] = values[..] {
        return Some((Some(volume), nexus));
    }
    if let [.., "nexus", nexus, "info"] = values[..] {
        return Some((None, nexus));
    }

    None
}

impl ObjectKey for NexusInfoKey {
    type Kind = StorableObjectType;

    fn key(&self) -> String {
        if self.mayastor_compat_v1 {
            return self.nexus_key_mayastor_v1();
        }
        let namespace = key_prefix(self.version());
        let nexus_uuid = self.nexus_id.clone();
        match &self.volume_id {
            Some(volume_uuid) if self.rm_all_nexuses => {
                format!("{namespace}/volume/{volume_uuid}/nexus/")
            }
            Some(volume_uuid) => {
                format!("{namespace}/volume/{volume_uuid}/nexus/{nexus_uuid}/info")
            }
            None => {
                format!("{namespace}/nexus/{nexus_uuid}/info")
            }
        }
    }

    fn version(&self) -> ApiVersion {
        ApiVersion::V0
    }

    fn key_type(&self) -> StorableObjectType {
        // The key is generated directly from the `key()` function above.
        unreachable!()
    }

    fn key_uuid(&self) -> String {
        // The key is generated directly from the `key()` function above.
        unreachable!()
    }
}

impl StorableObject for NexusInfo {
    type Key = NexusInfoKey;

    fn key(&self) -> Self::Key {
        NexusInfoKey {
            volume_id: self.volume_uuid.clone(),
            nexus_id: self.uuid.clone(),
            mayastor_compat_v1: false,
            rm_all_nexuses: false,
        }
    }
}

/// Deletes all v1 nexus_info by fetching all keys and parsing the key to UUID and deletes on
/// success.
pub async fn delete_all_v1_nexus_info<S: Store>(
    store: &mut S,
    etcd_page_limit: i64,
) -> Result<(), StoreError> {
    let mut first = true;
    let mut kvs;
    let mut last = Some("".to_string());

    while let Some(prefix) = &last {
        kvs = store.get_values_paged(prefix, etcd_page_limit, "").await?;

        if !first && !kvs.is_empty() {
            kvs.remove(0);
        }
        first = false;

        last = kvs.last().map(|(key, _)| key.to_string());
        // If the key is a uuid, i.e. nexus_info v1 key, and the value is a valid nexus_info then we
        // delete it.
        for (key, value) in &kvs {
            if Uuid::parse_str(key).is_ok()
                && serde_json::from_value::<NexusInfo>(value.clone()).is_ok()
            {
                store.delete_kv(&key).await?;
            }
        }
    }
    info!("v1.0.x nexus_info cleaned up successfully");
    Ok(())
}
