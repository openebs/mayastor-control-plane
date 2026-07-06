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
    /// Nexus need to be self shutdown by io-engine.
    #[serde(default)]
    pub do_self_shutdown: bool,
    /// Whether the nexus target was persisted as shared. When true, front-end
    /// I/O may have been in flight; when false, the nexus was quiet and its
    /// children can be trusted as in-sync. `None` means the entry was written
    /// by an io-engine version that predates this field, or by a mixed-version
    /// rollout, and must be treated conservatively (assume shared).
    #[serde(default)]
    pub shared: Option<bool>,
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

    /// Check if no replica is healthy.
    pub fn no_healthy_replicas(&self) -> bool {
        self.children.iter().all(|c| !c.healthy) || self.children.is_empty()
    }

    /// Get number of healthy replicas.
    pub fn nr_healthy_replicas(&self) -> u8 {
        self.children.iter().filter(|c| c.healthy).count() as u8
    }

    /// Whether no in-flight front-end I/O could have dirtied the children.
    /// True when either the nexus shut down cleanly (`clean_shutdown`), or when it was
    /// persisted as not-shared (front-end I/O was impossible while the target was down).
    ///
    /// Missing `shared` (an entry written by an io-engine version that predates the field,
    /// or by a mixed-version rollout) is treated as shared: we cannot prove the nexus was
    /// quiet, so we fall back to the conservative pre-existing behaviour.
    pub fn clean_state(&self) -> bool {
        self.clean_shutdown || !self.shared.unwrap_or(true)
    }

    /// Modify the self with the given key information.
    pub fn with_key(self, key: &str) -> Result<Option<Self>, uuid::Error> {
        let Some((volume, nexus)) = parse_info_key(key) else {
            return Ok(None);
        };
        let uuid = NexusId::try_from(nexus)?;

        let Some(volume) = volume else {
            return Ok(Some(Self { uuid, ..self }));
        };

        Ok(Some(Self {
            uuid,
            volume_uuid: Some(volume.try_into()?),
            ..self
        }))
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
}

impl NexusInfoKey {
    /// Create a new NexusInfoKey.
    pub fn new(volume_id: &Option<VolumeId>, nexus_id: &NexusId) -> Self {
        Self {
            volume_id: volume_id.clone(),
            nexus_id: nexus_id.clone(),
            mayastor_compat_v1: false,
        }
    }
    /// Set the `mayastor_compat_v1`.
    pub fn with_mayastor_compat_v1(mut self, compat: bool) -> Self {
        self.mayastor_compat_v1 = compat;
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

    /// The key prefix which is shared by all `Self`.
    /// # Warning
    /// This doesn't support compatible v1 nexus health info.
    pub fn key_prefix() -> String {
        let dummy = Self::new(&Some(VolumeId::new()), &NexusId::new());
        let namespace = key_prefix(dummy.version());
        format!("{namespace}/volume/")
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

enum NexusInfoKind<'a> {
    Nexus(&'a NexusId),
    Volume(&'a VolumeId, &'a NexusId),
    VolumePrefix(&'a VolumeId),
}

impl NexusInfoKind<'_> {
    fn key(&self, prefix: String) -> String {
        match &self {
            Self::Nexus(nexus_uuid) => format!("{prefix}/nexus/{nexus_uuid}/info"),
            Self::Volume(volume_uuid, nexus_uuid) => {
                format!("{prefix}/volume/{volume_uuid}/nexus/{nexus_uuid}/info")
            }
            Self::VolumePrefix(volume_uuid) => {
                format!("{prefix}/volume/{volume_uuid}/nexus/")
            }
        }
    }
}

/// Key used by the store to uniquely identify a NexusInfo structure.
/// The volume is optional because a nexus can be created which is not associated with a volume.
pub struct VolumeHealthKey {
    volume_id: VolumeId,
    nexus_id: NexusId,
}
impl VolumeHealthKey {
    /// Create a new `VolumeHealthKey`.
    pub fn new(volume_id: VolumeId, nexus_id: NexusId) -> Self {
        Self {
            volume_id,
            nexus_id,
        }
    }
    /// Get the id of the nexus.
    pub fn nexus_id(&self) -> &NexusId {
        &self.nexus_id
    }
    /// Get the id of the volume.
    pub fn volume_id(&self) -> &VolumeId {
        &self.volume_id
    }
    /// Convert into a `NexusInfoKey`.
    pub fn nexus_info_key(self) -> NexusInfoKey {
        NexusInfoKey {
            volume_id: Some(self.volume_id),
            nexus_id: self.nexus_id,
            mayastor_compat_v1: false,
        }
    }
}

/// A pstor helper for referencing all [`NexusInfo`] for the given volume.
pub struct VolumeHealthPrefix {
    volume_id: VolumeId,
}
impl VolumeHealthPrefix {
    /// Create a new `Self`, which can reference all volume health info.
    pub fn new(volume_id: VolumeId) -> VolumeHealthPrefix {
        Self { volume_id }
    }
}

impl ObjectKey for VolumeHealthPrefix {
    type Kind = StorableObjectType;

    fn key(&self) -> String {
        let namespace = key_prefix(self.version());
        NexusInfoKind::VolumePrefix(&self.volume_id).key(namespace)
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

impl ObjectKey for NexusInfoKey {
    type Kind = StorableObjectType;

    fn key(&self) -> String {
        if self.mayastor_compat_v1 {
            return self.nexus_key_mayastor_v1();
        }
        let namespace = key_prefix(self.version());
        let nexus_uuid = &self.nexus_id;
        match &self.volume_id {
            Some(volume_uuid) => NexusInfoKind::Volume(volume_uuid, nexus_uuid).key(namespace),
            None => NexusInfoKind::Nexus(nexus_uuid).key(namespace),
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

#[cfg(test)]
mod tests {
    use super::NexusInfo;

    /// Entries persisted by an io-engine version that predates the `shared` field must still
    /// deserialize cleanly, with `shared` filled in as `None`, and must be treated conservatively
    /// as if the nexus was shared so old / mixed-version-rollout entries never get promoted to
    /// "trust all children" incorrectly. This is the load-bearing upgrade-window property.
    #[test]
    fn deserialize_without_shared_field() {
        let json = r#"{"clean_shutdown":false,"children":[]}"#;
        let info: NexusInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.shared, None);
        assert!(!info.clean_shutdown);
        assert!(!info.clean_state());
    }

    /// A cleanly-shut-down nexus is always in a clean state, regardless of the persisted
    /// `shared` value at the time of shutdown.
    #[test]
    fn clean_state_when_clean_shutdown() {
        for shared in [None, Some(true), Some(false)] {
            let info = NexusInfo {
                clean_shutdown: true,
                shared,
                ..Default::default()
            };
            assert!(info.clean_state(), "clean_shutdown=true, shared={shared:?}");
        }
    }

    /// An unclean shutdown while the nexus was persisted as not-shared is still clean:
    /// no front-end I/O could have been in flight to dirty the children.
    #[test]
    fn clean_state_when_unclean_but_unshared() {
        let info = NexusInfo {
            clean_shutdown: false,
            shared: Some(false),
            ..Default::default()
        };
        assert!(info.clean_state());
    }

    /// An unclean shutdown while the nexus was actively shared is not clean.
    #[test]
    fn dirty_state_when_unclean_and_shared() {
        let info = NexusInfo {
            clean_shutdown: false,
            shared: Some(true),
            ..Default::default()
        };
        assert!(!info.clean_state());
    }
}
