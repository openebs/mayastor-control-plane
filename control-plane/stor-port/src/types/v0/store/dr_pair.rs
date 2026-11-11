use crate::types::v0::{
    store::SpecStatus,
    transport::{DrClusterId, DrClusterUid, DrPairId, DrTransport},
};
use pstor::{ApiVersion, ObjectKey, StorableObject, StorableObjectType};
use serde::{Deserialize, Serialize};

/// Key used by the store to uniquely identify a DrClusterPairSpec structure.
pub struct DrClusterPairSpecKey(DrPairId);

impl From<&DrPairId> for DrClusterPairSpecKey {
    fn from(id: &DrPairId) -> Self {
        Self(id.clone())
    }
}

impl ObjectKey for DrClusterPairSpecKey {
    type Kind = StorableObjectType;

    fn version(&self) -> ApiVersion {
        ApiVersion::V0
    }

    fn key_type(&self) -> StorableObjectType {
        StorableObjectType::DrClusterPairSpec
    }

    fn key_uuid(&self) -> String {
        self.0.to_string()
    }
}

impl StorableObject for DrClusterPairSpec {
    type Key = DrClusterPairSpecKey;

    fn key(&self) -> Self::Key {
        DrClusterPairSpecKey(self.id.clone())
    }
}

/// Lifecycle of a cluster pair, as for any other spec.
pub type DrClusterPairSpecStatus = SpecStatus<()>;

/// The pairing of this cluster with a peer, and the store they share.
///
/// Persisted separately from the per-volume dr entry because the two have different
/// lifetimes: a pair outlives every relationship underneath it, and many volumes name the
/// same one. It is also where the location of everything else comes from - the bucket and
/// prefix for every key this side writes, and the name it expects to find the peer under.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct DrClusterPairSpec {
    /// How volumes refer to this pair.
    pub id: DrPairId,
    /// This cluster's name in the store.
    pub cluster: DrClusterId,
    /// This cluster's own identity, as core reads it from the platform.
    ///
    /// Not the administrator's to choose, and not absent either: core is the one creating
    /// the pair and already knows which cluster it is, so it is recorded here at creation.
    pub cluster_uid: DrClusterUid,
    /// The peer's name in the store.
    pub peer: DrClusterId,
    /// Where the store is.
    pub transport: DrTransport,
    /// Lifecycle of this entry.
    pub status: DrClusterPairSpecStatus,
}

impl DrClusterPairSpec {
    /// Returns a new cluster pair in its creating state.
    pub fn new(
        id: DrPairId,
        cluster: DrClusterId,
        cluster_uid: DrClusterUid,
        peer: DrClusterId,
        transport: DrTransport,
    ) -> Self {
        Self {
            id,
            cluster,
            cluster_uid,
            peer,
            transport,
            status: DrClusterPairSpecStatus::Creating,
        }
    }
    /// How volumes refer to this pair.
    pub fn id(&self) -> &DrPairId {
        &self.id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::v0::transport::DrS3Transport;

    fn pair() -> DrClusterPairSpec {
        DrClusterPairSpec::new(
            "dr-west".into(),
            "east".into(),
            "7b9c1f3e-2a4d-4c8b-9e1f-0a2b3c4d5e6f".into(),
            "west".into(),
            DrTransport::S3(DrS3Transport {
                endpoint: "http://localhost:3900".into(),
                bucket: "default".into(),
                prefix: Some("prod".into()),
                region: Some("eu-west-1".into()),
                access_key: None,
                secret_key: None,
            }),
        )
    }

    #[test]
    fn dr_cluster_pair_round_trips() {
        let spec = pair();
        let json = serde_json::to_string(&spec).unwrap();
        let back: DrClusterPairSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(spec, back);
    }

    #[test]
    fn dr_cluster_pair_key_is_the_pair_id() {
        let spec = pair();
        assert_eq!(spec.key().key_uuid(), spec.id.to_string());
        assert_eq!(
            spec.key().key_type().to_string(),
            StorableObjectType::DrClusterPairSpec.to_string()
        );
    }

    #[test]
    fn cluster_uid_is_recorded_at_creation() {
        // Core knows which cluster it is, so the pair carries the uid from the start
        // rather than waiting for something to fill it in.
        assert_eq!(
            pair().cluster_uid.to_string(),
            "7b9c1f3e-2a4d-4c8b-9e1f-0a2b3c4d5e6f"
        );
    }
}
