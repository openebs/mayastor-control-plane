use crate::types::v0::{
    store::SpecStatus,
    transport::{
        DrClusterId, DrClusterUid, DrId, DrPairId, DrPolicy, DrRole, DrTransport,
        DrVolume as DrVolumeEntry, VolumeId,
    },
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
            // There is no data-plane step: the write to the store is the creation.
            status: DrClusterPairSpecStatus::Created(()),
        }
    }
    /// How volumes refer to this pair.
    pub fn id(&self) -> &DrPairId {
        &self.id
    }
}

/// Key used by the store to uniquely identify a DrVolume structure: the volume's uuid.
pub struct DrVolumeKey(VolumeId);

impl From<&VolumeId> for DrVolumeKey {
    fn from(id: &VolumeId) -> Self {
        Self(id.clone())
    }
}

impl ObjectKey for DrVolumeKey {
    type Kind = StorableObjectType;

    fn version(&self) -> ApiVersion {
        ApiVersion::V0
    }

    fn key_type(&self) -> StorableObjectType {
        StorableObjectType::DrVolume
    }

    fn key_uuid(&self) -> String {
        self.0.to_string()
    }
}

impl StorableObject for DrVolume {
    type Key = DrVolumeKey;

    fn key(&self) -> Self::Key {
        DrVolumeKey(self.spec.volume.clone())
    }
}

/// Lifecycle of a dr volume.
pub type DrVolumeSpecStatus = SpecStatus<()>;

/// A volume's dr entry as the persistent store holds it.
///
/// A wrapper rather than the spec itself, so that what is kept beside the intent - the
/// observed half the loops report back - has somewhere to go without the key or the
/// object type changing.
///
/// Separate from the volume's own spec: the loops update an entry every round, and on the
/// volume that would be continuous churn on its write path, contending with its reconciler
/// for something unrelated to it.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct DrVolume {
    /// The intent.
    pub spec: DrVolumeSpec,
}

impl DrVolume {
    /// The volume this entry protects.
    pub fn volume(&self) -> &VolumeId {
        &self.spec.volume
    }
}

/// A volume's part in a dr relationship, as it was asked for.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct DrVolumeSpec {
    /// This cluster's volume, which this entry protects: the source volume on the source,
    /// the target volume on the destination.
    pub volume: VolumeId,
    /// The relationship's id, which the store keys everything under.
    ///
    /// It is the source volume's uuid when replication was enabled, and it never changes:
    /// a failover swaps the source and the target, but the id stays.
    pub dr: DrId,
    /// The cluster pair the relationship runs over.
    pub pair: DrPairId,
    /// The source volume: the one written to, whose points are uploaded to the store.
    /// It is `volume` on the source.
    pub source_volume: VolumeId,
    /// The target volume: the one the source's points are applied to. It is `volume` on
    /// the destination.
    ///
    /// Absent while it is not known. For now both volumes carry the same uuid, so it
    /// always is.
    pub target_volume: Option<VolumeId>,
    /// Which end of the relationship this volume holds.
    ///
    /// # Warning
    ///
    /// This is what this cluster believes, not a fact about the relationship. Nothing in
    /// the store decides it, and the peer's entry carries its own: after a forced failover
    /// both sides can believe they are the source. What settles it is what each side does,
    /// and every action on the store is conditioned on a fresh read of the peer's side -
    /// never on this field alone.
    pub role: DrRole,
    /// How often, and for how long.
    pub policy: DrPolicy,
    /// Lifecycle of this entry.
    pub status: DrVolumeSpecStatus,
}

impl DrVolumeSpec {
    /// Returns a new entry with replication enabled.
    ///
    /// For now the source and target volumes must carry the same uuid, so on either side
    /// both are this volume, and so is the relationship's id. Volumes with different uuids
    /// are not supported yet.
    pub fn new(volume: VolumeId, pair: DrPairId, role: DrRole, policy: DrPolicy) -> Self {
        Self {
            dr: volume.uuid().into(),
            source_volume: volume.clone(),
            target_volume: Some(volume.clone()),
            volume,
            pair,
            role,
            policy,
            // There is no data-plane step: the write to the store is the creation.
            status: DrVolumeSpecStatus::Created(()),
        }
    }
}

impl From<DrVolume> for DrVolumeEntry {
    fn from(src: DrVolume) -> Self {
        let spec = src.spec;
        Self {
            volume: spec.volume,
            dr: spec.dr,
            pair: spec.pair,
            source_volume: spec.source_volume,
            target_volume: spec.target_volume,
            role: spec.role,
            policy: spec.policy,
            status: spec.status,
        }
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
                credentials: None,
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

    const VOLUME: &str = "8c1f2a3b-5d4e-4f60-9a7b-1c2d3e4f5a6b";

    fn policy() -> DrPolicy {
        DrPolicy {
            auto_resync: false,
            schedule: std::time::Duration::from_secs(15 * 60),
            ttl: std::time::Duration::from_secs(24 * 60 * 60),
            max_lag: None,
        }
    }

    fn volume(role: DrRole) -> DrVolume {
        DrVolume {
            spec: DrVolumeSpec::new(
                VolumeId::try_from(VOLUME).unwrap(),
                "dr-west".into(),
                role,
                policy(),
            ),
        }
    }

    #[test]
    fn dr_volume_round_trips() {
        let entry = volume(DrRole::Destination);
        let json = serde_json::to_string(&entry).unwrap();
        let back: DrVolume = serde_json::from_str(&json).unwrap();
        assert_eq!(entry, back);
    }

    #[test]
    fn dr_volume_key_is_the_volume() {
        let entry = volume(DrRole::Source);
        assert_eq!(entry.key().key_uuid(), VOLUME);
        assert_eq!(
            entry.key().key_type().to_string(),
            StorableObjectType::DrVolume.to_string()
        );
    }

    #[test]
    fn the_relationship_is_the_volume_it_was_enabled_on() {
        // On both ends, since for now both carry the same uuid, and so do the source and
        // the target.
        for role in [DrRole::Source, DrRole::Destination] {
            let spec = volume(role).spec;
            assert_eq!(spec.dr.as_str(), VOLUME);
            assert_eq!(spec.source_volume.as_str(), VOLUME);
            assert_eq!(
                spec.target_volume.as_ref().map(|v| v.as_str()),
                Some(VOLUME)
            );
        }
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
