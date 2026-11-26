use crate::{
    rpc_impl_string_id, rpc_impl_string_id_inner, rpc_impl_string_uuid, rpc_impl_string_uuid_inner,
    types::v0::{store::SpecStatus, transport::VolumeId},
};

use serde::{Deserialize, Serialize};
use std::fmt::Debug;

rpc_impl_string_id!(
    DrPairId,
    "ID of a dr cluster pair: a single path segment, not empty and without a `/`"
);
rpc_impl_string_id!(
    DrClusterId,
    "Name of a cluster within a dr relationship: a single path segment, not empty and \
     without a `/`"
);
rpc_impl_string_id!(
    DrClusterUid,
    "A cluster's own identity, as its control plane keys its persistent store on"
);

/// Where a relationship's store lives, and how it is reached.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub enum DrTransport {
    /// An S3-compatible object store both clusters can reach.
    S3(DrS3Transport),
}

/// Keys for a store, carried by the pair.
///
/// Both or neither: half a credential cannot sign anything.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct DrS3Keys {
    /// Access key id.
    pub access_key: String,
    /// Secret access key.
    pub secret_key: String,
}

/// Where the agent gets the credentials for a store.
///
/// Absent on the transport means the agent's own environment, which is the single-store
/// case and the one that keeps instance roles working - those refresh, and a key written
/// here would not. That default cannot serve two stores at once, which is what `Named`
/// is for: the pair says which credentials, the agent's environment holds them, and a
/// cluster can be party to relationships over different stores without any of their
/// secrets being written down here.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub enum DrS3Credentials {
    /// Keys carried by the pair, used for this store alone.
    Keys(DrS3Keys),
    /// A name the agent resolves against its own environment.
    Named(String),
}

/// An S3-compatible object store.
///
/// The region matters: AWS requires it in the signature, and some stores reject a
/// signature scoped to a region they were not configured with, while others ignore it
/// entirely. Carrying it per store is what lets two pairs live in different regions.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct DrS3Transport {
    /// Endpoint URL of the store.
    pub endpoint: String,
    /// Bucket the relationships live in.
    pub bucket: String,
    /// Key prefix within the bucket, when the bucket is shared with something else.
    pub prefix: Option<String>,
    /// Region the bucket lives in.
    pub region: Option<String>,
    /// How the agent authenticates to this store. Absent means its own environment.
    pub credentials: Option<DrS3Credentials>,
}

/// The pairing of this cluster with a peer, and the store they share.
///
/// Cluster-scoped rather than per-volume: a volume's dr entry names a pair, and many
/// volumes name the same one. The two cluster names are the whole handshake - each side
/// writes under its own and reads the other's.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct DrClusterPair {
    /// How volumes refer to this pair.
    pub id: DrPairId,
    /// The name of this side's subtree in the store.
    ///
    /// Everything this cluster writes goes under
    /// `<prefix>/dr/<dr-id>/sides/<cluster>/<uid>/`, and the peer's own pair has to carry the same string as its `peer` - otherwise
    /// neither side looks where the other writes.
    pub cluster: DrClusterId,
    /// The uid of the cluster this pair was created on, which is the one its control plane
    /// keys its own persistent store on.
    ///
    /// Filled in by core from the platform it runs on rather than supplied: a name is chosen
    /// by an administrator and two clusters may be given the same one, so this is what
    /// actually distinguishes them, and it is the second half of every store key this side
    /// writes - `<cluster>/<cluster_uid>/`.
    pub cluster_uid: DrClusterUid,
    /// The name of the peer's subtree, which is where this side reads the peer's config
    /// and status from. It mirrors the peer's own `cluster`.
    ///
    /// Named rather than deduced: taking whichever subtree is not ours is correct only
    /// while there are exactly two, and a misconfigured pair is better seen than
    /// converged against.
    pub peer: DrClusterId,
    /// Where the store is.
    pub transport: DrTransport,
}

/// Create a cluster pair, or adopt one that already exists.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct CreateDrClusterPair {
    /// How volumes will refer to this pair.
    pub id: DrPairId,
    /// This side's name in the store for this relationship.
    ///
    /// Not a claim about which cluster this is - that is the cluster's uid, which is not
    /// chosen by hand. So a name other than this cluster's own is allowed, and is what lets
    /// one cluster hold both sides of a relationship: two pairs, `east -> west` and
    /// `west -> east`, over the same store. It must differ from `peer`.
    pub cluster: DrClusterId,
    /// The peer's name in the store.
    pub peer: DrClusterId,
    /// Where the store is.
    pub transport: DrTransport,
}

/// Fetch a single cluster pair.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct GetDrClusterPair {
    /// The pair to fetch.
    pub id: DrPairId,
}

/// Remove a cluster pair.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct DestroyDrClusterPair {
    /// The pair to remove.
    pub id: DrPairId,
}

/// Every cluster pair.
///
/// Not a page: pairs are cluster-scoped, one per peer relationship, so a cluster has a
/// handful and there is nothing to paginate over.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Default)]
pub struct DrClusterPairs {
    /// The pairs.
    pub entries: Vec<DrClusterPair>,
}

rpc_impl_string_uuid!(
    DrId,
    "UUID of a volume's dr relationship: the source volume's uuid when replication was enabled"
);

/// Which end of a relationship a volume holds.
///
/// Fixed when the entry is created: moving a volume from one end to the other is failover
/// or failback, which are their own operations rather than an edit of the entry.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, Eq, PartialEq)]
pub enum DrRole {
    /// Takes the recovery points and uploads them.
    Source,
    /// Applies the source's recovery points.
    Destination,
}

/// How often a volume is replicated, and for how long its points are kept.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, Eq, PartialEq)]
pub struct DrPolicy {
    /// Whether a demoted side found to have diverged discards its un-replicated writes
    /// rather than waiting for an operator. Destructive, hence off by default.
    pub auto_resync: bool,
    /// How often the source takes a recovery point.
    pub schedule: std::time::Duration,
    /// How long recovery points are kept.
    pub ttl: std::time::Duration,
    /// The lag past which the source stops committing. Absent means it never stops.
    pub max_lag: Option<std::time::Duration>,
}

/// A volume's part in a dr relationship: the intent, as the api states it.
///
/// One per volume, keyed by its uuid.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct DrVolume {
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
    /// What this cluster believes, not a fact: the peer holds its own belief, and after a
    /// forced failover both can claim to be the source. See [`DrVolumeSpec::role`].
    ///
    /// [`DrVolumeSpec::role`]: crate::types::v0::store::dr::DrVolumeSpec::role
    pub role: DrRole,
    /// How often, and for how long.
    pub policy: DrPolicy,
    /// Lifecycle of this entry: `Created` while replication is enabled, `Deleting` once it
    /// was disabled and the relationship is waiting to be purged.
    pub status: SpecStatus<()>,
}

/// Enable replication on a volume, or amend the policy of one already enabled.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct EnableDrVolume {
    /// The volume to protect. It must exist.
    pub volume: VolumeId,
    /// The cluster pair to replicate over. It must exist.
    pub pair: DrPairId,
    /// Which end of the relationship this volume holds.
    pub role: DrRole,
    /// How often, and for how long.
    pub policy: DrPolicy,
}

/// Fetch a single dr volume.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct GetDrVolume {
    /// The volume whose entry to fetch.
    pub volume: VolumeId,
}

/// Disable replication on a volume.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct DisableDrVolume {
    /// The volume whose entry to remove.
    pub volume: VolumeId,
}

/// Every dr volume.
///
/// Not a page yet: one per protected volume, which is a number pagination can be added for
/// when it matters, as it was for volumes.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct DrVolumes {
    /// The entries.
    pub entries: Vec<DrVolume>,
}
