use crate::{rpc_impl_string_id, rpc_impl_string_id_inner};

use serde::{Deserialize, Serialize};
use std::fmt::Debug;

rpc_impl_string_id!(DrPairId, "ID of a dr cluster pair");
rpc_impl_string_id!(DrClusterId, "Name of a cluster within a dr relationship");
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

/// An S3-compatible object store.
///
/// The region matters: AWS requires it in the signature, and some stores reject a
/// signature scoped to a region they were not configured with, while others ignore it
/// entirely. Carrying it per store is what lets two pairs live in different regions.
///
/// Credentials are optional. Given, they are stored with the pair and used for this store
/// alone; omitted, the agent falls back to its own environment, which is what keeps
/// instance roles and other short-lived credentials working - those refresh, and a key
/// written here would not.
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
    /// Access key id for this store. Absent means the agent's environment.
    pub access_key: Option<String>,
    /// Secret access key for this store. Absent means the agent's environment.
    pub secret_key: Option<String>,
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
    /// This cluster's name in the store.
    pub cluster: DrClusterId,
    /// The uid of the cluster this pair was created on, which is the one its control plane
    /// keys its own persistent store on.
    ///
    /// Filled in by core from the platform it runs on rather than supplied: a name is chosen
    /// by an administrator and two clusters may be given the same one, so this is what
    /// actually distinguishes them, and it is the second half of every store key this side
    /// writes - `<cluster>/<cluster_uid>/`.
    pub cluster_uid: DrClusterUid,
    /// The peer's name in the store.
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

/// A page of cluster pairs.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Default)]
pub struct DrClusterPairs {
    /// The pairs in this page.
    pub entries: Vec<DrClusterPair>,
    /// Token to resume from, when more remain.
    pub next_token: Option<u64>,
}
