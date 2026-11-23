use crate::{rpc_impl_string_id, rpc_impl_string_id_inner};

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
