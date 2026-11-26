//! What each side publishes about itself.
//!
//! Two documents per side, and they are the only channel between the clusters: `dr.cfg`
//! says who the side is and what it believes about the relationship, and `state.json`
//! says how far it has got. Each is written by exactly one side, under that side's own
//! subtree, so neither needs a precondition and neither can be contended.
//!
//! Both carry a schema [`VERSION`], and both are read tolerantly: a field this reader
//! does not know is ignored, and an optional field a writer did not send reads as unset.
//! That is what lets a field be added later without the two clusters upgrading together.
//! It is also why the fields failover and teardown will turn on are reserved now, before
//! anything sets them: a reader has to be able to tell "not set" from "written by an
//! agent that has never heard of it", and only the version says which.

use crate::{
    error::{Error, Result},
    Checksum, DrStore, Layout,
};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use uuid::Uuid;

/// The schema version this agent writes.
///
/// Raised when a field is added whose absence a reader must be able to tell apart from
/// its being unset. A reader takes a document of any version and reads what it knows.
pub const VERSION: u32 = 1;

/// One end of a relationship, as the store addresses it.
///
/// Both halves are needed: `cluster` is which end a cluster holds, named in its
/// `DrClusterPair`, and `uid` is the cluster's own identity. The uid keeps two clusters
/// that were both called `east` apart; the name separates two sides that share a uid,
/// which is every loopback.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Side {
    /// The end of the relationship this side holds.
    pub cluster: String,
    /// The identity of the cluster holding it.
    pub uid: String,
}

impl Side {
    /// The side held by `cluster`, on the cluster identified by `uid`.
    pub fn new(cluster: impl Into<String>, uid: impl Into<String>) -> Self {
        Self {
            cluster: cluster.into(),
            uid: uid.into(),
        }
    }
}

impl std::fmt::Display for Side {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.cluster, self.uid)
    }
}

/// `sides/<side>/<cluster-uid>/dr.cfg`: a side's account of itself and of the
/// relationship.
///
/// Written when the side joins, and again only when what it believes changes. The peer
/// reads it once, to find the side's subtree and the settings a matching volume is built
/// from.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DrConfig {
    /// The schema version of the agent that wrote this.
    pub version: u32,
    /// The relationship.
    pub dr: Uuid,
    /// The end of the relationship this side holds.
    pub cluster: String,
    /// This cluster's own identity.
    pub uid: String,
    /// The end this side expects to find the other under.
    pub peer: String,
    /// This side's volume. What lets the peer map a volume it does not own.
    pub volume: Uuid,
    /// What this side believes it is. A belief: nothing arbitrates between two sides
    /// that both believe they are source.
    pub role: Role,
    /// The volume's geometry: what a matching volume is built from.
    pub cluster_size: u64,
    /// The unit this side's points use, derived from `cluster_size`.
    pub chunk_size: u64,
    /// What a point carries beyond its own changes.
    pub upload_format: UploadFormat,
    /// Whether chunks are shared between points, or kept under the point that
    /// introduced them.
    pub dedup: bool,
    /// The function whose digest names a chunk.
    pub hash: HashAlgorithm,
    /// When replication was disabled on this side's volume.
    ///
    /// A relationship is purged only once both sides carry it. Reserved: nothing sets
    /// it yet. Unset reads as still replicating, and so does a peer whose agent predates
    /// the field - which is why it is written as `null` rather than left out, and why a
    /// purge must never be decided on its absence.
    #[serde(default)]
    pub disabled: Option<DateTime<Utc>>,
}

impl DrConfig {
    /// The side this config describes, and so the subtree it lives in.
    pub fn side(&self) -> Side {
        Side::new(&self.cluster, &self.uid)
    }
}

/// `sides/<side>/<cluster-uid>/state.json`: a side's progress.
///
/// The one object a side rewrites in the steady state, and the one the peer polls.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterState {
    /// The schema version of the agent that wrote this.
    pub version: u32,
    /// The end of the relationship this side holds.
    pub cluster: String,
    /// The relationship.
    pub dr: Uuid,
    /// This side's volume. What lets the peer map a volume it does not own: the source
    /// learns the target's uuid from here, since it cannot know it when replication is
    /// enabled.
    pub volume: Uuid,
    /// What this side believes it is.
    pub role: Role,
    /// What orders two lineages when both sides believe they are source.
    ///
    /// Raised only by a promotion. Reserved: nothing raises it yet.
    #[serde(default)]
    pub epoch: u64,
    /// When a quiesce stopped the volume taking writes, cleared by the next cycle. What
    /// lets a destination tell "idle at N" from "quiesced at N".
    ///
    /// Reserved: nothing sets it yet. A peer whose agent predates it never writes it,
    /// which is why planned failover has to read the version before trusting its
    /// absence.
    #[serde(default)]
    pub drained: Option<DateTime<Utc>>,

    /// As source: the sequence of the newest point under this side's `pits/`.
    #[serde(default)]
    pub head: Option<u64>,
    /// As source: when `head` was committed.
    #[serde(default)]
    pub head_at: Option<DateTime<Utc>>,

    /// As destination: the newest point applied, until the first is.
    #[serde(default)]
    pub last_applied: Option<PitId>,
    /// As destination: when `last_applied` was applied.
    #[serde(default)]
    pub applied_at: Option<DateTime<Utc>>,
    /// As destination: the entries it could not apply - a chunk absent or not matching
    /// its checksum. The source treats these offsets as changed in its next cycle.
    #[serde(default)]
    pub missing: Vec<ChunkRef>,

    /// When this was written. What says how stale everything else in it is.
    pub observed_at: DateTime<Utc>,
}

impl ClusterState {
    /// A side's state before it has done anything: no point taken, none applied.
    pub fn new(cluster: impl Into<String>, dr: Uuid, volume: Uuid, role: Role) -> Self {
        Self {
            version: VERSION,
            cluster: cluster.into(),
            dr,
            volume,
            role,
            epoch: 0,
            drained: None,
            head: None,
            head_at: None,
            last_applied: None,
            applied_at: None,
            missing: Vec::new(),
            observed_at: Utc::now(),
        }
    }
}

/// What a side believes it is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Role {
    /// Takes the points and uploads them.
    Source,
    /// Applies the source's points.
    Destination,
    /// Rebuilding from the source after having diverged from it.
    Resyncing,
}

/// What a point carries beyond its own changes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum UploadFormat {
    /// Only what its cycle changed: the store holds the last `ttl` worth of changes
    /// rather than a copy.
    IncrementalOnly,
    /// The same, plus the entries the cycle carried forward, so every point is the
    /// whole layout.
    SyntheticFull,
}

/// The function whose digest names a chunk.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum HashAlgorithm {
    /// 32-byte names. The default.
    Blake3,
    /// 64-byte names.
    Blake2b512,
}

/// Names a point: the side that wrote it and its sequence there.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PitId {
    /// The end of the relationship that wrote it.
    pub writer: String,
    /// Its sequence under that side.
    pub seq: u64,
}

/// One chunk of a volume: where it is, and what names its contents.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkRef {
    /// Byte offset into the volume.
    pub offset: u64,
    /// The digest that names the stored object.
    pub checksum: Checksum,
}

/// Publishing and reading the side documents, over any store.
///
/// Each call names the relationship by its [`Layout`], since a store is the whole bucket.
///
/// What a missing document means is the caller's to decide, so absent is an answer here
/// rather than a failure: a side with no `dr.cfg` has not joined, and one with no
/// `state.json` has not reported yet. A document that is there and cannot be read is an
/// error, never absent - reading it as absent would take a peer that is replicating for
/// one that never joined.
#[async_trait::async_trait]
pub trait Sides: DrStore {
    /// Publish this side's config.
    async fn put_cfg(&self, layout: &Layout, cfg: &DrConfig) -> Result<()> {
        let side = cfg.side();
        let key = layout.cfg(&side.cluster, &side.uid);
        self.put(&key, encode(cfg)?).await.map(|_| ())
    }

    /// A side's config, if it has published one.
    async fn get_cfg(&self, layout: &Layout, side: &Side) -> Result<Option<DrConfig>> {
        let key = layout.cfg(&side.cluster, &side.uid);
        let Some(cfg) = read::<DrConfig>(self, &key).await? else {
            return Ok(None);
        };
        if cfg.side() != *side {
            return Err(Error::Malformed {
                key,
                reason: format!("it describes side '{}'", cfg.side()),
            });
        }
        Ok(Some(cfg))
    }

    /// Every side with a subtree in this relationship.
    ///
    /// Two delimited listings, one per level of `<side>/<cluster-uid>/`, so the cost
    /// does not grow with how many points the sides hold. A side listed here need not
    /// have published its config yet.
    async fn list_sides(&self, layout: &Layout) -> Result<Vec<Side>> {
        let sides = layout.sides();
        let mut found = Vec::new();
        for named in self.list_dirs(&sides).await? {
            let cluster = last_segment(&named);
            for uid in self.list_dirs(&named).await? {
                found.push(Side::new(cluster, last_segment(&uid)));
            }
        }
        Ok(found)
    }

    /// The config of the side published under the name this side expects its peer under.
    ///
    /// Named rather than deduced: "whichever side is not us" is right only while there
    /// are exactly two, and a volume re-targeted away and back leaves one behind. More
    /// than one cluster answering to the name is refused rather than guessed between.
    async fn find_peer(&self, layout: &Layout, peer: &str) -> Result<Option<DrConfig>> {
        let mut found = Vec::new();
        for side in self.list_sides(layout).await? {
            if side.cluster == peer {
                if let Some(cfg) = self.get_cfg(layout, &side).await? {
                    found.push(cfg);
                }
            }
        }
        match found.len() {
            0 | 1 => Ok(found.pop()),
            _ => Err(Error::AmbiguousPeer {
                peer: peer.to_string(),
                uids: found.into_iter().map(|cfg| cfg.uid).collect(),
            }),
        }
    }

    /// Publish this side's state.
    ///
    /// Takes the side because the state names only the cluster, not its uid.
    async fn put_state(&self, layout: &Layout, side: &Side, state: &ClusterState) -> Result<()> {
        let key = layout.state(&side.cluster, &side.uid);
        if state.cluster != side.cluster {
            return Err(Error::Config {
                reason: format!(
                    "the state of '{}' cannot be published as '{side}'",
                    state.cluster
                ),
            });
        }
        self.put(&key, encode(state)?).await.map(|_| ())
    }

    /// A side's state, if it has published one.
    async fn get_state(&self, layout: &Layout, side: &Side) -> Result<Option<ClusterState>> {
        let key = layout.state(&side.cluster, &side.uid);
        let Some(state) = read::<ClusterState>(self, &key).await? else {
            return Ok(None);
        };
        if state.cluster != side.cluster {
            return Err(Error::Malformed {
                key,
                reason: format!("it names side '{}'", state.cluster),
            });
        }
        Ok(Some(state))
    }
}

impl<S: DrStore + ?Sized> Sides for S {}

pub(crate) fn encode<T: Serialize>(document: &T) -> Result<Bytes> {
    serde_json::to_vec(document)
        .map(Bytes::from)
        .map_err(|error| Error::Config {
            reason: format!("could not encode a document: {error}"),
        })
}

pub(crate) async fn read<T: DeserializeOwned>(
    store: &(impl DrStore + ?Sized),
    key: &str,
) -> Result<Option<T>> {
    let Some(data) = store.get_opt(key).await? else {
        return Ok(None);
    };
    serde_json::from_slice(&data)
        .map(Some)
        .map_err(|error| Error::Malformed {
            key: key.to_string(),
            reason: error.to_string(),
        })
}

/// The last segment of a listed prefix, which comes back with its trailing separator.
fn last_segment(prefix: &str) -> &str {
    let prefix = prefix.trim_end_matches('/');
    prefix.rsplit('/').next().unwrap_or(prefix)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    const DR: &str = "a1b2c3d4-0000-4000-8000-000000000001";
    const VOLUME: &str = "a1b2c3d4-0000-4000-8000-000000000002";

    fn cfg() -> DrConfig {
        DrConfig {
            version: VERSION,
            dr: DR.parse().unwrap(),
            cluster: "east".into(),
            uid: "uid-1".into(),
            peer: "west".into(),
            volume: VOLUME.parse().unwrap(),
            role: Role::Source,
            cluster_size: 4 << 20,
            chunk_size: 4 << 20,
            upload_format: UploadFormat::SyntheticFull,
            dedup: true,
            hash: HashAlgorithm::Blake3,
            disabled: None,
        }
    }

    fn state() -> ClusterState {
        let mut state = ClusterState::new(
            "west",
            DR.parse().unwrap(),
            VOLUME.parse().unwrap(),
            Role::Destination,
        );
        state.last_applied = Some(PitId {
            writer: "east".into(),
            seq: 7,
        });
        state.applied_at = Some(state.observed_at);
        state.missing = vec![ChunkRef {
            offset: 8 << 20,
            checksum: Checksum::from([0xab; 32]),
        }];
        state
    }

    fn round_trip<T: Serialize + DeserializeOwned>(document: &T) -> T {
        serde_json::from_slice(&encode(document).unwrap()).unwrap()
    }

    #[test]
    fn documents_round_trip() {
        assert_eq!(round_trip(&cfg()), cfg());
        let state = state();
        assert_eq!(round_trip(&state), state);
    }

    /// The reserved fields are written even while unset, so a reader of a current
    /// document finds them there rather than having to infer them.
    #[test]
    fn reserved_fields_are_present_but_unset() {
        let cfg = serde_json::to_value(cfg()).unwrap();
        assert_eq!(cfg["version"], json!(VERSION));
        assert_eq!(cfg["disabled"], Value::Null);

        let state = serde_json::to_value(state()).unwrap();
        assert_eq!(state["version"], json!(VERSION));
        assert_eq!(state["epoch"], json!(0));
        assert_eq!(state["drained"], Value::Null);
    }

    /// How the store spells them, which a peer on another build has to read.
    #[test]
    fn enums_are_spelt_as_the_oep_does() {
        let cfg = serde_json::to_value(cfg()).unwrap();
        assert_eq!(cfg["role"], json!("source"));
        assert_eq!(cfg["upload_format"], json!("syntheticFull"));
        assert_eq!(cfg["hash"], json!("blake3"));
        let state = serde_json::to_value(state()).unwrap();
        assert_eq!(state["role"], json!("destination"));
        assert_eq!(state["missing"][0]["checksum"], json!("ab".repeat(32)));
    }

    /// A newer agent's document reads: what this one does not know is ignored.
    #[test]
    fn unknown_fields_are_ignored() {
        let mut newer = serde_json::to_value(cfg()).unwrap();
        newer["version"] = json!(VERSION + 1);
        newer["something_new"] = json!({ "at": 3 });
        let read: DrConfig = serde_json::from_value(newer).unwrap();
        assert_eq!(read.version, VERSION + 1);
        assert_eq!(
            DrConfig {
                version: VERSION,
                ..read
            },
            cfg()
        );
    }

    /// An older agent's document reads, with what it never wrote as unset - and above
    /// all, a peer that never wrote `disabled` reads as still replicating.
    #[test]
    fn fields_an_older_agent_never_wrote_read_as_unset() {
        let mut older = serde_json::to_value(cfg()).unwrap();
        older.as_object_mut().unwrap().remove("disabled");
        let read: DrConfig = serde_json::from_value(older).unwrap();
        assert_eq!(read.disabled, None);

        let mut older = serde_json::to_value(state()).unwrap();
        for field in ["epoch", "drained", "head", "head_at", "missing"] {
            older.as_object_mut().unwrap().remove(field);
        }
        let read: ClusterState = serde_json::from_value(older).unwrap();
        assert_eq!((read.epoch, read.drained, read.head), (0, None, None));
        assert!(read.missing.is_empty());
    }

    /// What identifies the document is required: without it there is no telling whose
    /// it is.
    #[test]
    fn identity_is_required() {
        for field in ["version", "dr", "cluster", "uid", "volume", "role"] {
            let mut cfg = serde_json::to_value(cfg()).unwrap();
            cfg.as_object_mut().unwrap().remove(field);
            assert!(
                serde_json::from_value::<DrConfig>(cfg).is_err(),
                "a config without '{field}' read"
            );
        }
        for field in ["version", "dr", "cluster", "volume", "role", "observed_at"] {
            let mut state = serde_json::to_value(state()).unwrap();
            state.as_object_mut().unwrap().remove(field);
            assert!(
                serde_json::from_value::<ClusterState>(state).is_err(),
                "a state without '{field}' read"
            );
        }
    }

    #[test]
    fn a_listed_prefix_names_its_last_segment() {
        assert_eq!(last_segment("backups/dr/x/sides/east/"), "east");
        assert_eq!(last_segment("east/"), "east");
        assert_eq!(last_segment("east"), "east");
    }
}
