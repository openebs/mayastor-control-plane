//! Where everything in a relationship lives.
//!
//! The one place keys are built. Nothing else may concatenate a key by hand: the layout
//! is the single thing a populated bucket cannot be migrated away from, so it is worth
//! having exactly one definition of it, in a form a test can hold to.

use std::fmt::Write;

/// Hex digits of the checksum each shard level takes.
///
/// Width is a constant and depth is not, deliberately. Two levels of two digits give
/// 65,536 shards while capping the fan-out at each level to 256; one level of four
/// reaches the same count but puts 65,536 entries under a single prefix, which is more
/// than a delimited listing returns in a page.
const SHARD_DIGITS: usize = 2;
/// How many shard levels a chunk is filed under. See [`SHARD_DIGITS`].
const SHARD_LEVELS: usize = 2;

/// Width a point's sequence is zero-padded to, so that lexicographic order - the only
/// order a store lists in - is numeric order.
const SEQ_DIGITS: usize = 16;

/// Builds every key in one relationship's subtree.
///
/// Holds the two things that do not change for the life of a relationship - where the
/// bucket's dr subtree starts, and which relationship this is - so that a caller passes
/// only what varies.
///
/// A side is named by two things, and both are needed: `side` is which end of the
/// relationship a cluster holds, named in its `DrClusterPair`, and `uid` is the
/// cluster's own identity. The uid keeps two clusters that were both called `east`
/// apart; the side separates two sides that share a uid, which is every loopback.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Layout {
    root: String,
}

impl Layout {
    /// The subtree of `<prefix>` holding relationship `dr`.
    ///
    /// The relationship id sits outermost so that a credential can be granted per
    /// relationship: a policy wildcards `<dr-id>` and pins `<side>/<cluster-uid>`,
    /// which every major store's policy language allows.
    pub fn new(prefix: &str, dr: &str) -> Self {
        let prefix = prefix.trim_matches('/');
        let mut root = String::new();
        if !prefix.is_empty() {
            root.push_str(prefix);
            root.push('/');
        }
        let _ = write!(root, "dr/{dr}/");
        Self { root }
    }

    /// Everything in this relationship, as a listing prefix.
    pub fn root(&self) -> &str {
        &self.root
    }

    /// The shared, content-addressed chunk pool - `dedup: true`.
    ///
    /// Shared only in that both clusters put into it: the key is the hash of the
    /// content, so the same bytes are the same object and a second writer changes
    /// nothing.
    pub fn chunk(&self, checksum: &str) -> String {
        let (root, shard) = (&self.root, sharded(checksum));
        format!("{root}chunks/{shard}")
    }

    /// A pit's claim on a chunk, written before the chunk is relied upon and removed
    /// when that pit is pruned.
    ///
    /// Filed at the chunk's own shard and name, so that the collector asking "does
    /// anything still reference this?" is a listing of one prefix.
    pub fn chunk_ref(&self, checksum: &str, side: &str, uid: &str, seq: u64) -> String {
        let (root, shard) = (&self.root, sharded(checksum));
        format!("{root}chunks-ref/{shard}/{side}.{uid}.{seq}")
    }

    /// Every claim on a chunk, as a listing prefix.
    pub fn chunk_refs(&self, checksum: &str) -> String {
        let (root, shard) = (&self.root, sharded(checksum));
        format!("{root}chunks-ref/{shard}/")
    }

    /// The collector's intent to delete a chunk, raised before it looks for references
    /// and cleared by any cycle that wants the chunk back.
    pub fn chunk_rm(&self, checksum: &str) -> String {
        let (root, shard) = (&self.root, sharded(checksum));
        format!("{root}chunks-rm/{shard}")
    }

    /// One side's subtree: everything that side alone writes.
    pub fn side(&self, side: &str, uid: &str) -> String {
        let root = &self.root;
        format!("{root}{side}/{uid}/")
    }

    /// Every side of this relationship, as a listing prefix. What a cluster walks once
    /// to find its peer, and the only listing on the identity path.
    pub fn sides(&self) -> String {
        self.root.clone()
    }

    /// A side's account of itself and of the relationship.
    pub fn cfg(&self, side: &str, uid: &str) -> String {
        let side = self.side(side, uid);
        format!("{side}dr.cfg")
    }

    /// A side's progress: head, what it has applied, what it could not.
    pub fn state(&self, side: &str, uid: &str) -> String {
        let side = self.side(side, uid);
        format!("{side}state.json")
    }

    /// A point: its header, and the chunks that cycle changed.
    pub fn point(&self, side: &str, uid: &str, seq: u64) -> String {
        let pit = self.pit(side, uid, seq);
        format!("{pit}point")
    }

    /// The chunks a point did not change - the `full` half, absent under
    /// `IncrementalOnly`.
    pub fn full(&self, side: &str, uid: &str, seq: u64) -> String {
        let pit = self.pit(side, uid, seq);
        format!("{pit}full")
    }

    /// A chunk belonging to one point alone - `dedup: false`, where chunks live under
    /// the point that introduced them instead of in the shared pool.
    pub fn point_chunk(&self, side: &str, uid: &str, seq: u64, checksum: &str) -> String {
        let pit = self.pit(side, uid, seq);
        let shard = sharded(checksum);
        format!("{pit}chunks/{shard}")
    }

    /// One point's subtree, as a listing prefix.
    pub fn pit(&self, side: &str, uid: &str, seq: u64) -> String {
        let side = self.side(side, uid);
        format!("{side}pits/{seq:0SEQ_DIGITS$}/")
    }

    /// Every point one side has written, as a listing prefix. Lists in sequence order,
    /// which is what the zero padding is for.
    pub fn pits(&self, side: &str, uid: &str) -> String {
        let side = self.side(side, uid);
        format!("{side}pits/")
    }
}

/// A per-cluster document, outside any relationship.
///
/// Reserved: nothing writes one yet. It is here so that the key is defined in the same
/// place as the rest, rather than invented later beside them.
pub fn cluster_cfg(prefix: &str, cluster: &str, uid: &str) -> String {
    let prefix = prefix.trim_matches('/');
    let lead = if prefix.is_empty() {
        String::new()
    } else {
        format!("{prefix}/")
    };
    format!("{lead}clusters/{cluster}/{uid}/cluster.cfg")
}

/// `<aa>/<bb>/<checksum>`: the shard levels cut from the checksum, then the checksum.
///
/// The shard is even because it is cut from a hash, not because of how many shards
/// there are. A checksum too short to cut is filed under what it has, which keeps key
/// construction total - a malformed checksum is caught where checksums are made, not
/// here, where returning no key at all would mean every caller handling an error that
/// cannot happen.
fn sharded(checksum: &str) -> String {
    let mut key = String::with_capacity(checksum.len() + SHARD_LEVELS * (SHARD_DIGITS + 1));
    for level in 0..SHARD_LEVELS {
        let from = level * SHARD_DIGITS;
        let to = from + SHARD_DIGITS;
        if checksum.len() < to {
            break;
        }
        key.push_str(&checksum[from..to]);
        key.push('/');
    }
    key.push_str(checksum);
    key
}
