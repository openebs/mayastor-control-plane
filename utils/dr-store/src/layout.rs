//! Where everything in a relationship lives.
//!
//! The one place keys are built. Nothing else may concatenate a key by hand: the layout
//! is the single thing a populated bucket cannot be migrated away from, so it is worth
//! having exactly one definition of it, in a form a test can hold to.

use crate::Checksum;

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
///
/// Key construction is total, so the names it is given must already be single path
/// segments, and a side name must carry no `.`: a chunk reference joins side, uid and
/// sequence with dots. The pair service refuses names that break either rule.
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
        let lead = lead(prefix);
        Self {
            root: format!("{lead}dr/{dr}/"),
        }
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
    pub fn chunk(&self, checksum: &Checksum) -> String {
        let (root, shard) = (&self.root, checksum.sharded());
        format!("{root}chunks/{shard}")
    }

    /// A pit's claim on a chunk, written before the chunk is relied upon and removed
    /// when that pit is pruned.
    ///
    /// Filed at the chunk's own shard and name, so that the collector asking "does
    /// anything still reference this?" is a listing of one prefix. The sequence is padded
    /// as the pit's is, so a reference sorts like the pit it names.
    pub fn chunk_ref(&self, checksum: &Checksum, side: &str, uid: &str, seq: u64) -> String {
        let (root, shard) = (&self.root, checksum.sharded());
        format!("{root}chunks-ref/{shard}/{side}.{uid}.{seq:0SEQ_DIGITS$}")
    }

    /// Every claim on a chunk, as a listing prefix.
    pub fn chunk_refs(&self, checksum: &Checksum) -> String {
        let (root, shard) = (&self.root, checksum.sharded());
        format!("{root}chunks-ref/{shard}/")
    }

    /// The collector's intent to delete a chunk, raised before it looks for references
    /// and cleared by any cycle that wants the chunk back.
    pub fn chunk_rm(&self, checksum: &Checksum) -> String {
        let (root, shard) = (&self.root, checksum.sharded());
        format!("{root}chunks-rm/{shard}")
    }

    /// One side's subtree: everything that side alone writes.
    pub fn side(&self, side: &str, uid: &str) -> String {
        let sides = self.sides();
        format!("{sides}{side}/{uid}/")
    }

    /// Every side of this relationship, as a listing prefix. What a cluster walks once
    /// to find its peer, and the only listing on the identity path.
    ///
    /// Its own prefix rather than the root, so no side name can land among the shared
    /// prefixes, and a shared prefix added later cannot collide with a side already in a
    /// bucket.
    pub fn sides(&self) -> String {
        let root = &self.root;
        format!("{root}sides/")
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
    pub fn point_chunk(&self, side: &str, uid: &str, seq: u64, checksum: &Checksum) -> String {
        let pit = self.pit(side, uid, seq);
        let shard = checksum.sharded();
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
    let lead = lead(prefix);
    format!("{lead}clusters/{cluster}/{uid}/cluster.cfg")
}

/// The bucket prefix as a key's lead: no separators of its own, one after it.
fn lead(prefix: &str) -> String {
    match prefix.trim_matches('/') {
        "" => String::new(),
        prefix => format!("{prefix}/"),
    }
}

impl Checksum {
    /// `<aa>/<bb>/<checksum>`: two levels of two hex digits cut from the checksum, then
    /// the checksum.
    ///
    /// Here rather than beside the checksum, because the shard is part of the layout.
    /// Two levels of two digits give 65,536 shards while capping the fan-out at each
    /// level to 256; one level of four reaches the same count but puts 65,536 entries
    /// under a single prefix, which is more than a delimited listing returns in a page.
    /// It is even because it is cut from a hash, not because of how many shards there
    /// are. Two hex digits are one byte, and every digest has at least 32.
    fn sharded(&self) -> String {
        let (aa, bb) = (self.digest()[0], self.digest()[1]);
        format!("{aa:02x}/{bb:02x}/{self}")
    }
}
