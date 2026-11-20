//! The keys, against the layout the design writes down.
//!
//! Spelled out as literals rather than rebuilt from the same pieces: a test that
//! composed the key the way the code does would agree with any change to it, including
//! the ones a populated bucket cannot survive.

use dr_store::{cluster_cfg, DrStore, Layout, Store};

const CHECKSUM: &str = "ab12cd34ef567890ab12cd34ef567890ab12cd34ef567890ab12cd34ef567890";

fn layout() -> Layout {
    Layout::new("backups", "dr-west")
}

#[test]
fn relationship_scoping() {
    assert_eq!(layout().root(), "backups/dr/dr-west/");

    // The prefix is optional, and a bucket rooted at the relationship is not a special
    // case in any key.
    assert_eq!(Layout::new("", "dr-west").root(), "dr/dr-west/");
    // However it was given: the layout owns the separators.
    assert_eq!(
        Layout::new("/backups/", "dr-west").root(),
        "backups/dr/dr-west/"
    );
    assert_eq!(Layout::new("a/b", "dr-west").root(), "a/b/dr/dr-west/");
}

#[test]
fn per_side_subtrees() {
    let layout = layout();
    assert_eq!(
        layout.side("east", "uid-1"),
        "backups/dr/dr-west/east/uid-1/"
    );
    assert_eq!(
        layout.cfg("east", "uid-1"),
        "backups/dr/dr-west/east/uid-1/dr.cfg"
    );
    assert_eq!(
        layout.state("east", "uid-1"),
        "backups/dr/dr-west/east/uid-1/state.json"
    );

    // The two halves answer different questions, so neither alone separates two
    // subtrees: same uid holding both ends is the loopback, same name under two uids is
    // two clusters that were both called east.
    assert_ne!(layout.side("east", "uid-1"), layout.side("west", "uid-1"));
    assert_ne!(layout.side("east", "uid-1"), layout.side("east", "uid-2"));

    // Every side, which is the one listing on the identity path.
    assert_eq!(layout.sides(), layout.root());
}

#[test]
fn points_sort_numerically() {
    let layout = layout();
    assert_eq!(
        layout.point("east", "uid-1", 7),
        "backups/dr/dr-west/east/uid-1/pits/0000000000000007/point"
    );
    assert_eq!(
        layout.full("east", "uid-1", 7),
        "backups/dr/dr-west/east/uid-1/pits/0000000000000007/full"
    );
    assert_eq!(
        layout.pits("east", "uid-1"),
        "backups/dr/dr-west/east/uid-1/pits/"
    );

    // The padding is the point: a store lists lexicographically, and 10 must not come
    // between 1 and 2.
    let mut keys = [
        layout.point("east", "uid-1", 10),
        layout.point("east", "uid-1", 2),
        layout.point("east", "uid-1", 1),
    ];
    keys.sort();
    assert_eq!(
        keys,
        [
            layout.point("east", "uid-1", 1),
            layout.point("east", "uid-1", 2),
            layout.point("east", "uid-1", 10),
        ]
    );
}

#[test]
fn chunks_are_addressed_by_content() {
    let layout = layout();
    assert_eq!(
        layout.chunk(CHECKSUM),
        format!("backups/dr/dr-west/chunks/ab/12/{CHECKSUM}")
    );
    assert_eq!(
        layout.chunk_ref(CHECKSUM, "east", "uid-1", 7),
        format!("backups/dr/dr-west/chunks-ref/ab/12/{CHECKSUM}/east.uid-1.7")
    );
    assert_eq!(
        layout.chunk_refs(CHECKSUM),
        format!("backups/dr/dr-west/chunks-ref/ab/12/{CHECKSUM}/")
    );
    assert_eq!(
        layout.chunk_rm(CHECKSUM),
        format!("backups/dr/dr-west/chunks-rm/ab/12/{CHECKSUM}")
    );

    // A reference is filed at the chunk's own shard and name, so "does anything still
    // reference this?" is a listing of one prefix rather than a search.
    assert!(layout
        .chunk_ref(CHECKSUM, "east", "uid-1", 7)
        .starts_with(&layout.chunk_refs(CHECKSUM)));

    // The offset is deliberately absent: the checksum says what the bytes are, and the
    // point's entry says where they go. Identical content at two offsets is one object.
    assert_eq!(layout.chunk(CHECKSUM), layout.chunk(CHECKSUM));

    // Under `dedup: false` a chunk belongs to the point that introduced it instead.
    assert_eq!(
        layout.point_chunk("east", "uid-1", 7, CHECKSUM),
        format!("backups/dr/dr-west/east/uid-1/pits/0000000000000007/chunks/ab/12/{CHECKSUM}")
    );
    assert_ne!(
        layout.point_chunk("east", "uid-1", 7, CHECKSUM),
        layout.chunk(CHECKSUM)
    );
}

#[test]
fn a_checksum_too_short_to_shard_is_still_a_key() {
    // Key construction is total on purpose: a malformed checksum is caught where
    // checksums are made, so that every caller here is not handling an error that
    // cannot happen. It files under what digits it has.
    assert_eq!(
        layout().chunk("ab12"),
        "backups/dr/dr-west/chunks/ab/12/ab12"
    );
    assert_eq!(layout().chunk("ab1"), "backups/dr/dr-west/chunks/ab/ab1");
    assert_eq!(layout().chunk("a"), "backups/dr/dr-west/chunks/a");
}

#[test]
fn reserved_cluster_document() {
    assert_eq!(
        cluster_cfg("backups", "east", "uid-1"),
        "backups/clusters/east/uid-1/cluster.cfg"
    );
    assert_eq!(
        cluster_cfg("", "east", "uid-1"),
        "clusters/east/uid-1/cluster.cfg"
    );
}

/// The keys a relationship builds are keys a store accepts, and a side's subtree
/// actually contains what the layout says it does.
#[tokio::test]
async fn keys_round_trip_through_a_store() {
    let layout = layout();
    let store = Store::memory(layout.clone());
    let data = bytes::Bytes::from_static(b"x");

    store
        .put(&layout.cfg("east", "uid-1"), data.clone())
        .await
        .unwrap();
    store
        .put(&layout.state("east", "uid-1"), data.clone())
        .await
        .unwrap();
    store
        .put(&layout.point("east", "uid-1", 1), data.clone())
        .await
        .unwrap();
    store
        .put(&layout.cfg("west", "uid-2"), data.clone())
        .await
        .unwrap();
    store
        .put(&layout.chunk(CHECKSUM), data.clone())
        .await
        .unwrap();

    // One side's subtree holds that side's three keys and nothing of the other's.
    let east = store.list(&layout.side("east", "uid-1")).await.unwrap();
    assert_eq!(east.len(), 3);

    // The chunk pool is outside both, which is what lets the two sides share it.
    assert!(!east
        .iter()
        .any(|entry| entry.location.as_ref() == layout.chunk(CHECKSUM)));

    // And the one listing on the identity path finds both sides' configs.
    let all = store.list(&layout.sides()).await.unwrap();
    assert_eq!(
        all.iter()
            .filter(|entry| entry.location.as_ref().ends_with("/dr.cfg"))
            .count(),
        2
    );
}
