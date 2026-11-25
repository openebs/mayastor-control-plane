//! What every store must answer the same way, whichever one it is.
//!
//! Driven over both backends. The in-memory store is only worth having if a caller
//! exercised against it is exercised against the real semantics, and the way to know
//! that is to run one body against both rather than two bodies that agree by eye.

use bytes::Bytes;
use dr_store::{
    Checksum, ClusterState, DrConfig, DrStore, Error, HashAlgorithm, Layout, Role, Side, Sides,
    UploadFormat, VERSION,
};

/// A checksum to address a chunk by. Not of real content - what matters is that it is
/// the same on both runs.
pub fn checksum() -> Checksum {
    "ab12cd34ef567890ab12cd34ef567890ab12cd34ef567890ab12cd34ef567890"
        .parse()
        .unwrap()
}

/// A relationship to build keys under. Every key the suite touches comes from here, so
/// nothing in it knows the shape of the store's namespace.
pub fn layout() -> Layout {
    Layout::new("backups", "dr-west")
}

pub fn bytes(of: &str) -> Bytes {
    Bytes::from(of.to_string())
}

/// Everything a store must answer, in order.
///
/// Takes `&dyn DrStore` rather than a type: what every implementation owes its callers
/// is the same, and taking the interface is what keeps this honest when the next one is
/// a peer agent rather than a bucket.
pub async fn suite(store: &dyn DrStore) {
    verbs(store).await;
    listing(store).await;
    conditional_delete(store).await;
    sides(store).await;
}

pub async fn verbs(store: &dyn DrStore) {
    let key = layout().cfg("east", "uid-1");
    let etag = store.put(&key, bytes("first")).await.unwrap();
    assert!(etag.is_some(), "the store issues a tag on a write");
    assert_eq!(store.get(&key).await.unwrap(), bytes("first"));

    let head = store.head(&key).await.unwrap();
    assert_eq!(head.size, 5);
    assert_eq!(head.location.as_ref(), key);
    assert_eq!(head.e_tag, etag);

    // A rewrite moves the tag, which is what the collector's condition turns on.
    store.put(&key, bytes("second")).await.unwrap();
    assert_ne!(store.head(&key).await.unwrap().e_tag, etag);

    // What the store says about a key it does not hold, as the one distinction the
    // callers act on.
    let missing = layout().cfg("east", "nobody");
    assert!(store.get(&missing).await.unwrap_err().not_found());
    assert!(store.head(&missing).await.unwrap_err().not_found());
    assert!(store.get_opt(&missing).await.unwrap().is_none());
    assert!(!store.exists(&missing).await.unwrap());

    store.delete(&missing).await.unwrap();
    store.delete(&key).await.unwrap();
    assert!(!store.exists(&key).await.unwrap());
}

pub async fn listing(store: &dyn DrStore) {
    let layout = layout();
    for seq in 0..5 {
        store
            .put(&layout.point("east", "uid-1", seq), bytes("x"))
            .await
            .unwrap();
    }
    store
        .put(&layout.cfg("west", "uid-2"), bytes("x"))
        .await
        .unwrap();

    // One level at a time: the sides, and none of what is under them.
    let sides = layout.sides();
    assert_eq!(
        store.list_dirs(&sides).await.unwrap(),
        vec![format!("{sides}east/"), format!("{sides}west/")]
    );
    assert_eq!(
        store.list_dirs(&format!("{sides}east/")).await.unwrap(),
        vec![layout.side("east", "uid-1")]
    );
    assert_eq!(
        store.list_dirs(&layout.cfg("west", "uid-2")).await.unwrap(),
        Vec::<String>::new()
    );

    // One side's points, and nothing of the other side.
    let points = store.list(&layout.pits("east", "uid-1")).await.unwrap();
    assert_eq!(points.len(), 5);
    let keys: Vec<_> = points.iter().map(|e| e.location.to_string()).collect();
    assert!(keys
        .iter()
        .all(|key| key.starts_with(&layout.pits("east", "uid-1"))));
    // Zero padded, so the store's lexicographic order is sequence order.
    assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));

    // Resuming after a key is exclusive, and takes a key rather than a cursor the store
    // issued, so a walk survives the process that started it.
    let third = layout.point("east", "uid-1", 2);
    let rest = store
        .list_after(&layout.pits("east", "uid-1"), &third)
        .await
        .unwrap();
    assert_eq!(
        rest.iter()
            .map(|e| e.location.to_string())
            .collect::<Vec<_>>(),
        vec![
            layout.point("east", "uid-1", 3),
            layout.point("east", "uid-1", 4)
        ]
    );

    for entry in points {
        store.delete(entry.location.as_ref()).await.unwrap();
    }
    store.delete(&layout.cfg("west", "uid-2")).await.unwrap();
}

pub async fn conditional_delete(store: &dyn DrStore) {
    let key = layout().chunk(&checksum());
    let stale = store.put(&key, bytes("x")).await.unwrap().expect("a tag");
    store.put(&key, bytes("y")).await.unwrap();

    // The store evaluates the condition rather than ignoring the header: it refuses,
    // and - the half that matters - the object is still there afterwards.
    let error = store.delete_if_match(&key, &stale).await.unwrap_err();
    assert!(matches!(error, Error::Precondition { .. }), "{error}");
    assert!(store.exists(&key).await.unwrap());

    let current = store.head(&key).await.unwrap().e_tag.expect("a tag");
    store.delete_if_match(&key, &current).await.unwrap();
    assert!(!store.exists(&key).await.unwrap());

    // What a relationship runs before it relies on any of the above.
    let probe = layout().chunk_rm(&checksum());
    assert!(store.probe_conditional_delete(&probe).await.unwrap());
    assert!(!store.exists(&probe).await.unwrap(), "the probe cleans up");
}

fn cfg(side: &Side, peer: &str, role: Role) -> DrConfig {
    DrConfig {
        version: VERSION,
        dr: "a1b2c3d4-0000-4000-8000-000000000001".parse().unwrap(),
        cluster: side.cluster.clone(),
        uid: side.uid.clone(),
        peer: peer.to_string(),
        volume: "a1b2c3d4-0000-4000-8000-000000000002".parse().unwrap(),
        role,
        cluster_size: 4 << 20,
        chunk_size: 4 << 20,
        upload_format: UploadFormat::SyntheticFull,
        dedup: true,
        hash: HashAlgorithm::Blake3,
        disabled: None,
    }
}

/// Publishing and reading what each side says about itself, and finding the peer.
pub async fn sides(store: &dyn DrStore) {
    let layout = &layout();
    let east = Side::new("east", "uid-1");
    // A loopback: one cluster holding both ends, told apart by the name alone.
    let west = Side::new("west", "uid-1");

    // Nothing published is an answer, not a failure.
    assert_eq!(store.list_sides(layout).await.unwrap(), vec![]);
    assert_eq!(store.get_cfg(layout, &east).await.unwrap(), None);
    assert_eq!(store.get_state(layout, &east).await.unwrap(), None);
    assert_eq!(store.find_peer(layout, "west").await.unwrap(), None);

    let source = cfg(&east, "west", Role::Source);
    store.put_cfg(layout, &source).await.unwrap();
    assert_eq!(
        store.get_cfg(layout, &east).await.unwrap(),
        Some(source.clone())
    );
    // A side that has joined is found, and a point under it does not make it two.
    store
        .put(&layout.point("east", "uid-1", 0), bytes("x"))
        .await
        .unwrap();
    assert_eq!(store.list_sides(layout).await.unwrap(), vec![east.clone()]);
    assert_eq!(
        store.find_peer(layout, "east").await.unwrap(),
        Some(source.clone())
    );
    assert_eq!(store.find_peer(layout, "west").await.unwrap(), None);

    let destination = cfg(&west, "east", Role::Destination);
    store.put_cfg(layout, &destination).await.unwrap();
    let mut listed = store.list_sides(layout).await.unwrap();
    listed.sort();
    assert_eq!(listed, vec![east.clone(), west.clone()]);
    assert_eq!(
        store.find_peer(layout, "west").await.unwrap(),
        Some(destination)
    );

    // The state rewritten in place, and read back as written.
    let mut state = ClusterState::new("east", source.dr, source.volume, Role::Source);
    store.put_state(layout, &east, &state).await.unwrap();
    assert_eq!(
        store.get_state(layout, &east).await.unwrap(),
        Some(state.clone())
    );
    state.head = Some(0);
    state.head_at = Some(state.observed_at);
    store.put_state(layout, &east, &state).await.unwrap();
    assert_eq!(
        store.get_state(layout, &east).await.unwrap(),
        Some(state.clone())
    );
    assert_eq!(store.get_state(layout, &west).await.unwrap(), None);
    // Published only under the side it names.
    let error = store.put_state(layout, &west, &state).await.unwrap_err();
    assert!(matches!(error, Error::Config { .. }), "{error}");

    // A second cluster answering to the peer's name - a cluster rebuilt under the same
    // name, say - is refused rather than guessed between.
    let rebuilt = Side::new("west", "uid-2");
    store
        .put_cfg(layout, &cfg(&rebuilt, "east", Role::Destination))
        .await
        .unwrap();
    let error = store.find_peer(layout, "west").await.unwrap_err();
    assert!(matches!(error, Error::AmbiguousPeer { .. }), "{error}");

    // There and unreadable is an error, never absent: a peer whose document cannot be
    // read is not a peer that has not joined.
    let garbled = layout.state("west", "uid-1");
    store.put(&garbled, bytes("{ not json")).await.unwrap();
    let error = store.get_state(layout, &west).await.unwrap_err();
    assert!(matches!(error, Error::Malformed { .. }), "{error}");
    // As is a document that describes a side other than the one it is filed under.
    let misfiled = layout.cfg("west", "uid-2");
    let other = serde_json::to_vec(&source).unwrap();
    store.put(&misfiled, other.into()).await.unwrap();
    let error = store.get_cfg(layout, &rebuilt).await.unwrap_err();
    assert!(matches!(error, Error::Malformed { .. }), "{error}");
    let error = store.find_peer(layout, "west").await.unwrap_err();
    assert!(matches!(error, Error::Malformed { .. }), "{error}");

    for entry in store.list(layout.root()).await.unwrap() {
        store.delete(entry.location.as_ref()).await.unwrap();
    }
}
