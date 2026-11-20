//! What every store must answer the same way, whichever one it is.
//!
//! Driven over both backends. The in-memory store is only worth having if a caller
//! exercised against it is exercised against the real semantics, and the way to know
//! that is to run one body against both rather than two bodies that agree by eye.

use bytes::Bytes;
use dr_store::{Checksum, DrStore, Error, Layout};

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
}

pub async fn verbs(store: &dyn DrStore) {
    let key = store.layout().cfg("east", "uid-1");
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
    let missing = store.layout().cfg("east", "nobody");
    assert!(store.get(&missing).await.unwrap_err().not_found());
    assert!(store.head(&missing).await.unwrap_err().not_found());
    assert!(store.get_opt(&missing).await.unwrap().is_none());
    assert!(!store.exists(&missing).await.unwrap());

    store.delete(&missing).await.unwrap();
    store.delete(&key).await.unwrap();
    assert!(!store.exists(&key).await.unwrap());
}

pub async fn listing(store: &dyn DrStore) {
    let layout = store.layout().clone();
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
    let key = store.layout().chunk(&checksum());
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
    let probe = store.layout().chunk_rm(&checksum());
    assert!(store.probe_conditional_delete(&probe).await.unwrap());
    assert!(!store.exists(&probe).await.unwrap(), "the probe cleans up");
}
