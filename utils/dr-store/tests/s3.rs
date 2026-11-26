//! The verbs against a real object store.
//!
//! The same suite the in-memory store runs, plus what only an implementation can answer
//! for: the error it returns for a key it will not serve, and whether it evaluates a
//! conditional delete at all - the design's one hard requirement, and the thing two of
//! the three stores measured get wrong.
//!
//! The store is the deployer's, brought up with nothing else around it. Which image,
//! which credentials and which bucket it has are the deployer's to say, so this does not
//! carry a second copy of them to drift from.

mod common;

use deployer_cluster::{ClusterBuilder, S3Access};
use dr_store::{DrStore, Error, S3Config, S3Keys, Store};

fn config(s3: &S3Access) -> S3Config {
    S3Config {
        endpoint: s3.endpoint.clone(),
        region: s3.region.clone(),
        bucket: s3.bucket.clone(),
        keys: Some(S3Keys {
            access_key_id: s3.access_key_id.clone(),
            secret_access_key: s3.secret_access_key.clone(),
        }),
        ca: None,
    }
}

#[tokio::test]
async fn s3() {
    // Everything off but the object store: this tests a bucket, not a control plane.
    let cluster = ClusterBuilder::builder()
        .with_io_engines(0)
        .with_agents(vec![])
        .with_rest(false)
        .with_jaeger(false)
        .with_rustfs(true)
        .build()
        .await
        .unwrap();

    let s3 = cluster.s3();
    let store = Store::s3(&config(&s3)).unwrap();

    common::suite(&store).await;
    denied(&s3).await;
}

/// What a store does when it will not serve the request.
///
/// The mapping itself is covered where it is written, over every variant a store can
/// answer with; this is the end of that, and the one thing only an implementation can
/// say: that a refusal arrives as a refusal, over the wire, and that the signed delete
/// - which builds its own request and so has its own way to get this wrong - agrees.
async fn denied(s3: &S3Access) {
    let wrong = S3Config {
        keys: Some(S3Keys {
            access_key_id: s3.access_key_id.clone(),
            secret_access_key: "not-the-secret".to_string(),
        }),
        ..config(s3)
    };
    let store = Store::s3(&wrong).unwrap();
    let key = common::layout().cfg("east", "uid-1");
    let error = store.get(&key).await.unwrap_err();
    // Refused, and told apart from absent: a caller that confused the two would read a
    // bad credential as an empty relationship.
    assert!(
        matches!(error, Error::Denied { .. }),
        "expected a denial, got {error}"
    );
    assert!(!error.not_found());

    // The conditional delete signs its own request, so it has its own way to get this
    // wrong, and is checked separately.
    let error = store
        .delete_if_match(&key, "\"whatever\"")
        .await
        .unwrap_err();
    assert!(
        matches!(error, Error::Denied { .. }),
        "expected a denial from the signed delete, got {error}"
    );
}
