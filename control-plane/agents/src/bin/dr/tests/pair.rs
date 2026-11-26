use deployer_cluster::{Cluster, ClusterBuilder};
use grpc::operations::dr::{client::DrClient, traits::DrOperations};
use stor_port::types::v0::{
    openapi::models,
    transport::{
        CreateDrClusterPair, DestroyDrClusterPair, DrS3Credentials, DrTransport, GetDrClusterPair,
    },
};

/// A client for the cluster's dr agent.
async fn dr_client(cluster: &Cluster) -> DrClient {
    let ip = cluster.composer().container_ip("agent-dr");
    // https: the deployer gives the agent an auto-signed certificate.
    let uri = format!("https://{ip}:11700")
        .parse()
        .expect("a well-formed uri");
    DrClient::new(uri, None).await
}

/// The cluster's pair `id` to `peer`, carrying `secret` for its store.
fn create(cluster: &Cluster, id: &str, peer: &str, secret: &str) -> CreateDrClusterPair {
    let mut pair = cluster.dr_pair(id, "east", peer);
    let DrTransport::S3(s3) = &mut pair.transport;
    let Some(DrS3Credentials::Keys(keys)) = &mut s3.credentials else {
        panic!("the cluster's pair to carry keys");
    };
    keys.secret_key = secret.to_string();
    pair
}

/// The store secret a pair carries, as rest reports it.
fn secret(pair: &models::DrClusterPair) -> String {
    let models::DrTransport::s3(s3) = pair.transport.clone().expect("a transport");
    match s3.credentials {
        Some(models::DrS3Credentials::keys(keys)) => keys.secret_key,
        other => panic!("expected keys, got {other:?}"),
    }
}

/// The pair as a put states it.
fn put_body(cluster: &Cluster, secret: &str) -> models::CreateDrClusterPairBody {
    let pair = create(cluster, "dr-west", "west", secret);
    let DrTransport::S3(s3) = pair.transport;
    let Some(DrS3Credentials::Keys(keys)) = s3.credentials else {
        panic!("the cluster's pair to carry keys");
    };
    models::CreateDrClusterPairBody::new_all(
        pair.cluster.as_str(),
        pair.peer.as_str(),
        models::DrTransport::s3(models::DrS3Transport::new_all(
            s3.endpoint,
            s3.bucket,
            s3.prefix,
            s3.region,
            models::DrS3Credentials::keys(models::DrS3Keys::new(keys.access_key, keys.secret_key)),
        )),
    )
}

/// The pairs the agent holds: created, amended, read back, surviving a restart, removed.
#[tokio::test]
async fn dr_cluster_pair() {
    let cluster = ClusterBuilder::builder()
        // A pair is a record; nothing here touches a volume.
        .with_io_engines(0)
        // What `--dr` stands for: the agent, and the store it will work through.
        .with_dr(true)
        .build()
        .await
        .unwrap();

    // The cluster waited on the agent's probe as it came up.
    let dr = dr_client(&cluster).await;

    let asked = create(&cluster, "dr-west", "west", "supersecret");
    let pair = dr.create_pair(&asked, None).await.unwrap();
    assert_eq!(pair.id.as_str(), "dr-west");
    assert_eq!(pair.peer.as_str(), "west");
    assert!(
        !pair.cluster_uid.as_str().is_empty(),
        "the uid is read from the platform at creation, not left for later"
    );
    assert_eq!(pair.transport, asked.transport);

    // Create is an upsert: the same ends with a moved store converge rather than failing.
    let mut moved = create(&cluster, "dr-west", "west", "rotated");
    let DrTransport::S3(s3) = &mut moved.transport;
    s3.bucket = "elsewhere".to_string();
    let amended = dr.create_pair(&moved, None).await.unwrap();
    assert_eq!(amended.transport, moved.transport);

    // The two names are not amended: they say which relationship this is.
    dr.create_pair(&create(&cluster, "dr-west", "north", "supersecret"), None)
        .await
        .expect_err("a put repointing a pair at a different peer");

    let id = GetDrClusterPair {
        id: "dr-west".into(),
    };
    assert_eq!(dr.get_pair(&id, None).await.unwrap(), amended);
    assert_eq!(
        dr.list_pairs(None).await.unwrap().entries,
        vec![amended.clone()]
    );

    // A pair outlives the agent because it is read back from the persistent store.
    cluster.composer().restart("agent-dr").await.unwrap();
    let dr = dr_client(&cluster).await;
    // Nothing waits on a restart, and the agent reads its pairs back before it serves.
    dr.wait_ready(None).await.expect("the agent to be serving");
    assert_eq!(dr.get_pair(&id, None).await.unwrap(), amended);

    // The agent answers with the secret - `amended` was compared against it above - and
    // rest is what withholds it, from every answer that was not asked.
    let rest = cluster.rest_v00();
    let api = rest.disaster_recovery_api();

    // A put answers with what it stored, minus the secret.
    let created = api
        .put_dr_pair("dr-west", put_body(&cluster, "hunter2"))
        .await
        .unwrap();
    assert_eq!(secret(&created), "");

    // So does a get, and every pair a list answers for.
    let got = api.get_dr_pair("dr-west", None).await.unwrap();
    assert_eq!(secret(&got), "");
    let listed = api.get_dr_pairs(None).await.unwrap();
    assert!(listed.iter().all(|pair| secret(pair).is_empty()));

    // Only asking for it answers with it.
    let shown = api.get_dr_pair("dr-west", Some(true)).await.unwrap();
    assert_eq!(secret(&shown), "hunter2");
    let listed = api.get_dr_pairs(Some(true)).await.unwrap();
    assert!(listed.iter().all(|pair| secret(pair) == "hunter2"));

    // Putting the withheld secret back would replace a working credential with nothing.
    api.put_dr_pair("dr-west", put_body(&cluster, ""))
        .await
        .expect_err("a put carrying the empty secret a get answers with");
    let shown = api.get_dr_pair("dr-west", Some(true)).await.unwrap();
    assert_eq!(secret(&shown), "hunter2");

    // Names the store layout is built from: not empty, and a single path segment.
    dr.create_pair(&create(&cluster, "", "west", "s"), None)
        .await
        .expect_err("an empty id");
    dr.create_pair(&create(&cluster, "a/b", "west", "s"), None)
        .await
        .expect_err("an id carrying a separator");
    dr.create_pair(&create(&cluster, "slashed", "a/b", "s"), None)
        .await
        .expect_err("a cluster name carrying a separator");
    dr.create_pair(&create(&cluster, "..", "west", "s"), None)
        .await
        .expect_err("an id that is a relative path");
    // A side name is joined with dots into a chunk reference, so it may not carry one.
    dr.create_pair(&create(&cluster, "dotted", "a.b", "s"), None)
        .await
        .expect_err("a side name carrying a dot");

    // Both ends must differ, or neither side can tell its subtree from the other's.
    dr.create_pair(&create(&cluster, "same", "east", "s"), None)
        .await
        .expect_err("a pair whose cluster and peer are the same name");

    let destroy = DestroyDrClusterPair {
        id: "dr-west".into(),
    };
    dr.destroy_pair(&destroy, None).await.unwrap();
    dr.get_pair(&id, None).await.expect_err("a removed pair");
    dr.destroy_pair(&destroy, None)
        .await
        .expect_err("removing what is not there");
    assert!(dr.list_pairs(None).await.unwrap().entries.is_empty());
}
