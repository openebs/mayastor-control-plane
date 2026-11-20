use deployer_cluster::{Cluster, ClusterBuilder};
use grpc::operations::dr::{client::DrClient, traits::DrOperations};
use stor_port::types::v0::transport::{
    CreateDrClusterPair, DestroyDrClusterPair, DrS3Credentials, DrS3Keys, DrS3Transport,
    DrTransport, GetDrClusterPair,
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

fn transport(secret: &str) -> DrTransport {
    DrTransport::S3(DrS3Transport {
        endpoint: "http://rustfs:9000".to_string(),
        bucket: "default".to_string(),
        prefix: Some("prod".to_string()),
        region: Some("us-east-1".to_string()),
        credentials: Some(DrS3Credentials::Keys(DrS3Keys {
            access_key: "GKde9ade".to_string(),
            secret_key: secret.to_string(),
        })),
    })
}

fn create(id: &str, peer: &str, secret: &str) -> CreateDrClusterPair {
    CreateDrClusterPair {
        id: id.into(),
        cluster: "east".into(),
        peer: peer.into(),
        transport: transport(secret),
    }
}

/// The pairs the agent holds: created, amended, read back, surviving a restart, removed.
#[tokio::test]
async fn dr_cluster_pair() {
    let cluster = ClusterBuilder::builder()
        // A pair is a record; nothing here touches a volume.
        .with_io_engines(0)
        .with_rest(false)
        // What `--dr` stands for: the agent, and the store it will work through.
        .with_dr(true)
        .build()
        .await
        .unwrap();

    // The cluster waited on the agent's probe as it came up.
    let dr = dr_client(&cluster).await;

    let pair = dr
        .create(&create("dr-west", "west", "supersecret"), None)
        .await
        .unwrap();
    assert_eq!(pair.id.as_str(), "dr-west");
    assert_eq!(pair.peer.as_str(), "west");
    assert!(
        !pair.cluster_uid.as_str().is_empty(),
        "the uid is read from the platform at creation, not left for later"
    );
    assert_eq!(pair.transport, transport("supersecret"));

    // Create is an upsert: the same ends with a moved store converge rather than failing.
    let mut moved = create("dr-west", "west", "rotated");
    let DrTransport::S3(s3) = &mut moved.transport;
    s3.bucket = "elsewhere".to_string();
    let amended = dr.create(&moved, None).await.unwrap();
    assert_eq!(amended.transport, moved.transport);

    // The two names are not amended: they say which relationship this is.
    dr.create(&create("dr-west", "north", "supersecret"), None)
        .await
        .expect_err("a put repointing a pair at a different peer");

    let id = GetDrClusterPair {
        id: "dr-west".into(),
    };
    assert_eq!(dr.get(&id, None).await.unwrap(), amended);
    assert_eq!(dr.list(None).await.unwrap().entries, vec![amended.clone()]);

    // A pair outlives the agent because it is read back from the persistent store.
    cluster.composer().restart("agent-dr").await.unwrap();
    let dr = dr_client(&cluster).await;
    // Nothing waits on a restart, and the agent reads its pairs back before it serves.
    dr.wait_ready(None).await.expect("the agent to be serving");
    assert_eq!(dr.get(&id, None).await.unwrap(), amended);

    // Names the store layout is built from: not empty, and a single path segment.
    dr.create(&create("", "west", "s"), None)
        .await
        .expect_err("an empty id");
    dr.create(&create("a/b", "west", "s"), None)
        .await
        .expect_err("an id carrying a separator");
    dr.create(&create("slashed", "a/b", "s"), None)
        .await
        .expect_err("a cluster name carrying a separator");

    // Both ends must differ, or neither side can tell its subtree from the other's.
    dr.create(&create("same", "east", "s"), None)
        .await
        .expect_err("a pair whose cluster and peer are the same name");

    let destroy = DestroyDrClusterPair {
        id: "dr-west".into(),
    };
    dr.destroy(&destroy, None).await.unwrap();
    dr.get(&id, None).await.expect_err("a removed pair");
    dr.destroy(&destroy, None)
        .await
        .expect_err("removing what is not there");
    assert!(dr.list(None).await.unwrap().entries.is_empty());
}
