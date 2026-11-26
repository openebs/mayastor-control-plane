use deployer_cluster::{Cluster, ClusterBuilder};
use dr_store::{Clusters, Prefix};
use grpc::operations::dr::{client::DrClient, traits::DrOperations};
use stor_port::{
    transport_api::ReplyErrorKind,
    types::v0::{
        openapi::models,
        transport::{
            CreateDrClusterPair, DestroyDrClusterPair, DrS3Credentials, DrTransport,
            GetDrClusterPair,
        },
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

/// The cluster's pair `id` to `peer`, over the cluster's store.
fn create(cluster: &Cluster, id: &str, peer: &str) -> CreateDrClusterPair {
    cluster.dr_pair(id, "east", peer)
}

/// The same pair, carrying `secret` for its store instead of the one it takes.
fn with_secret(mut pair: CreateDrClusterPair, secret: &str) -> CreateDrClusterPair {
    let DrTransport::S3(s3) = &mut pair.transport;
    let Some(DrS3Credentials::Keys(keys)) = &mut s3.credentials else {
        panic!("the cluster's pair to carry keys");
    };
    keys.secret_key = secret.to_string();
    pair
}

/// The same pair, naming its credentials rather than carrying them.
fn with_credentials(mut pair: CreateDrClusterPair, name: &str) -> CreateDrClusterPair {
    let DrTransport::S3(s3) = &mut pair.transport;
    s3.credentials = Some(DrS3Credentials::Named(name.to_string()));
    pair
}

/// The secret the cluster's store takes.
fn store_secret(cluster: &Cluster) -> String {
    cluster.s3().secret_access_key
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
    let pair = with_secret(create(cluster, "dr-west", "west"), secret);
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

/// The pairs the agent holds: created, published to the bucket, amended, refused where the
/// bucket refuses them, read back, surviving a restart, removed.
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

    let asked = create(&cluster, "dr-west", "west");
    let pair = dr.create_pair(&asked, None).await.unwrap();
    assert_eq!(pair.id.as_str(), "dr-west");
    assert_eq!(pair.peer.as_str(), "west");
    assert!(
        !pair.cluster_uid.as_str().is_empty(),
        "the uid is read from the platform at creation, not left for later"
    );
    assert_eq!(pair.transport, asked.transport);

    // A pair is per cluster, and what it puts in the bucket is the cluster's own
    // document: the peer finds this cluster there before any volume is replicated.
    let bucket = super::bucket(&cluster);
    let uid = pair.cluster_uid.as_str();
    let published = bucket
        .get_cluster_cfg(&Prefix::new(""), "east", uid)
        .await
        .unwrap()
        .expect("the cluster's cluster.cfg");
    assert_eq!(
        (published.cluster.as_str(), published.uid.as_str()),
        ("east", uid)
    );

    // Create is an upsert: the same ends with a moved store converge rather than failing,
    // and publish to where they moved.
    let mut moved = create(&cluster, "dr-west", "west");
    let DrTransport::S3(s3) = &mut moved.transport;
    s3.prefix = Some("moved".to_string());
    let amended = dr.create_pair(&moved, None).await.unwrap();
    assert_eq!(amended.transport, moved.transport);
    assert!(bucket
        .get_cluster_cfg(&Prefix::new("moved"), "east", uid)
        .await
        .unwrap()
        .is_some());

    // A store the pair cannot write to is refused, and the pair stays as it was: a bucket
    // that is not there, and a credential the store does not take.
    let mut nowhere = moved.clone();
    let DrTransport::S3(s3) = &mut nowhere.transport;
    s3.bucket = "elsewhere".to_string();
    let error = dr.create_pair(&nowhere, None).await.unwrap_err();
    assert_eq!(error.kind, ReplyErrorKind::FailedPrecondition, "{error:?}");
    let error = dr
        .create_pair(&with_secret(moved.clone(), "not-the-secret"), None)
        .await
        .unwrap_err();
    assert_eq!(error.kind, ReplyErrorKind::FailedPrecondition, "{error:?}");
    let id = GetDrClusterPair {
        id: "dr-west".into(),
    };
    assert_eq!(dr.get_pair(&id, None).await.unwrap(), amended);
    // Nor is a pair created over one.
    let mut refused = create(&cluster, "dr-nowhere", "west");
    let DrTransport::S3(s3) = &mut refused.transport;
    s3.bucket = "elsewhere".to_string();
    dr.create_pair(&refused, None)
        .await
        .expect_err("a pair over a bucket that is not there");
    let nowhere = GetDrClusterPair {
        id: "dr-nowhere".into(),
    };
    dr.get_pair(&nowhere, None)
        .await
        .expect_err("a pair the bucket refused");

    // The two names are not amended: they say which relationship this is.
    dr.create_pair(&create(&cluster, "dr-west", "north"), None)
        .await
        .expect_err("a put repointing a pair at a different peer");

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
    let secret_key = store_secret(&cluster);
    let created = api
        .put_dr_pair("dr-west", put_body(&cluster, &secret_key))
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
    assert_eq!(secret(&shown), secret_key);
    let listed = api.get_dr_pairs(Some(true)).await.unwrap();
    assert!(listed.iter().all(|pair| secret(pair) == secret_key));

    // Putting the withheld secret back would replace a working credential with nothing.
    api.put_dr_pair("dr-west", put_body(&cluster, ""))
        .await
        .expect_err("a put carrying the empty secret a get answers with");
    let shown = api.get_dr_pair("dr-west", Some(true)).await.unwrap();
    assert_eq!(secret(&shown), secret_key);

    // Names the store layout is built from: not empty, and a single path segment.
    dr.create_pair(&create(&cluster, "", "west"), None)
        .await
        .expect_err("an empty id");
    dr.create_pair(&create(&cluster, "a/b", "west"), None)
        .await
        .expect_err("an id carrying a separator");
    dr.create_pair(&create(&cluster, "slashed", "a/b"), None)
        .await
        .expect_err("a cluster name carrying a separator");
    dr.create_pair(&create(&cluster, "..", "west"), None)
        .await
        .expect_err("an id that is a relative path");
    // A side name is joined with dots into a chunk reference, so it may not carry one.
    dr.create_pair(&create(&cluster, "dotted", "a.b"), None)
        .await
        .expect_err("a side name carrying a dot");

    // Named credentials are looked up in a directory this agent was given none of.
    let named = with_credentials(create(&cluster, "dr-named", "west"), "east");
    let error = dr.create_pair(&named, None).await.unwrap_err();
    assert_eq!(error.kind, ReplyErrorKind::FailedPrecondition, "{error:?}");

    // Both ends must differ, or neither side can tell its subtree from the other's.
    dr.create_pair(&create(&cluster, "same", "east"), None)
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

/// A directory of named credentials on the host, laid out as mounted secrets are.
struct Credentials(std::path::PathBuf);

impl Credentials {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("dr-credentials-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    /// A secret called `name`, holding `files`.
    fn secret(&self, name: &str, files: &[(&str, &str)]) {
        let dir = self.0.join(name);
        std::fs::create_dir_all(&dir).unwrap();
        for (file, content) in files {
            std::fs::write(dir.join(file), content).unwrap();
        }
    }
}

impl Drop for Credentials {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A pair that names its credentials gets them, and the store's CA, from the secret of
/// that name mounted under the agent's credentials directory.
#[tokio::test]
async fn dr_named_credentials() {
    let credentials = Credentials::new();
    let dir = credentials.0.clone();
    let cluster = ClusterBuilder::builder()
        .with_io_engines(0)
        .with_dr(true)
        .with_options(|options| options.with_dr_credentials_dir(&dir))
        .build()
        .await
        .unwrap();
    let s3 = cluster.s3();
    let access = s3.access_key_id.as_str();
    let secret = s3.secret_access_key.as_str();
    // As a secret is usually written to a file: with a newline.
    let secret_line = format!("{secret}\n");
    let ca = rcgen::generate_simple_self_signed(vec!["rustfs".to_string()])
        .unwrap()
        .cert
        .pem();
    credentials.secret(
        "store",
        &[("accessKey", access), ("secretKey", &secret_line)],
    );
    credentials.secret(
        "private",
        &[
            ("accessKey", access),
            ("secretKey", secret),
            ("ca.crt", &ca),
        ],
    );
    credentials.secret("wrong", &[("accessKey", access), ("secretKey", "not-it")]);
    credentials.secret("half", &[("accessKey", access)]);
    credentials.secret(
        "garbled",
        &[
            ("accessKey", access),
            ("secretKey", secret),
            ("ca.crt", "not a certificate"),
        ],
    );

    let dr = dr_client(&cluster).await;
    let bucket = super::bucket(&cluster);
    let named = |name: &str| with_credentials(create(&cluster, "dr-west", "west"), name);

    let pair = dr.create_pair(&named("store"), None).await.unwrap();
    // What was published was signed with the named keys.
    assert!(bucket
        .get_cluster_cfg(&Prefix::new(""), "east", pair.cluster_uid.as_str())
        .await
        .unwrap()
        .is_some());
    // The pair records the name, never what it resolved to.
    let DrTransport::S3(transport) = &pair.transport;
    assert_eq!(
        transport.credentials,
        Some(DrS3Credentials::Named("store".to_string()))
    );

    // A CA is taken alongside the keys. This store is plain http, so nothing is verified
    // against it: what this shows is that one is read and accepted.
    dr.create_pair(&named("private"), None).await.unwrap();

    // Credentials the store refuses, that are not there, that are half there, or whose CA
    // cannot be read: refused, until the secret is fixed.
    for name in ["wrong", "missing", "half", "garbled"] {
        let error = dr.create_pair(&named(name), None).await.unwrap_err();
        assert_eq!(
            error.kind,
            ReplyErrorKind::FailedPrecondition,
            "{name}: {error:?}"
        );
    }
    // A name is one directory under the credentials directory, and none of the hidden
    // ones a mounted secret keeps its versions in.
    for name in ["../store", "..data", ""] {
        let error = dr.create_pair(&named(name), None).await.unwrap_err();
        assert_eq!(
            error.kind,
            ReplyErrorKind::InvalidArgument,
            "{name:?}: {error:?}"
        );
    }
}
