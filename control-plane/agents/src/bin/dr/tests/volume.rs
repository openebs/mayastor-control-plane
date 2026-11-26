use deployer_cluster::{Cluster, ClusterBuilder};
use grpc::operations::dr::{client::DrClient, traits::DrOperations};
use std::time::Duration;
use stor_port::types::v0::{
    openapi::models,
    store::SpecStatus,
    transport::{
        DestroyDrClusterPair, DisableDrVolume, DrPolicy, DrRole, EnableDrVolume, GetDrVolume,
        VolumeId,
    },
};

/// The volume replication is enabled on.
const SOURCE: &str = "8c1f2a3b-5d4e-4f60-9a7b-1c2d3e4f5a6b";
/// A volume joining a relationship enabled elsewhere.
const DESTINATION: &str = "1a2b3c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d";
/// Not a volume here.
const UNKNOWN: &str = "4f2e1a90-6b7c-4d8e-9f01-2a3b4c5d6e7f";

/// A client for the cluster's dr agent.
async fn dr_client(cluster: &Cluster) -> DrClient {
    let ip = cluster.composer().container_ip("agent-dr");
    // https: the deployer gives the agent an auto-signed certificate.
    let uri = format!("https://{ip}:11700")
        .parse()
        .expect("a well-formed uri");
    DrClient::new(uri, None).await
}

fn id(uuid: &str) -> VolumeId {
    VolumeId::try_from(uuid).expect("a uuid")
}

fn policy(schedule: Duration) -> DrPolicy {
    DrPolicy {
        auto_resync: false,
        schedule,
        ttl: Duration::from_secs(24 * 60 * 60),
        max_lag: None,
    }
}

fn enable(volume: &str, role: DrRole) -> EnableDrVolume {
    EnableDrVolume {
        volume: id(volume),
        pair: "dr-west".into(),
        role,
        policy: policy(Duration::from_secs(15 * 60)),
    }
}

/// A volume in core for an entry to protect.
async fn create_volume(cluster: &Cluster, uuid: &str) {
    cluster
        .rest_v00()
        .volumes_api()
        .put_volume(
            &uuid.parse().unwrap(),
            models::CreateVolumeBody::new(
                models::VolumePolicy::new(true),
                1,
                10 * 1024 * 1024u64,
                true,
                false,
            ),
        )
        .await
        .expect("a volume");
}

/// The volume entries the agent holds: enabled, amended, refused, surviving a restart,
/// disabled.
#[tokio::test]
async fn dr_volume() {
    let cluster = ClusterBuilder::builder()
        .with_io_engines(1)
        .with_pool(0, "malloc:///p1?size_mb=100")
        .with_dr(true)
        .build()
        .await
        .unwrap();

    // Pair both sides of the async dr relationship
    // This would usually pair 2 clusters, but we fake it out with a single cluster for
    // easy local testing with the deployer.
    let dr = dr_client(&cluster).await;
    dr.create_pair(&cluster.dr_pair("dr-west", "east", "west"), None)
        .await
        .unwrap();
    dr.create_pair(&cluster.dr_pair("dr-north", "east", "west"), None)
        .await
        .unwrap();

    // Neither the volume nor the pair can be made up.
    dr.enable_volume(&enable(SOURCE, DrRole::Source), None)
        .await
        .expect_err("on a volume that does not exist");

    // Create volumes on both sides
    create_volume(&cluster, SOURCE).await;
    create_volume(&cluster, DESTINATION).await;

    // Can't enable DR on a cluster pair which doesn't exist
    let mut unknown_pair = enable(SOURCE, DrRole::Source);
    unknown_pair.pair = "no-such-pair".into();
    dr.enable_volume(&unknown_pair, None)
        .await
        .expect_err("on a pair that does not exist");

    // Nor can we use an interval that is zero.
    let mut never = enable(SOURCE, DrRole::Source);
    never.policy.schedule = Duration::ZERO;
    dr.enable_volume(&never, None)
        .await
        .expect_err("on a zero schedule");

    // The relationship is named after the source volume. For now the source and target
    // volumes carry the same uuid, so on the source both are this volume.
    let source = dr
        .enable_volume(&enable(SOURCE, DrRole::Source), None)
        .await
        .unwrap();
    assert_eq!(source.dr.as_str(), SOURCE);
    assert_eq!(source.source_volume, id(SOURCE));
    assert_eq!(source.target_volume, Some(id(SOURCE)));
    assert_eq!(source.status, SpecStatus::Created(()));

    // And so on the destination.
    let destination = dr
        .enable_volume(&enable(DESTINATION, DrRole::Destination), None)
        .await
        .unwrap();
    assert_eq!(destination.dr.as_str(), DESTINATION);
    assert_eq!(destination.source_volume, id(DESTINATION));
    assert_eq!(destination.target_volume, Some(id(DESTINATION)));

    // Enabling again is an upsert in the policy: the same relationship converges.
    let mut amended = enable(SOURCE, DrRole::Source);
    amended.policy = policy(Duration::from_secs(5 * 60));
    let source = dr.enable_volume(&amended, None).await.unwrap();
    assert_eq!(source.policy, amended.policy);

    // Only the policy can be amended: the role and the pair are fixed once enabled.
    // Changing the role is failover or failback, not a re-enable.
    dr.enable_volume(&enable(SOURCE, DrRole::Destination), None)
        .await
        .expect_err("on a source turned destination");
    let mut moved = amended.clone();
    moved.pair = "dr-north".into();
    dr.enable_volume(&moved, None)
        .await
        .expect_err("on a relationship moved to another pair");

    // A pair is not destroyed from under the volumes replicating over it.
    let west = DestroyDrClusterPair {
        id: "dr-west".into(),
    };
    dr.destroy_pair(&west, None)
        .await
        .expect_err("a pair volumes still name");
    let north = DestroyDrClusterPair {
        id: "dr-north".into(),
    };
    dr.destroy_pair(&north, None).await.unwrap();

    // The entries outlive the agent because they are read back from the persistent store.
    cluster.composer().restart("agent-dr").await.unwrap();
    let dr = dr_client(&cluster).await;
    dr.wait_ready(None).await.expect("the agent to be serving");
    let get = GetDrVolume { volume: id(SOURCE) };
    assert_eq!(dr.get_volume(&get, None).await.unwrap(), source);
    let mut listed = dr.list_volumes(None).await.unwrap().entries;
    listed.sort_by(|a, b| a.volume.as_str().cmp(b.volume.as_str()));
    assert_eq!(listed, vec![destination, source.clone()]);

    // Rest answers with the same entry, its intervals as humantime.
    let rest = cluster.rest_v00();
    let api = rest.disaster_recovery_api();
    let rested = api.get_dr_volume(&SOURCE.parse().unwrap()).await.unwrap();
    assert_eq!(rested.dr.to_string(), SOURCE);
    assert_eq!(rested.policy.schedule, "5m");
    assert_eq!(rested.status, models::SpecStatus::Created);
    // And takes a put in the same terms.
    let body = models::EnableDrVolumeBody::new_all(
        "dr-west",
        models::DrRole::Source,
        models::DrPolicy::new_all(None, "10m", "1d", Some("1h".to_string())),
    );
    let rested = api
        .put_dr_volume(&SOURCE.parse().unwrap(), body.clone())
        .await
        .unwrap();
    assert_eq!(rested.policy.schedule, "10m");
    assert_eq!(rested.policy.max_lag.as_deref(), Some("1h"));
    let mut unparsed = body;
    unparsed.policy.schedule = "often".to_string();
    api.put_dr_volume(&SOURCE.parse().unwrap(), unparsed)
        .await
        .expect_err("on a schedule that is not humantime");

    // Disabling marks the entry rather than removing it: the relationship is purged only
    // once both ends have let go of it.
    let disable = DisableDrVolume { volume: id(SOURCE) };
    dr.disable_volume(&disable, None).await.unwrap();
    let deleting = dr.get_volume(&get, None).await.unwrap();
    assert_eq!(deleting.status, SpecStatus::Deleting);
    // Repeating it converges.
    dr.disable_volume(&disable, None).await.unwrap();
    // Enabling it again would resume a relationship being torn down.
    dr.enable_volume(&enable(SOURCE, DrRole::Source), None)
        .await
        .expect_err("enabling a volume still being disabled");
    // And the pair is still the store it is being torn down in.
    dr.destroy_pair(&west, None)
        .await
        .expect_err("a pair a disabled volume still names");

    dr.disable_volume(
        &DisableDrVolume {
            volume: id(UNKNOWN),
        },
        None,
    )
    .await
    .expect_err("disabling a volume with no entry");
}
