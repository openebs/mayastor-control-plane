use deployer_cluster::ClusterBuilder;
use grpc::operations::{
    node::traits::NodeOperations,
    pool::traits::{PoolDrainRequest, PoolOperations},
    replica::traits::ReplicaOperations,
    volume::traits::VolumeOperations,
};
use std::time::Duration;
use stor_port::types::v0::{
    store::pool::{DrainPolicy, SnapshotPolicy},
    transport::{
        AffinityGroup, CreatePool, CreateVolume, DrainDryRunReply, Filter, PoolId, VolumeId,
    },
};

const VOLUME_SIZE: u64 = 50 * 1024 * 1024;

fn drain_request(pool_id: &PoolId, unsafe_evict: bool) -> PoolDrainRequest {
    PoolDrainRequest {
        node_id: None,
        pool_id: pool_id.clone(),
        policy: DrainPolicy::new(SnapshotPolicy::Ignore, None, unsafe_evict),
    }
}

/// The dry run must not leave any drain state behind on the pool.
async fn assert_no_drain_state(pool_client: &impl PoolOperations, pool_id: &PoolId) {
    let pools = pool_client
        .get(Filter::Pool(pool_id.clone()), None)
        .await
        .unwrap()
        .into_inner();
    let config = pools[0].config().expect("pool has a config");
    assert!(
        !config.drain_state_exist(),
        "dry run must not set drain state"
    );
}

/// Drain dry run reports, for a volume whose replica cannot be moved, every constraint which
/// excluded a pool, and changes nothing.
#[tokio::test]
async fn pool_drain_dry_run() {
    let cluster = ClusterBuilder::builder()
        .with_rest(false)
        .with_agents(vec!["core"])
        .with_io_engines(3)
        // The only pool when the volume is created, so it gets the replica.
        .with_pool(0, "malloc:///disk0?size_mb=100")
        .with_cache_period("100ms")
        .with_node_deadline("100ms")
        .with_reconcile_period(Duration::from_millis(100), Duration::from_millis(100))
        .build()
        .await
        .unwrap();

    let pool_client = cluster.grpc_client().pool();
    let node_client = cluster.grpc_client().node();
    let volume_client = cluster.grpc_client().volume();
    let replica_client = cluster.grpc_client().replica();

    let source = pool_client
        .get(Filter::None, None)
        .await
        .unwrap()
        .into_inner()
        .first()
        .unwrap()
        .id()
        .clone();

    let volume = VolumeId::new();
    volume_client
        .create(
            &CreateVolume {
                uuid: volume.clone(),
                size: VOLUME_SIZE,
                replicas: 1,
                thin: false,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
    let replicas = replica_client
        .get(Filter::Volume(volume.clone()), None)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(replicas[0].pool_id, source);

    // With no other pool, nothing can receive the replica.
    let reply = pool_client
        .drain_dry_run(&drain_request(&source, false))
        .await
        .unwrap();
    assert_eq!(reply.num_replicas, 1);
    assert_eq!(reply.num_volume_replica_immovable, 1);
    assert_eq!(
        reply.immovable_resources.no_candidate_pool,
        vec![volume.clone()]
    );
    assert!(reply.immovable_resources.other.is_empty());
    assert_no_drain_state(&pool_client, &source).await;

    // The drain places a spare replica first, so another pool on the node which already holds the
    // replica is not a valid target.
    pool_client
        .create(
            &CreatePool {
                node: cluster.node(0),
                id: "pool0b".into(),
                disks: vec!["malloc:///disk0b?size_mb=100".into()],
                labels: None,
                encryption: None,
                cluster_size: None,
                max_expansion: None,
            },
            None,
        )
        .await
        .unwrap();
    let reply = pool_client
        .drain_dry_run(&drain_request(&source, false))
        .await
        .unwrap();
    assert_eq!(reply.num_volume_replica_immovable, 1);
    assert_eq!(reply.immovable_resources.nodes, vec![volume.clone()]);
    assert!(reply.immovable_resources.no_candidate_pool.is_empty());
    assert_no_drain_state(&pool_client, &source).await;

    // A pool on node 1, which we then cordon, and a pool on node 2 which is too small for the
    // volume: the volume is blocked by two different constraints.
    for (index, size_mb) in [(1, 100), (2, 20)] {
        pool_client
            .create(
                &CreatePool {
                    node: cluster.node(index),
                    id: format!("pool{index}").into(),
                    disks: vec![format!("malloc:///disk{index}?size_mb={size_mb}").into()],
                    labels: None,
                    encryption: None,
                    cluster_size: None,
                    max_expansion: None,
                },
                None,
            )
            .await
            .unwrap();
    }
    node_client
        .cordon(cluster.node(1), "dry-run".to_string())
        .await
        .unwrap();

    let reply = pool_client
        .drain_dry_run(&drain_request(&source, false))
        .await
        .unwrap();
    assert_eq!(reply.num_volume_replica_immovable, 1);
    let reasons = &reply.immovable_resources;
    assert_eq!(reasons.nodes, vec![volume.clone()]);
    assert_eq!(reasons.size, vec![volume.clone()]);
    assert!(reasons.topology.is_empty());
    assert!(reasons.cluster_size.is_empty());
    assert!(reasons.no_candidate_pool.is_empty());
    assert!(reasons.other.is_empty());
    assert_no_drain_state(&pool_client, &source).await;

    // Once a suitable pool is available the volume is movable.
    node_client
        .uncordon(cluster.node(1), "dry-run".to_string())
        .await
        .unwrap();
    let reply = pool_client
        .drain_dry_run(&drain_request(&source, false))
        .await
        .unwrap();
    assert_eq!(
        reply,
        DrainDryRunReply {
            num_replicas: 1,
            ..Default::default()
        }
    );
    assert_no_drain_state(&pool_client, &source).await;

    // The replica of a single replica volume is never evicted, so it cannot be moved.
    let reply = pool_client
        .drain_dry_run(&drain_request(&source, true))
        .await
        .unwrap();
    assert_eq!(reply.num_volume_replica_immovable, 1);
    assert_eq!(
        reply.immovable_resources.single_replica_eviction,
        vec![volume.clone()]
    );
    assert!(reply.immovable_resources.other.is_empty());
    assert_no_drain_state(&pool_client, &source).await;
}

/// Replicas which would each fit on the target pool, but not all together, are not all reported
/// as movable: the space taken by the replicas placed first is accounted for.
#[tokio::test]
async fn pool_drain_dry_run_shared_free_space() {
    const VOLUMES: usize = 10;
    const REPLICA_SIZE: u64 = 5 * 1024 * 1024;

    let cluster = ClusterBuilder::builder()
        .with_rest(false)
        .with_agents(vec!["core"])
        .with_io_engines(2)
        // The only pool when the volumes are created, so it gets all the replicas.
        .with_pool(0, "malloc:///disk0?size_mb=300")
        .with_cache_period("100ms")
        .with_node_deadline("100ms")
        .with_reconcile_period(Duration::from_millis(100), Duration::from_millis(100))
        .build()
        .await
        .unwrap();

    let pool_client = cluster.grpc_client().pool();
    let volume_client = cluster.grpc_client().volume();
    let replica_client = cluster.grpc_client().replica();

    let source = pool_client
        .get(Filter::None, None)
        .await
        .unwrap()
        .into_inner()
        .first()
        .unwrap()
        .id()
        .clone();

    let mut volumes = Vec::with_capacity(VOLUMES);
    for _ in 0..VOLUMES {
        let volume = VolumeId::new();
        volume_client
            .create(
                &CreateVolume {
                    uuid: volume.clone(),
                    size: REPLICA_SIZE,
                    replicas: 1,
                    thin: false,
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap();
        volumes.push(volume);
    }
    let replicas = replica_client.get(Filter::None, None).await.unwrap();
    assert_eq!(replicas.into_inner().len(), VOLUMES);

    // The space a replica takes on the pool, which is more than its size because of the pool's
    // cluster size and metadata.
    let source_pool = pool_client
        .get(Filter::Pool(source.clone()), None)
        .await
        .unwrap()
        .into_inner();
    let footprint = source_pool[0].state().expect("source pool is online").used / VOLUMES as u64;
    // A pool on another node which has room for one replica, but not for two. One cluster of the
    // pool is used for its metadata.
    let cluster_size = 4 * 1024 * 1024;
    let target_size = (footprint * 3 / 2).div_ceil(cluster_size) * cluster_size + cluster_size;
    pool_client
        .create(
            &CreatePool {
                node: cluster.node(1),
                id: "target".into(),
                disks: vec![
                    format!("malloc:///disk1?size_mb={}", target_size / (1024 * 1024)).into(),
                ],
                labels: None,
                encryption: None,
                cluster_size: None,
                max_expansion: None,
            },
            None,
        )
        .await
        .unwrap();
    let target = pool_client
        .get(Filter::Pool("target".into()), None)
        .await
        .unwrap()
        .into_inner();
    let state = target[0].state().expect("target pool is online");
    let free = state.capacity - state.used;
    assert!(
        free > footprint && free < 2 * footprint,
        "the target pool must fit exactly one replica, free space: {free}"
    );

    let reply = pool_client
        .drain_dry_run(&drain_request(&source, false))
        .await
        .unwrap();
    assert_eq!(reply.num_replicas, VOLUMES as u64);
    assert_eq!(reply.num_volume_replica_immovable, VOLUMES as u64 - 1);
    let reasons = &reply.immovable_resources;
    assert_eq!(reasons.size.len(), VOLUMES - 1);
    assert!(reasons.size.iter().all(|volume| volumes.contains(volume)));
    assert!(reasons.nodes.is_empty());
    assert!(reasons.no_candidate_pool.is_empty());
    assert!(reasons.other.is_empty());
    assert_no_drain_state(&pool_client, &source).await;
}

/// Create a single replica volume which is part of the affinity group "ag1".
async fn create_ag_volume(volume_client: &impl VolumeOperations, volume: VolumeId) {
    volume_client
        .create(
            &CreateVolume {
                uuid: volume,
                size: VOLUME_SIZE,
                replicas: 1,
                thin: false,
                affinity_group: Some(AffinityGroup::new("ag1".to_string())),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
}

/// Drain dry run reports a volume whose only candidate node already holds a replica of another
/// single replica volume of the same affinity group.
#[tokio::test]
async fn pool_drain_dry_run_affinity_group() {
    let cluster = ClusterBuilder::builder()
        .with_rest(false)
        .with_agents(vec!["core"])
        .with_io_engines(2)
        // The only pool when the first volume is created, so it gets the replica.
        .with_pool(0, "malloc:///disk0?size_mb=100")
        .with_cache_period("100ms")
        .with_node_deadline("100ms")
        .with_reconcile_period(Duration::from_millis(100), Duration::from_millis(100))
        .build()
        .await
        .unwrap();

    let pool_client = cluster.grpc_client().pool();
    let volume_client = cluster.grpc_client().volume();
    let replica_client = cluster.grpc_client().replica();

    let source = pool_client
        .get(Filter::None, None)
        .await
        .unwrap()
        .into_inner()
        .first()
        .unwrap()
        .id()
        .clone();

    let volume = VolumeId::new();
    create_ag_volume(&volume_client, volume.clone()).await;

    // The second volume of the group can only go to the new pool, on the other node.
    pool_client
        .create(
            &CreatePool {
                node: cluster.node(1),
                id: "pool1".into(),
                disks: vec!["malloc:///disk1?size_mb=200".into()],
                labels: None,
                encryption: None,
                cluster_size: None,
                max_expansion: None,
            },
            None,
        )
        .await
        .unwrap();
    let other_volume = VolumeId::new();
    create_ag_volume(&volume_client, other_volume.clone()).await;
    let replicas = replica_client
        .get(Filter::Volume(other_volume.clone()), None)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(replicas[0].node, cluster.node(1));

    // The only other pool is on a node which holds the replica of the other volume of the group.
    let reply = pool_client
        .drain_dry_run(&drain_request(&source, false))
        .await
        .unwrap();
    assert_eq!(reply.num_replicas, 1);
    assert_eq!(reply.num_volume_replica_immovable, 1);
    let reasons = &reply.immovable_resources;
    assert_eq!(reasons.affinity_group, vec![volume.clone()]);
    assert!(reasons.nodes.is_empty());
    assert!(reasons.size.is_empty());
    assert!(reasons.topology.is_empty());
    assert!(reasons.cluster_size.is_empty());
    assert!(reasons.single_replica_eviction.is_empty());
    assert!(reasons.encryption.is_empty());
    assert!(reasons.no_candidate_pool.is_empty());
    assert!(reasons.other.is_empty());
    assert_no_drain_state(&pool_client, &source).await;
}
