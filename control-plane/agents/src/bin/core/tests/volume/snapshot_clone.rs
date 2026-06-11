use crate::pool::validate_vol_repl_cluster_size;
use deployer_cluster::ClusterBuilder;
use grpc::operations::{
    pool::traits::PoolOperations,
    volume::traits::{CreateVolumeSnapshot, DestroyVolumeSnapshot, VolumeOperations},
};
use std::time::Duration;
use stor_port::{
    transport_api::ReplyErrorKind,
    types::v0::{
        store::pool::POOL_BS_CLUSTER_SIZE_DEFAULT,
        transport::{
            CreatePool, CreateSnapshotVolume, CreateVolume, Filter, SnapshotId,
            SnapshotRestorePolicy,
        },
    },
};

#[tokio::test]
async fn snapshot_clone() {
    let cluster = ClusterBuilder::builder()
        .with_rest(true)
        .with_io_engines(1)
        .with_pools(1)
        .with_cache_period("1s")
        .with_reconcile_period(Duration::from_secs(1), Duration::from_secs(1))
        .build()
        .await
        .unwrap();

    let vol_cli = cluster.grpc_client().volume();
    let pool_cli = cluster.grpc_client().pool();

    let volume = vol_cli
        .create(
            &CreateVolume {
                uuid: "1e3cf927-80c2-47a8-adf0-95c486bdd7b7".try_into().unwrap(),
                size: 20 * 1024 * 1024,
                replicas: 1,
                thin: false,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    assert!(!volume.spec().thin);
    assert!(!volume.spec().as_thin(), "Volume should not be thin!");

    let replica_snapshot = vol_cli
        .create_snapshot(
            &CreateVolumeSnapshot::new(volume.uuid(), SnapshotId::new()),
            None,
        )
        .await
        .unwrap();

    tracing::info!("Replica Snapshot: {replica_snapshot:?}");

    let pools = pool_cli
        .get(Filter::Pool(cluster.pool(0, 0)), None)
        .await
        .unwrap()
        .into_inner();
    let pool = pools.first().unwrap();
    assert_eq!(pool.state.as_ref().unwrap().repl_count, Some(1));
    assert_eq!(pool.state.as_ref().unwrap().snap_count, Some(1));
    assert_eq!(
        pool.config.as_ref().unwrap().definition.replica_count,
        Some(1)
    );
    assert_eq!(
        pool.config.as_ref().unwrap().definition.snapshot_count,
        Some(1)
    );

    let error = vol_cli
        .create_snapshot_volume(
            &CreateSnapshotVolume::new(
                replica_snapshot.spec().snap_id().clone(),
                CreateVolume {
                    uuid: "1e3cf927-80c2-47a8-adf0-95c486bdd7b8".try_into().unwrap(),
                    size: 60 * 1024 * 1024,
                    replicas: 1,
                    thin: false,
                    ..Default::default()
                },
            ),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind, ReplyErrorKind::InvalidArgument);

    let clone_1 = vol_cli
        .create_snapshot_volume(
            &CreateSnapshotVolume::new(
                replica_snapshot.spec().snap_id().clone(),
                CreateVolume {
                    uuid: "1e3cf927-80c2-47a8-adf0-95c486bdd7b8".try_into().unwrap(),
                    size: 20 * 1024 * 1024,
                    replicas: 1,
                    thin: true,
                    ..Default::default()
                },
            ),
            None,
        )
        .await
        .unwrap();
    let pools = pool_cli
        .get(Filter::Pool(cluster.pool(0, 0)), None)
        .await
        .unwrap()
        .into_inner();
    let pool = pools.first().unwrap();
    assert_eq!(pool.state.as_ref().unwrap().repl_count, Some(2));
    assert_eq!(pool.state.as_ref().unwrap().snap_count, Some(1));
    assert_eq!(
        pool.config.as_ref().unwrap().definition.replica_count,
        Some(2)
    );
    assert_eq!(
        pool.config.as_ref().unwrap().definition.snapshot_count,
        Some(1)
    );
    let clone_2 = vol_cli
        .create_snapshot_volume(
            &CreateSnapshotVolume::new(
                replica_snapshot.spec().snap_id().clone(),
                CreateVolume {
                    uuid: "1e3cf927-80c2-47a8-adf0-95c486bdd7b9".try_into().unwrap(),
                    size: 20 * 1024 * 1024,
                    replicas: 1,
                    thin: true,
                    ..Default::default()
                },
            ),
            None,
        )
        .await
        .unwrap();
    let pools = pool_cli
        .get(Filter::Pool(cluster.pool(0, 0)), None)
        .await
        .unwrap()
        .into_inner();
    let pool = pools.first().unwrap();
    assert_eq!(pool.state.as_ref().unwrap().repl_count, Some(3));
    assert_eq!(pool.state.as_ref().unwrap().snap_count, Some(1));
    assert_eq!(
        pool.config.as_ref().unwrap().definition.replica_count,
        Some(3)
    );
    assert_eq!(
        pool.config.as_ref().unwrap().definition.snapshot_count,
        Some(1)
    );

    vol_cli.destroy(&clone_1, None).await.unwrap();
    let pools = pool_cli
        .get(Filter::Pool(cluster.pool(0, 0)), None)
        .await
        .unwrap()
        .into_inner();
    let pool = pools.first().unwrap();
    assert_eq!(pool.state.as_ref().unwrap().repl_count, Some(2));
    assert_eq!(pool.state.as_ref().unwrap().snap_count, Some(1));
    assert_eq!(
        pool.config.as_ref().unwrap().definition.replica_count,
        Some(2)
    );
    assert_eq!(
        pool.config.as_ref().unwrap().definition.snapshot_count,
        Some(1)
    );

    vol_cli.destroy(&clone_2, None).await.unwrap();
    let pools = pool_cli
        .get(Filter::Pool(cluster.pool(0, 0)), None)
        .await
        .unwrap()
        .into_inner();
    let pool = pools.first().unwrap();
    assert_eq!(pool.state.as_ref().unwrap().repl_count, Some(1));
    assert_eq!(pool.state.as_ref().unwrap().snap_count, Some(1));
    assert_eq!(
        pool.config.as_ref().unwrap().definition.replica_count,
        Some(1)
    );
    assert_eq!(
        pool.config.as_ref().unwrap().definition.snapshot_count,
        Some(1)
    );

    let volumes = vol_cli.get(Filter::None, false, None, None).await.unwrap();
    assert_eq!(volumes.entries.len(), 1);

    vol_cli
        .create_snapshot_volume(
            &CreateSnapshotVolume::new(
                replica_snapshot.spec().snap_id().clone(),
                CreateVolume {
                    uuid: "1e3cf927-80c2-47a8-adf0-95c486bdd7b9".try_into().unwrap(),
                    size: 20 * 1024 * 1024,
                    replicas: 1,
                    thin: true,
                    ..Default::default()
                },
            ),
            None,
        )
        .await
        .unwrap();
    let pools = pool_cli
        .get(Filter::Pool(cluster.pool(0, 0)), None)
        .await
        .unwrap()
        .into_inner();
    let pool = pools.first().unwrap();
    assert_eq!(pool.state.as_ref().unwrap().repl_count, Some(2));
    assert_eq!(pool.state.as_ref().unwrap().snap_count, Some(1));
    assert_eq!(
        pool.config.as_ref().unwrap().definition.replica_count,
        Some(2)
    );
    assert_eq!(
        pool.config.as_ref().unwrap().definition.snapshot_count,
        Some(1)
    );
}

#[tokio::test]
async fn snapshot_clone_pool_cluster_size_constraint() {
    let cluster = ClusterBuilder::builder()
        .with_io_engines(3)
        .with_reconcile_period(Duration::from_secs(10), Duration::from_secs(10))
        .build()
        .await
        .unwrap();

    let client = cluster.grpc_client();

    let vol_cli = cluster.grpc_client().volume();

    let pool_4m_1 = CreatePool {
        node: cluster.node(0),
        id: "pool_cs_4m_1".into(),
        disks: vec!["malloc:///disk0?size_mb=200".into()],
        labels: None,
        encryption: None,
        cluster_size: None,
        max_expansion: None,
    };

    let pool_32m_1 = CreatePool {
        node: cluster.node(1),
        id: "pool_cs_32m_1".into(),
        disks: vec!["malloc:///disk1?size_mb=200".into()],
        labels: None,
        encryption: None,
        cluster_size: Some(33554432),
        max_expansion: None,
    };

    let pool_32m_2 = CreatePool {
        node: cluster.node(2),
        id: "pool_cs_32m_2".into(),
        disks: vec!["malloc:///disk2?size_mb=200".into()],
        labels: None,
        encryption: None,
        cluster_size: Some(33554432),
        max_expansion: None,
    };

    let _ = client.pool().create(&pool_4m_1, None).await.unwrap();
    let _ = client.pool().create(&pool_32m_1, None).await.unwrap();
    let _ = client.pool().create(&pool_32m_2, None).await.unwrap();

    // Craete a 1-repl volume. Should succeed on default cluster size pool.
    let volume_1 = vol_cli
        .create(
            &CreateVolume {
                uuid: "1e3cf927-80c2-47a8-adf0-95c486bdd7b7".try_into().unwrap(),
                size: 20 * 1024 * 1024,
                replicas: 1,
                thin: false,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    let vol_snapshot_1 = vol_cli
        .create_snapshot(
            &CreateVolumeSnapshot::new(volume_1.uuid(), SnapshotId::new()),
            None,
        )
        .await
        .unwrap();

    let clone_1 = vol_cli
        .create_snapshot_volume(
            &CreateSnapshotVolume::new(
                vol_snapshot_1.spec().snap_id().clone(),
                CreateVolume {
                    uuid: "2e3cf927-80c2-47a8-adf0-95c486bdd7b8".try_into().unwrap(),
                    size: 20 * 1024 * 1024,
                    replicas: 1,
                    thin: true,
                    ..Default::default()
                },
            ),
            None,
        )
        .await
        .unwrap();

    assert_eq!(volume_1.spec().cluster_size, POOL_BS_CLUSTER_SIZE_DEFAULT);
    assert_eq!(clone_1.spec().cluster_size, volume_1.spec().cluster_size);
    validate_vol_repl_cluster_size(&cluster, &volume_1, POOL_BS_CLUSTER_SIZE_DEFAULT.into())
        .await
        .unwrap();
    validate_vol_repl_cluster_size(&cluster, &clone_1, POOL_BS_CLUSTER_SIZE_DEFAULT.into())
        .await
        .unwrap();

    // Now create a 2-repl volume with 32MiB requested pool cluster size, take a snapshot
    // and restore into a 2-repl clone. Clone volume also must have replicas on 32MiB cluster
    // size pools.
    let volume_2 = vol_cli
        .create(
            &CreateVolume {
                uuid: "3e3cf927-80c2-47a8-adf0-95c486bdd7b7".try_into().unwrap(),
                size: 20 * 1024 * 1024,
                replicas: 2,
                thin: false,
                cluster_size: Some(33554432),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    let vol_snapshot_2 = vol_cli
        .create_snapshot(
            &CreateVolumeSnapshot::new(volume_2.uuid(), SnapshotId::new()),
            None,
        )
        .await
        .unwrap();

    let clone_2 = vol_cli
        .create_snapshot_volume(
            &CreateSnapshotVolume::new(
                vol_snapshot_2.spec().snap_id().clone(),
                CreateVolume {
                    uuid: "4e3cf927-80c2-47a8-adf0-95c486bdd7b8".try_into().unwrap(),
                    size: 20 * 1024 * 1024,
                    replicas: 2,
                    thin: true,
                    ..Default::default()
                },
            ),
            None,
        )
        .await
        .unwrap();

    assert_eq!(volume_2.spec().cluster_size, 33554432);
    assert_eq!(clone_2.spec().cluster_size, volume_2.spec().cluster_size);
    validate_vol_repl_cluster_size(&cluster, &volume_2, 33554432)
        .await
        .unwrap();
    validate_vol_repl_cluster_size(&cluster, &clone_2, 33554432)
        .await
        .unwrap();

    // And scaling the volume should fail, even though there is a third pool available,
    // because that pool won't fit cluster size constraints for the volume.
    let rest_client = cluster.rest_v00();
    rest_client
        .volumes_api()
        .put_volume_replica_count(clone_2.uuid(), 3)
        .await
        .expect_err("volume replica count scale-up must fail");

    vol_cli.destroy(&clone_1, None).await.unwrap();
    vol_cli.destroy(&clone_2, None).await.unwrap();
    vol_cli
        .destroy_snapshot(&DestroyVolumeSnapshot::from(&vol_snapshot_1), None)
        .await
        .unwrap();
    vol_cli
        .destroy_snapshot(&DestroyVolumeSnapshot::from(&vol_snapshot_2), None)
        .await
        .unwrap();
    vol_cli.destroy(&volume_1, None).await.unwrap();
    vol_cli.destroy(&volume_2, None).await.unwrap();
}

/// Restoring a snapshot when only a subset of source pools can host a clone.
///
/// Setup: 2 io-engines, 2-replica volume, snapshot. Then pause one of the
/// io-engines so its pool is unreachable for new replica creation. With
/// only one usable source pool left:
///
/// - `Strict` (default) restore must fail.
/// - `BestEffort` restore must succeed with one clone replica. The remaining
///   replica is left for the regular replica reconciler to fill in.
#[tokio::test]
async fn snapshot_restore_best_effort() {
    let cluster = ClusterBuilder::builder()
        .with_rest(true)
        .with_io_engines(2)
        .with_pools(1)
        .with_cache_period("1s")
        .with_reconcile_period(Duration::from_secs(1), Duration::from_secs(1))
        .build()
        .await
        .unwrap();

    let vol_cli = cluster.grpc_client().volume();

    let source = vol_cli
        .create(
            &CreateVolume {
                uuid: "1e3cf927-80c2-47a8-adf0-95c486bdd8a0".try_into().unwrap(),
                size: 20 * 1024 * 1024,
                replicas: 2,
                thin: true,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    let snapshot = vol_cli
        .create_snapshot(
            &CreateVolumeSnapshot::new(source.uuid(), SnapshotId::new()),
            None,
        )
        .await
        .unwrap();

    // Make one of the two source pools unreachable.
    cluster.composer().pause(&cluster.node(1)).await.unwrap();

    // Strict: 2 replicas requested, only one source pool reachable, must fail.
    // The upfront `setup` gate accepts both source pools (the scheduler does
    // not drop the paused-node pool at candidate-listing time), so the failure
    // surfaces from the per-replica create on the unreachable node and the
    // post-create check against the requested replica count.
    let strict_err = vol_cli
        .create_snapshot_volume(
            &CreateSnapshotVolume::new(
                snapshot.spec().snap_id().clone(),
                CreateVolume {
                    uuid: "1e3cf927-80c2-47a8-adf0-95c486bdd8a1".try_into().unwrap(),
                    size: 20 * 1024 * 1024,
                    replicas: 2,
                    thin: true,
                    snapshot_restore_policy: SnapshotRestorePolicy::Strict,
                    ..Default::default()
                },
            ),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(strict_err.kind, ReplyErrorKind::ReplicaCreateNumber);

    // BestEffort: same request must succeed with one clone replica.
    let restored = vol_cli
        .create_snapshot_volume(
            &CreateSnapshotVolume::new(
                snapshot.spec().snap_id().clone(),
                CreateVolume {
                    uuid: "1e3cf927-80c2-47a8-adf0-95c486bdd8a2".try_into().unwrap(),
                    size: 20 * 1024 * 1024,
                    replicas: 2,
                    thin: true,
                    snapshot_restore_policy: SnapshotRestorePolicy::BestEffort,
                    ..Default::default()
                },
            ),
            None,
        )
        .await
        .unwrap();

    // The volume spec still asks for 2 replicas, but only one was provisioned
    // up-front. The replica reconciler is responsible for bringing the count
    // back up once the paused node returns or another pool becomes a candidate.
    assert_eq!(restored.spec().num_replicas, 2);
    let actual_replicas = restored.state().replica_topology.len();
    assert!(
        actual_replicas >= 1,
        "BestEffort restore should produce at least one replica, got {actual_replicas}",
    );

    cluster.composer().thaw(&cluster.node(1)).await.unwrap();
}
