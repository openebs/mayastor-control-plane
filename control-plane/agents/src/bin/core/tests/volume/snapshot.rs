use crate::volume::helpers::wait_node_online;
use deployer_cluster::{Cluster, ClusterBuilder};
use grpc::operations::{
    pool::traits::PoolOperations,
    replica::traits::ReplicaOperations,
    volume::traits::{CreateVolumeSnapshot, DestroyVolumeSnapshot, VolumeOperations},
};
use std::time::Duration;
use stor_port::{
    transport_api::{ReplyErrorKind, TimeoutOptions},
    types::v0::transport::{
        CreateReplica, CreateVolume, DestroyPool, DestroyReplica, DestroyVolume, Filter,
        PublishVolume, ReplicaId, SnapshotId, Volume,
    },
};

#[tokio::test]
async fn snapshot() {
    let cluster = ClusterBuilder::builder()
        .with_rest(true)
        //   .with_agents(vec!["core"])
        .with_io_engines(1)
        .with_pools(1)
        .with_cache_period("1s")
        .with_reconcile_period(Duration::from_secs(1), Duration::from_secs(1))
        .build()
        .await
        .unwrap();

    let vol_cli = cluster.grpc_client().volume();

    let volume = vol_cli
        .create(
            &CreateVolume {
                uuid: "1e3cf927-80c2-47a8-adf0-95c486bdd7b7".try_into().unwrap(),
                size: 60 * 1024 * 1024,
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

    let volumes = vol_cli
        .get(Filter::Volume(volume.uuid().clone()), false, None, None)
        .await
        .unwrap();
    let volume = &volumes.entries[0];

    assert!(!volume.spec().thin);
    assert!(volume.spec().as_thin(), "Volume should be thin!");

    // Get snapshot by source id and snapshot id.
    let snaps = vol_cli
        .get_snapshots(
            Filter::VolumeSnapshot(
                volume.uuid().clone(),
                replica_snapshot.spec().snap_id.clone(),
            ),
            false,
            None,
            None,
        )
        .await
        .expect("Error while listing snapshot...");

    tracing::info!("List Snapshot by sourceid and snapid: {snaps:?}");

    // Get snapshot by snapshot id.
    let snaps = vol_cli
        .get_snapshots(
            Filter::Snapshot(replica_snapshot.spec().snap_id.clone()),
            false,
            None,
            None,
        )
        .await
        .expect("Error while listing snapshot...");

    tracing::info!("List Snapshot by snapid: {snaps:?}");
    assert!(!snaps.entries().is_empty());

    vol_cli
        .destroy_snapshot(&DestroyVolumeSnapshot::from(&replica_snapshot), None)
        .await
        .unwrap();

    tracing::info!("Deleted Snapshot: {}", replica_snapshot.spec().snap_id);

    assert!(!volume.spec().thin);
    assert!(volume.spec().as_thin(), "Volume should still be thin!");

    let volumes = vol_cli
        .get(Filter::Volume(volume.uuid().clone()), false, None, None)
        .await
        .unwrap();
    let volume = &volumes.entries[0];

    // Get snapshot by snapshot id. Shouldn't be found.
    vol_cli
        .get_snapshots(
            Filter::Snapshot(replica_snapshot.spec().snap_id.clone()),
            false,
            None,
            None,
        )
        .await
        .expect_err("Snapshot not found - expected");

    let volume = vol_cli
        .publish(
            &PublishVolume {
                uuid: volume.uuid().clone(),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    let nexus_snapshot = vol_cli
        .create_snapshot(
            &CreateVolumeSnapshot::new(volume.uuid(), SnapshotId::new()),
            None,
        )
        .await
        .unwrap();

    tracing::info!("Nexus Snapshot: {nexus_snapshot:?}");
    vol_cli
        .destroy_snapshot(&DestroyVolumeSnapshot::from(&nexus_snapshot), None)
        .await
        .unwrap();

    pool_destroy_validation(&cluster).await;
    thin_provisioning(&cluster, volume).await;
}

async fn thin_provisioning(cluster: &Cluster, volume: Volume) {
    let vol_cli = cluster.grpc_client().volume();
    let pool_cli = cluster.grpc_client().pool();

    let pools = pool_cli
        .get(Filter::Pool(cluster.pool(0, 0)), None)
        .await
        .unwrap();
    let pool = pools.0.get(0).unwrap();
    let pool_state = pool.state().unwrap();
    let watermark = 16 * 1024 * 1024;
    let free_space = pool_state.capacity - pool_state.used - watermark;

    // Take up the entire pool free space
    let repl_cli = cluster.grpc_client().replica();
    let mut req = CreateReplica {
        node: cluster.node(0),
        uuid: ReplicaId::new(),
        pool_id: cluster.pool(0, 0),
        size: free_space,
        thin: false,
        ..Default::default()
    };
    let replica = repl_cli.create(&req, None).await.unwrap();

    // Try to take a snapshot now, should fail!
    let error = vol_cli
        .create_snapshot(
            &CreateVolumeSnapshot::new(volume.uuid(), SnapshotId::new()),
            None,
        )
        .await
        .expect_err("No free space");
    assert_eq!(error.kind, ReplyErrorKind::ResourceExhausted);

    repl_cli
        .destroy(&DestroyReplica::from(replica), None)
        .await
        .unwrap();

    req.size = free_space / 2;
    let replica = repl_cli.create(&req, None).await.unwrap();

    let snapshot = vol_cli
        .create_snapshot(
            &CreateVolumeSnapshot::new(volume.uuid(), SnapshotId::new()),
            None,
        )
        .await
        .unwrap();

    repl_cli
        .destroy(&DestroyReplica::from(replica), None)
        .await
        .unwrap();
    vol_cli
        .destroy_snapshot(&DestroyVolumeSnapshot::from(&snapshot), None)
        .await
        .unwrap();

    // create 2 replicas of pool cap, exceeding pool commitment of 2.5x
    req.thin = true;
    req.size = pool_state.capacity - watermark;
    let _replica = repl_cli.create(&req, None).await.unwrap();

    // 2x the pool capacity is ok
    let _snapshot = vol_cli
        .create_snapshot(
            &CreateVolumeSnapshot::new(volume.uuid(), SnapshotId::new()),
            None,
        )
        .await
        .unwrap();

    req.uuid = ReplicaId::new();
    let _replica = repl_cli.create(&req, None).await.unwrap();

    let error = vol_cli
        .create_snapshot(
            &CreateVolumeSnapshot::new(volume.uuid(), SnapshotId::new()),
            None,
        )
        .await
        .expect_err("Pool over-committed");
    assert_eq!(error.kind, ReplyErrorKind::ResourceExhausted);
}

async fn pool_destroy_validation(cluster: &Cluster) {
    let vol_cli = cluster.grpc_client().volume();
    let pool_cli = cluster.grpc_client().pool();

    let volume = vol_cli
        .create(
            &CreateVolume {
                uuid: "0e3cf927-80c2-47a8-adf0-95c486bdd7b7".try_into().unwrap(),
                size: 5242880,
                replicas: 1,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    let replica_snapshot = vol_cli
        .create_snapshot(
            &CreateVolumeSnapshot::new(volume.uuid(), SnapshotId::new()),
            None,
        )
        .await
        .unwrap();

    vol_cli
        .destroy(&DestroyVolume::new(volume.uuid()), None)
        .await
        .unwrap();

    let destroy_pool = DestroyPool::new(cluster.node(0), cluster.pool(0, 0));
    let del_error = pool_cli
        .destroy(&destroy_pool, None)
        .await
        .expect_err("Snapshot Exists");
    assert_eq!(del_error.kind, ReplyErrorKind::InUse);

    vol_cli
        .destroy_snapshot(&DestroyVolumeSnapshot::from(&replica_snapshot), None)
        .await
        .unwrap();
}

#[tokio::test]
async fn snapshot_timeout() {
    fn grpc_timeout_opts(timeout: Duration) -> TimeoutOptions {
        TimeoutOptions::default()
            .with_max_retries(0)
            .with_req_timeout(timeout)
            .with_min_req_timeout(None)
    }
    let req_timeout = Duration::from_millis(800);
    let grpc_timeout = Duration::from_millis(250);

    let cluster = ClusterBuilder::builder()
        .with_rest(true)
        //  .with_agents(vec!["core"])
        .with_io_engines(2)
        .with_tmpfs_pool_ix(1, 100 * 1024 * 1024)
        .with_cache_period("10s")
        .with_reconcile_period(Duration::from_secs(10), Duration::from_secs(10))
        .with_req_timeouts(req_timeout, req_timeout)
        .with_grpc_timeouts(grpc_timeout_opts(grpc_timeout))
        .build()
        .await
        .unwrap();

    let vol_cli = cluster.grpc_client().volume();
    let volume = vol_cli
        .create(
            &CreateVolume {
                uuid: "1e3cf927-80c2-47a8-adf0-95c486bdd7b7".try_into().unwrap(),
                size: 5242880,
                replicas: 1,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    let snapshot_id = SnapshotId::new();
    cluster.composer().pause(&cluster.node(1)).await.unwrap();
    let replica_snapshot = vol_cli
        .create_snapshot(
            &CreateVolumeSnapshot::new(volume.uuid(), snapshot_id.clone()),
            None,
        )
        .await
        .expect_err("timeout");
    tracing::info!("Replica Snapshot Error: {replica_snapshot:?}");
    cluster.composer().pause("core").await.unwrap();
    cluster.composer().thaw(&cluster.node(1)).await.unwrap();
    tokio::time::sleep(req_timeout - grpc_timeout).await;
    cluster.restart_core_with_liveness(None).await.unwrap();

    let snapshots = vol_cli
        .get_snapshots(Filter::Snapshot(snapshot_id.clone()), false, None, None)
        .await
        .unwrap();
    let snapshot = &snapshots.entries[0];

    assert_eq!(snapshot.meta().txn_id().as_str(), "1");
    assert_eq!(snapshot.meta().transactions().len(), 1);
    let tx = snapshot.meta().transactions().get("1").unwrap();
    assert_eq!(tx[0].status().to_string().as_str(), "Deleting");

    let grpc = cluster.grpc_client();
    wait_node_online(&grpc.node(), cluster.node(1))
        .await
        .unwrap();

    let snapshot = vol_cli
        .create_snapshot(&CreateVolumeSnapshot::new(volume.uuid(), snapshot_id), None)
        .await
        .unwrap();
    tracing::info!("Volume Snapshot: {snapshot:?}");

    assert_eq!(snapshot.meta().txn_id().as_str(), "2");
    assert_eq!(snapshot.meta().transactions().len(), 2);
    let tx = snapshot.meta().transactions().get("1").unwrap();
    assert_eq!(tx[0].status().to_string().as_str(), "Deleting");
    let tx = snapshot.meta().transactions().get("2").unwrap();
    assert_eq!(tx[0].status().to_string().as_str(), "Created");

    let grpc = cluster
        .new_grpc_client(grpc_timeout_opts(req_timeout))
        .await;
    let vol_cli_req = grpc.volume();
    let volume = vol_cli_req
        .publish(
            &PublishVolume {
                uuid: volume.uuid().clone(),
                target_node: Some(cluster.node(0)),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    let snapshot_id = SnapshotId::new();
    cluster.composer().pause(&cluster.node(0)).await.unwrap();
    let replica_snapshot = vol_cli
        .create_snapshot(
            &CreateVolumeSnapshot::new(volume.uuid(), snapshot_id.clone()),
            None,
        )
        .await
        .expect_err("timeout");
    tracing::info!("Replica Snapshot Error: {replica_snapshot:?}");
    tokio::time::sleep(req_timeout - grpc_timeout).await;

    cluster.composer().thaw(&cluster.node(0)).await.unwrap();
    wait_node_online(&grpc.node(), cluster.node(0))
        .await
        .unwrap();

    let snapshots = vol_cli
        .get_snapshots(Filter::Snapshot(snapshot_id.clone()), false, None, None)
        .await
        .unwrap();
    let snapshot = &snapshots.entries[0];
    tracing::info!("Snapshot: {snapshot:?}");

    assert_eq!(snapshot.meta().txn_id().as_str(), "1");
    assert!(snapshot.meta().transactions().is_empty());

    let snapshot = vol_cli
        .create_snapshot(&CreateVolumeSnapshot::new(volume.uuid(), snapshot_id), None)
        .await
        .unwrap();
    tracing::info!("Snapshot: {snapshot:?}");

    assert_eq!(snapshot.meta().txn_id().as_str(), "2");
    assert_eq!(snapshot.meta().transactions().len(), 1);
    let tx = snapshot.meta().transactions().get("2").unwrap();
    assert_eq!(tx[0].status().to_string().as_str(), "Created");
}

#[tokio::test]
async fn unknown_snapshot_garbage_collector() {
    let cluster = ClusterBuilder::builder()
        .with_rest(false)
        //  .with_agents(vec!["core"])
        .with_io_engines(1)
        .with_pools(1)
        .with_cache_period("50ms")
        .with_reconcile_period(Duration::from_millis(50), Duration::from_millis(50))
        .build()
        .await
        .unwrap();

    let vol_cli = cluster.grpc_client().volume();
    let volume = vol_cli
        .create(
            &CreateVolume {
                uuid: "1e3cf927-80c2-47a8-adf0-95c486bdd7b7".try_into().unwrap(),
                size: 60 * 1024 * 1024,
                replicas: 1,
                thin: false,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    // Create two snapshot id's that we would use to create leaked resources.
    let parent_repl_snapid = SnapshotId::new();
    let parent_nexus_snapid = SnapshotId::new();

    // Create one replica snapshot.
    vol_cli
        .create_snapshot(
            &CreateVolumeSnapshot::new(volume.uuid(), parent_repl_snapid.clone()),
            None,
        )
        .await
        .unwrap();

    // Publish the volume to take nexus snapshot.
    let volume = vol_cli
        .publish(
            &PublishVolume {
                uuid: volume.uuid().clone(),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    // Create a nexus snapshot.
    vol_cli
        .create_snapshot(
            &CreateVolumeSnapshot::new(volume.uuid(), parent_nexus_snapid.clone()),
            None,
        )
        .await
        .unwrap();

    // Stop the core agent so that we can create the leaked resources.
    cluster.composer().stop("core").await.unwrap();

    // Extract the nexus and replica to be used to create leaked resources.
    let volume_state = volume.state();
    let replica = volume_state.replica_topology.iter().next().unwrap();
    let nexus = volume_state.target.unwrap();

    // Create the rpc handle.
    let mut rpc_handle = cluster
        .grpc_handle(&cluster.node(0))
        .await
        .expect("Should get handle");
    // At this point no leaked resources create so, snapshot count is 2.
    let snaps = rpc_handle
        .list_replica_snaps(None, None)
        .await
        .expect("Should get replica snaps");
    assert_eq!(snaps.snapshots.len(), 2);

    // Create the first leaked replica snapshot, with the existing replica snapshot's id.
    rpc_handle
        .create_replica_snap(
            &format!("{parent_repl_snapid}/322"),
            volume.spec().uuid.as_str(),
            "322",
            replica.0.as_str(),
            SnapshotId::new().as_str(),
        )
        .await
        .unwrap();
    let snaps = rpc_handle
        .list_replica_snaps(None, None)
        .await
        .expect("Should get replica snaps");
    assert_eq!(snaps.snapshots.len(), 3);

    // Create the first leaked replica snapshot, with the existing nexus snapshot's id.
    rpc_handle
        .create_nexus_snap(
            nexus.uuid.as_str(),
            &format!("{parent_nexus_snapid}/766"),
            volume.spec().uuid.as_str(),
            "766",
            replica.0.as_str(),
            SnapshotId::new().as_str(),
        )
        .await
        .unwrap();
    let snaps = rpc_handle
        .list_replica_snaps(None, None)
        .await
        .expect("Should get replica snaps");
    assert_eq!(snaps.snapshots.len(), 4);

    // Create the third leaked replica snapshot, with the snapshot id that no longer exists.
    rpc_handle
        .create_nexus_snap(
            nexus.uuid.as_str(),
            &format!("{}/54", SnapshotId::new()),
            volume.spec().uuid.as_str(),
            "54",
            replica.0.as_str(),
            SnapshotId::new().as_str(),
        )
        .await
        .unwrap();
    let snaps = rpc_handle
        .list_replica_snaps(None, None)
        .await
        .expect("Should get replica snaps");
    assert_eq!(snaps.snapshots.len(), 5);

    // Create the third leaked replica snapshot, with invalid name. This would not be garbage
    // collected.
    rpc_handle
        .create_nexus_snap(
            nexus.uuid.as_str(),
            "snap_54",
            volume.spec().uuid.as_str(),
            "54",
            replica.0.as_str(),
            SnapshotId::new().as_str(),
        )
        .await
        .unwrap();
    let snaps = rpc_handle
        .list_replica_snaps(None, None)
        .await
        .expect("Should get replica snaps");
    assert_eq!(snaps.snapshots.len(), 6);

    cluster
        .restart_core_with_liveness(None)
        .await
        .expect("Core agent should have been up");

    let snaps = rpc_handle
        .list_replica_snaps(None, None)
        .await
        .expect("Should get replica snaps");
    assert_eq!(snaps.snapshots.len(), 3);
}
