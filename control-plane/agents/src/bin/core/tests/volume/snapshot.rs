use crate::volume::helpers::wait_node_online;
use deployer_cluster::{Cluster, ClusterBuilder};
use grpc::operations::{
    pool::traits::PoolOperations,
    volume::traits::{CreateVolumeSnapshot, DeleteVolumeSnapshot, VolumeOperations},
};
use stor_port::{
    transport_api::{ReplyErrorKind, TimeoutOptions},
    types::v0::transport::{
        CreateVolume, DestroyPool, DestroyVolume, Filter, PublishVolume, SnapshotId,
    },
};

use std::time::Duration;

#[tokio::test]
async fn snapshot() {
    let cluster = ClusterBuilder::builder()
        .with_rest(true)
        .with_agents(vec!["core"])
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

    tracing::info!("Replica Snapshot: {replica_snapshot:?}");

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
        .delete_snapshot(
            &DeleteVolumeSnapshot::new(
                &Some(replica_snapshot.spec().source_id.clone()),
                replica_snapshot.spec().snap_id.clone(),
            ),
            None,
        )
        .await
        .unwrap();

    tracing::info!("Deleted Snapshot: {}", replica_snapshot.spec().snap_id);

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

    pool_destroy_validation(&cluster).await;
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
        .delete_snapshot(
            &DeleteVolumeSnapshot::new(
                &Some(replica_snapshot.spec().source_id.clone()),
                replica_snapshot.spec().snap_id.clone(),
            ),
            None,
        )
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
        .with_agents(vec!["core"])
        .with_io_engines(2)
        .with_tmpfs_pool_ix(1, 100 * 1024 * 1024)
        .with_cache_period("1s")
        .with_reconcile_period(Duration::from_secs(1), Duration::from_secs(1))
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
    cluster.composer().restart("core").await.unwrap();
    cluster.volume_service_liveness(None).await.unwrap();

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
    tracing::info!("Replica Snapshot: {replica_snapshot:?}");

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
    tokio::time::sleep(req_timeout).await;

    cluster.composer().thaw(&cluster.node(0)).await.unwrap();

    let snapshots = vol_cli
        .get_snapshots(Filter::Snapshot(snapshot_id.clone()), false, None, None)
        .await
        .unwrap();
    let snapshot = &snapshots.entries[0];

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
