use deployer_cluster::{Cluster, ClusterBuilder};
use grpc::operations::{
    pool::traits::PoolOperations,
    volume::traits::{CreateVolumeSnapshot, DeleteVolumeSnapshot, VolumeOperations},
};
use stor_port::types::v0::transport::{
    CreateVolume, DestroyPool, DestroyVolume, Filter, PublishVolume, SnapshotId,
};

use std::time::Duration;
use stor_port::transport_api::ReplyErrorKind;

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
    let snaps = vol_cli
        .get_snapshots(
            Filter::Snapshot(replica_snapshot.spec().snap_id.clone()),
            false,
            None,
        )
        .await
        .expect("Error while listing snapshot...");

    assert!(snaps.entries().is_empty());

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
        .expect_err("unimplemented");

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
        .expect_err("Delete is expected to fail");
}
