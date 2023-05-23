use deployer_cluster::ClusterBuilder;
use grpc::operations::volume::traits::{CreateVolumeSnapshot, VolumeOperations};
use stor_port::types::v0::transport::{CreateVolume, PublishVolume, SnapshotId};

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

    tracing::info!("Snapshot: {replica_snapshot:?}");

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

    tracing::info!("Snapshot: {nexus_snapshot:?}");
}
