use deployer_cluster::ClusterBuilder;
use grpc::operations::volume::traits::{CreateVolumeSnapshot, VolumeOperations};
use std::time::Duration;
use stor_port::{
    transport_api::ReplyErrorKind,
    types::v0::transport::{CreateSnapshotVolume, CreateVolume, Filter, SnapshotId},
};

#[tokio::test]
async fn snapshot_clone() {
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

    vol_cli.destroy(&clone_1, None).await.unwrap();
    vol_cli.destroy(&clone_2, None).await.unwrap();

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
}
