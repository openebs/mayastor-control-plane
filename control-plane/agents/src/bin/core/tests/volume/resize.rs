use deployer_cluster::{Cluster, ClusterBuilder};
use grpc::operations::{
    nexus::traits::NexusOperations, replica::traits::ReplicaOperations,
    volume::traits::VolumeOperations,
};
use std::time::Duration;
use stor_port::types::v0::transport::{
    CreateVolume, DestroyVolume, Filter, PublishVolume, ResizeVolume, Volume, VolumeId,
    VolumeShareProtocol,
};

/// Validate that the size of volume and replicas are as per expected_size
/// Return true if validation is successful, otherwise false.
async fn validate_resized_volume(
    cluster: &Cluster,
    uuid: &VolumeId,
    resized_volume: &Volume,
    expected_size: u64,
    published: bool,
) {
    let nex_cli = cluster.grpc_client().nexus();
    let repl_cli = cluster.grpc_client().replica();
    assert!(resized_volume.spec().uuid == *uuid);
    assert!(resized_volume.spec().size == expected_size);

    let replicas = repl_cli
        .get(Filter::Volume(uuid.clone()), None)
        .await
        .unwrap();
    // Compare >= since replicas have some additional book-keeping space.
    replicas
        .into_inner()
        .iter()
        .for_each(|r| assert!(r.size >= resized_volume.spec().size));

    if published {
        let nexus = nex_cli
            .get(Filter::Volume(uuid.clone()), None)
            .await
            .unwrap()
            .0;
        assert!(nexus.len() == 1);
        assert!(nexus[0].size == expected_size);
        tracing::info!("Validated Resized Nexus: {:?}", nexus[0]);
    }
}

#[tokio::test]
async fn resize_unpublished() {
    let cluster = ClusterBuilder::builder()
        .with_rest(true)
        .with_agents(vec!["core"])
        .with_io_engines(3)
        .with_pool(0, "malloc:///p1?size_mb=200")
        .with_pool(1, "malloc:///p1?size_mb=200")
        .with_pool(2, "malloc:///p1?size_mb=200")
        .with_cache_period("1s")
        .with_reconcile_period(Duration::from_secs(1), Duration::from_secs(1))
        .build()
        .await
        .unwrap();

    let vol_cli = cluster.grpc_client().volume();

    // Create the volume.
    let volume_size_orig = 50 * 1024 * 1024;
    let volume = vol_cli
        .create(
            &CreateVolume {
                uuid: "de3cf927-80c2-47a8-adf0-95c486bdd7b7".try_into().unwrap(),
                size: volume_size_orig,
                replicas: 3,
                thin: false,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    // Unpublished volume
    assert!(volume.spec().active_config().is_none() && volume.spec().num_replicas == 3);

    let resized_volume = vol_cli
        .resize(
            &ResizeVolume {
                uuid: volume.uuid().clone(),
                requested_size: 2 * volume.spec().size,
                capacity_limit: None,
            },
            None,
        )
        .await
        .unwrap();

    tracing::info!("Resized Unpublished {resized_volume:?}");
    validate_resized_volume(
        &cluster,
        &volume.spec().uuid,
        &resized_volume,
        2 * volume_size_orig,
        false,
    )
    .await;

    let _ = vol_cli
        .destroy(
            &DestroyVolume {
                uuid: volume.uuid().clone(),
            },
            None,
        )
        .await;

    // Test resizing a published volume.
    resize_published(&cluster).await;
}

// Resizing a published volume should throw error that volume is in-use.
async fn resize_published(cluster: &Cluster) {
    let vol_cli = cluster.grpc_client().volume();
    // Create and publish the volume.
    let volume_size_orig = 50 * 1024 * 1024;
    let volume = vol_cli
        .create(
            &CreateVolume {
                uuid: "df3cf927-80c2-47a8-adf0-95c486bdd7b7".try_into().unwrap(),
                size: 50 * 1024 * 1024,
                replicas: 3,
                thin: false,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    vol_cli
        .publish(
            &PublishVolume {
                uuid: volume.spec().uuid,
                target_node: Some(cluster.node(0)),
                share: Some(VolumeShareProtocol::Nvmf),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    let resized_volume = vol_cli
        .resize(
            &ResizeVolume {
                uuid: volume.uuid().clone(),
                requested_size: 2 * volume.spec().size,
                capacity_limit: None,
            },
            None,
        )
        .await
        .unwrap();

    tracing::info!("Resized Published {resized_volume:?}");
    validate_resized_volume(
        cluster,
        &volume.spec().uuid,
        &resized_volume,
        2 * volume_size_orig,
        true,
    )
    .await;
}

// Try to resize a volume. When any one of the replica can't be resized due to
// insufficient capacity on  pool, the volume resize should fail and volume size
// should remain unchanged.
#[tokio::test]
async fn resize_on_no_capacity_pool() {
    let cluster = ClusterBuilder::builder()
        .with_rest(true)
        .with_agents(vec!["core"])
        .with_io_engines(3)
        .with_pool(0, "malloc:///p1?size_mb=200")
        .with_pool(1, "malloc:///p1?size_mb=200")
        .with_pool(2, "malloc:///p1?size_mb=100")
        .with_cache_period("1s")
        .with_reconcile_period(Duration::from_secs(1), Duration::from_secs(1))
        .build()
        .await
        .unwrap();

    let vol_cli = cluster.grpc_client().volume();

    let volume = vol_cli
        .create(
            &CreateVolume {
                uuid: "de3cf927-80c2-47a8-adf0-95c486bdd7b7".try_into().unwrap(),
                size: 50 * 1024 * 1024,
                replicas: 3,
                thin: false,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    let resized_volume = vol_cli
        .resize(
            &ResizeVolume {
                uuid: volume.uuid().clone(),
                requested_size: 2 * volume.spec().size,
                capacity_limit: None,
            },
            None,
        )
        .await
        .expect_err("Expected error due to insufficient pool capacity");

    tracing::info!("Volume resize error: {resized_volume:?}");
    let v_arr = vol_cli
        .get(Filter::Volume(volume.spec().uuid), false, None, None)
        .await
        .unwrap();
    let vol_obj = &v_arr.entries[0];
    // Size shouldn't have changed.
    assert!(vol_obj.spec().size == volume.spec().size);
    // TODO: Add reclaim monitor validations for replicas that got resized as part
    // of this failed volume resize.
}
