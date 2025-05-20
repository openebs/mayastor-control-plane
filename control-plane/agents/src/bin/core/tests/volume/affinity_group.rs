use deployer_cluster::{Cluster, ClusterBuilder};
use grpc::operations::{registry::traits::RegistryOperations, volume::traits::VolumeOperations};
use stor_port::types::v0::transport::{
    AffinityGroup, CreateVolume, DestroyVolume, GetSpecs, SetVolumeReplica, VolumeId,
};
use tracing::info;

#[tokio::test]
async fn affinity_group() {
    let cluster = ClusterBuilder::builder()
        .with_rest(false)
        .with_agents(vec!["core"])
        .with_io_engines(3)
        .with_pools(3)
        .with_cache_period("1s")
        .build()
        .await
        .unwrap();

    startup_test(&cluster).await;
    scale_up_test(&cluster).await;
}

async fn startup_test(cluster: &Cluster) {
    let vols = vec![
        (Some("ag1"), "eba487d9-0b57-407b-8b48-0b631a372183"),
        (Some("ag1"), "359b7e1a-b724-443b-98b4-e6d97fabbb60"),
        (Some("ag2"), "f2296d6a-77a6-401d-aad3-ccdc247b0a56"),
        (None, "bdd3431c-0ccd-4a00-91cd-bb3d7cccb4b2"),
        (None, "52c8f1e9-8538-48ce-9906-adfe3623e032"),
    ];

    let volume_client = cluster.grpc_client().volume();

    // Create all the volumes.
    for &item in &vols {
        volume_client
            .create(
                &CreateVolume {
                    uuid: VolumeId::try_from(item.1).unwrap(),
                    size: 5242880,
                    replicas: 1,
                    affinity_group: item.0.map(|val| AffinityGroup::new(val.to_string())),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("Volume creation should succeed");
    }

    // Restart the core-agent.
    cluster.restart_core().await;

    // Wait for the core-agent grpc to come up.
    cluster
        .node_service_liveness(None)
        .await
        .expect("Should have restarted by now");

    let registry_client = cluster.grpc_client().registry();

    // The Affinity Group specs should now have been loaded in memory.
    // Fetch the specs.
    let specs = registry_client
        .get_specs(&GetSpecs {}, None)
        .await
        .expect("should be able to fetch specs");

    info!("Affinity Group Specs: {:?}", specs.affinity_groups);

    // Check for the validity of the Affinity Group specs.
    for vol_grp_spec in specs.affinity_groups {
        match vol_grp_spec.id().as_str() {
            "ag1" => {
                assert_eq!(vol_grp_spec.volumes().len(), 2);
                assert!(vol_grp_spec
                    .volumes()
                    .contains(&VolumeId::try_from(vols[0].1).unwrap()));
                assert!(vol_grp_spec
                    .volumes()
                    .contains(&VolumeId::try_from(vols[1].1).unwrap()));
            }
            "ag2" => {
                assert_eq!(vol_grp_spec.volumes().len(), 1);
                assert!(vol_grp_spec
                    .volumes()
                    .contains(&VolumeId::try_from(vols[2].1).unwrap()));
            }
            _ => {}
        }
    }

    // Create all the volumes.
    for &item in &vols {
        volume_client
            .destroy(
                &DestroyVolume {
                    uuid: VolumeId::try_from(item.1).unwrap(),
                },
                None,
            )
            .await
            .expect("Volume deletion should succeed");
    }
}

async fn scale_up_test(cluster: &Cluster) {
    let vols = vec![
        (Some("ag1"), "eba487d9-0b57-407b-8b48-0b631a372183"),
        (Some("ag1"), "359b7e1a-b724-443b-98b4-e6d97fabbb60"),
        (Some("ag1"), "f2296d6a-77a6-401d-aad3-ccdc247b0a56"),
    ];

    let registry_client = cluster.grpc_client().registry();

    // The Affinity Group specs should now have been loaded in memory.
    // Fetch the specs.
    let specs = registry_client
        .get_specs(&GetSpecs {}, None)
        .await
        .expect("Should be able to fetch specs");

    // Fail if there are affinity group specs from previous test lingering around.
    assert_eq!(specs.affinity_groups.len(), 0);

    let volume_client = cluster.grpc_client().volume();

    for &item in &vols {
        volume_client
            .create(
                &CreateVolume {
                    uuid: VolumeId::try_from(item.1).unwrap(),
                    size: 5242880,
                    replicas: 1,
                    affinity_group: item.0.map(|val| AffinityGroup::new(val.to_string())),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("Volume creation should succeed");
    }

    for &item in &vols {
        volume_client
            .set_replica(
                &SetVolumeReplica {
                    uuid: VolumeId::try_from(item.1).unwrap(),
                    replicas: 2,
                },
                None,
            )
            .await
            .expect("Scale up should not fail");
    }

    for &item in &vols {
        volume_client
            .set_replica(
                &SetVolumeReplica {
                    uuid: VolumeId::try_from(item.1).unwrap(),
                    replicas: 3,
                },
                None,
            )
            .await
            .expect("Scale up should not fail");
    }

    for &item in &vols {
        volume_client
            .set_replica(
                &SetVolumeReplica {
                    uuid: VolumeId::try_from(item.1).unwrap(),
                    replicas: 2,
                },
                None,
            )
            .await
            .expect("Scale down should not fail");
    }

    for &item in &vols {
        volume_client
            .set_replica(
                &SetVolumeReplica {
                    uuid: VolumeId::try_from(item.1).unwrap(),
                    replicas: 1,
                },
                None,
            )
            .await
            .expect_err("Scale down should fail");
    }
}
