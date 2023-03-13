use deployer_cluster::{Cluster, ClusterBuilder};
use grpc::operations::{registry::traits::RegistryOperations, volume::traits::VolumeOperations};
use stor_port::types::v0::transport::{CreateVolume, GetSpecs, VolumeGroup, VolumeId};
use tracing::info;

#[tokio::test]
async fn volume_group() {
    let cluster = ClusterBuilder::builder()
        .with_rest(false)
        .with_agents(vec!["core"])
        .with_io_engines(2)
        .with_pools(2)
        .with_cache_period("1s")
        .build()
        .await
        .unwrap();

    startup_test(&cluster).await;
}

async fn startup_test(cluster: &Cluster) {
    let vols = vec![
        (Some("vg1"), "eba487d9-0b57-407b-8b48-0b631a372183"),
        (Some("vg1"), "359b7e1a-b724-443b-98b4-e6d97fabbb60"),
        (Some("vg2"), "f2296d6a-77a6-401d-aad3-ccdc247b0a56"),
        (None, "bdd3431c-0ccd-4a00-91cd-bb3d7cccb4b2"),
        (None, "52c8f1e9-8538-48ce-9906-adfe3623e032"),
    ];

    let volume_client = cluster.grpc_client().volume();

    // Create all the volumes.
    for &item in &vols {
        let _ = volume_client
            .create(
                &CreateVolume {
                    uuid: VolumeId::try_from(item.1).unwrap(),
                    size: 5242880,
                    replicas: 1,
                    volume_group: item.0.map(|val| VolumeGroup::new(val.to_string())),
                    ..Default::default()
                },
                None,
            )
            .await;
    }

    // Restart the core-agent.
    cluster.restart_core().await;

    // Wait for the core-agent grpc to come up.
    cluster
        .node_service_liveness(None)
        .await
        .expect("Should have restarted by now");

    let registry_client = cluster.grpc_client().registry();

    // The volume group specs should now have been loaded in memory.
    // Fetch the specs.
    let specs = registry_client
        .get_specs(&GetSpecs {}, None)
        .await
        .expect("should be able to fetch specs");

    info!("Volume Group Specs: {:?}", specs.volume_groups);

    // Check for the validity of the volume group specs.
    for vol_grp_spec in specs.volume_groups {
        match vol_grp_spec.id().as_str() {
            "vg1" => {
                assert_eq!(vol_grp_spec.volumes().len(), 2);
                assert!(vol_grp_spec
                    .volumes()
                    .contains(&VolumeId::try_from(vols[0].1).unwrap()));
                assert!(vol_grp_spec
                    .volumes()
                    .contains(&VolumeId::try_from(vols[1].1).unwrap()));
            }
            "vg2" => {
                assert_eq!(vol_grp_spec.volumes().len(), 1);
                assert!(vol_grp_spec
                    .volumes()
                    .contains(&VolumeId::try_from(vols[2].1).unwrap()));
            }
            _ => {}
        }
    }
}
