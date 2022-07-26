use deployer_cluster::{Cluster, ClusterBuilder};
use openapi::{
    apis::{volumes_api, Url, Uuid},
    models,
};

#[tokio::test]
async fn concurrent_rebuilds() {
    let nr_volumes = 20u32;
    let io_engines = 2u32;
    let gig = 1024 * 1024 * 1024u64;
    let pool_size_bytes = (nr_volumes as u64) * (io_engines as u64 + 1) * gig;
    let rebuild_loops = 1;
    let mut replica_count = 1;

    let cluster = ClusterBuilder::builder()
        .with_options(|o| {
            o.with_io_engines(io_engines as u32)
                .with_isolated_io_engine(true)
                .with_io_engine_env("NVME_QPAIR_CONNECT_ASYNC", "true")
        })
        .with_cache_period("10s")
        .with_tmpfs_pool(pool_size_bytes)
        .build()
        .await
        .unwrap();

    let cli = cluster.rest_v00();
    let vol_cli = cli.volumes_api();

    let mut volumes = vec![];
    for i in 0 .. nr_volumes {
        let i = i % io_engines;
        #[allow(clippy::identity_op)]
        let volume = vol_cli
            .put_volume(
                &Uuid::new_v4(),
                models::CreateVolumeBody::new(
                    models::VolumePolicy::new(true),
                    replica_count,
                    1 * gig,
                    false,
                ),
            )
            .await
            .unwrap();
        let volume = vol_cli
            .put_volume_target(
                &volume.spec.uuid,
                cluster.node(i).as_str(),
                models::VolumeShareProtocol::Nvmf,
            )
            .await
            .unwrap();

        volumes.push(volume);
    }

    for _ in 0 .. rebuild_loops {
        match replica_count {
            1 => {
                for volume in &volumes {
                    vol_cli
                        .put_volume_replica_count(&volume.spec.uuid, 2)
                        .await
                        .unwrap();
                }
            }
            // reduce and increase the number of replicas again in order to trigger anothe rebuild
            2 => {
                for volume in &volumes {
                    vol_cli
                        .put_volume_replica_count(&volume.spec.uuid, 1)
                        .await
                        .unwrap();
                }
                for volume in &volumes {
                    vol_cli
                        .put_volume_replica_count(&volume.spec.uuid, 2)
                        .await
                        .unwrap();
                }
            }
            _ => {
                unimplemented!()
            }
        };
        replica_count = 2;

        check_volumes(&cluster, vol_cli, &volumes).await;
    }

    async fn check_volumes(
        cluster: &Cluster,
        vol_cli: &dyn volumes_api::tower::client::direct::Volumes,
        volumes: &[models::Volume],
    ) {
        let start = std::time::Instant::now();
        let mut timeout = std::time::Duration::from_secs(120);
        let timeout_slack = timeout / 2;
        let mut added_slack = false;
        let check_interval = std::time::Duration::from_secs(5);
        loop {
            let curr_volumes = vol_cli.get_volumes(0, None).await.unwrap().entries;
            assert_eq!(volumes.len(), curr_volumes.len());
            // volumes should either be online or degraded (while rebuilding)
            let not_expected = curr_volumes
                .iter()
                .filter(|v| {
                    matches!(
                        v.state.status,
                        models::VolumeStatus::Unknown | models::VolumeStatus::Faulted
                    )
                })
                .collect::<Vec<_>>();
            assert!(not_expected.is_empty());

            let online = curr_volumes
                .iter()
                .filter(|v| matches!(v.state.status, models::VolumeStatus::Online))
                .collect::<Vec<_>>();

            tracing::info!(
                "{} out of {} volumes have been rebuilt...",
                online.len(),
                volumes.len()
            );

            if online.len() == volumes.len() {
                break;
            }

            if start.elapsed() >= timeout {
                // if the nodes are still responsive allow for a bit more slack if the CI
                // performance is slow.
                let mut nodes_ok = true;
                let nodes = cluster.rest_v00().nodes_api().get_nodes().await.unwrap();
                for node in nodes {
                    if let Ok(mut handle) = cluster.grpc_handle(&node.id).await {
                        if handle
                            .io_engine
                            .list_nexus(rpc::io_engine::Null {})
                            .await
                            .is_err()
                        {
                            nodes_ok = false;
                            break;
                        }
                    }
                }
                if nodes_ok {
                    tracing::warn!("Timeout has been hit but the nodes are not locked up");
                    if !added_slack {
                        tracing::warn!("Increasing the timeout slack just once since the nodes are not locked up");
                        added_slack = true;
                        timeout += timeout_slack;
                    }
                }
            }

            if start.elapsed() >= timeout {
                assert_eq!(
                    online.len(),
                    volumes.len(),
                    "Volumes not online within {:?}",
                    timeout
                );
            }

            tokio::time::sleep(check_interval).await;
        }
    }
}

#[tokio::test]
async fn loopback_nvmf() {
    let nexus_size = 10 * 1024 * 1024u64;
    let repl_size = nexus_size * 2;
    let repl_size_mb = repl_size / (1024 * 1024);

    let cluster = ClusterBuilder::builder()
        .with_io_engines(1)
        .with_pools(1)
        .with_options(|o| o.with_io_engine_env("NVME_QPAIR_CONNECT_ASYNC", "true"))
        .build()
        .await
        .unwrap();

    let cli = cluster.rest_v00();
    let repl_api = cli.replicas_api();

    let replica = repl_api
        .put_node_pool_replica(
            cluster.node(0).as_str(),
            cluster.pool(0, 0).as_str(),
            &Uuid::new_v4(),
            models::CreateReplicaBody::new(repl_size, false),
        )
        .await
        .unwrap();

    let share_uri = repl_api
        .put_node_pool_replica_share(
            cluster.node(0).as_str(),
            cluster.pool(0, 0).as_str(),
            &replica.uuid,
        )
        .await
        .unwrap();

    let share_url: Url = share_uri.parse().unwrap();
    assert_eq!(share_url.scheme(), "nvmf");

    let nx_api = cli.nexuses_api();
    let malloc_uri = format!(
        "malloc:///ch?size_mb={}&uuid=68e5fcf2-276a-4b29-8fb6-90bfa841297a",
        repl_size_mb
    );
    let nexus = nx_api
        .put_node_nexus(
            cluster.node(0).as_str(),
            &Uuid::new_v4(),
            models::CreateNexusBody::new(vec![share_uri], nexus_size),
        )
        .await
        .unwrap();

    let ch_api = cli.children_api();

    ch_api
        .put_nexus_child(&nexus.uuid, malloc_uri.as_str())
        .await
        .unwrap();
}
