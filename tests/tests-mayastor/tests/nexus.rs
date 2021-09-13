#![feature(allow_fail)]
use testlib::*;

#[actix_rt::test]
async fn create_nexus_malloc() {
    let cluster = ClusterBuilder::builder().build().await.unwrap();

    cluster
        .rest_v0()
        .create_nexus(v0::CreateNexus {
            node: cluster.node(0),
            uuid: v0::NexusId::new(),
            size: 10 * 1024 * 1024,
            children: vec![
                "malloc:///disk?size_mb=100&uuid=281b87d3-0401-459c-a594-60f76d0ce0da".into(),
            ],
            ..Default::default()
        })
        .await
        .unwrap();
}

#[actix_rt::test]
async fn create_nexus_sizes() {
    let cluster = ClusterBuilder::builder()
        .with_rest_timeout(std::time::Duration::from_secs(2))
        // don't log whilst we have the allow_fail
        .compose_build(|c| c.with_logs(false))
        .await
        .unwrap();

    for size_mb in &vec![6, 10, 100] {
        let size = size_mb * 1024 * 1024;
        let disk = || {
            format!(
                "malloc:///disk?size_mb={}&uuid=281b87d3-0401-459c-a594-60f76d0ce0da",
                size_mb
            )
        };
        let sizes = vec![Ok(size / 2), Ok(size), Err(size + 512)];
        for test in sizes {
            let size = result_either!(test);
            test_result(&test, async {
                let nexus = cluster
                    .rest_v0()
                    .create_nexus(v0::CreateNexus {
                        node: cluster.node(0),
                        uuid: v0::NexusId::new(),
                        size,
                        children: vec![disk().into()],
                        ..Default::default()
                    })
                    .await;
                if let Ok(nexus) = &nexus {
                    cluster
                        .rest_v0()
                        .destroy_nexus(v0::DestroyNexus {
                            node: nexus.node.clone(),
                            uuid: nexus.uuid.clone(),
                        })
                        .await
                        .unwrap();
                }
                nexus
            })
            .await
            .unwrap();
        }
    }

    for size_mb in &vec![1, 2, 4] {
        let size = size_mb * 1024 * 1024;
        let disk = || {
            format!(
                "malloc:///disk?size_mb={}&uuid=281b87d3-0401-459c-a594-60f76d0ce0da",
                size_mb
            )
        };
        let sizes = vec![Err(size / 2), Err(size), Err(size + 512)];
        for test in sizes {
            let size = result_either!(test);
            test_result(&test, async {
                let nexus = cluster
                    .rest_v0()
                    .create_nexus(v0::CreateNexus {
                        node: cluster.node(0),
                        uuid: v0::NexusId::new(),
                        size,
                        children: vec![disk().into()],
                        ..Default::default()
                    })
                    .await;
                if let Ok(nexus) = &nexus {
                    cluster
                        .rest_v0()
                        .destroy_nexus(v0::DestroyNexus {
                            node: nexus.node.clone(),
                            uuid: nexus.uuid.clone(),
                        })
                        .await
                        .unwrap();
                }
                nexus
            })
            .await
            .unwrap();
        }
    }
}

#[actix_rt::test]
async fn create_nexus_local_replica() {
    let size = 10 * 1024 * 1024;
    let cluster = ClusterBuilder::builder()
        .with_pools(1)
        .with_replicas(1, size, v0::Protocol::None)
        .build()
        .await
        .unwrap();

    let replica = cluster
        .rest_v00()
        .replicas_api()
        .get_replica(Cluster::replica(0, 0, 0).as_str())
        .await
        .unwrap();
    cluster
        .rest_v0()
        .create_nexus(v0::CreateNexus {
            node: cluster.node(0),
            uuid: v0::NexusId::new(),
            size,
            children: vec![replica.uri.into()],
            ..Default::default()
        })
        .await
        .unwrap();
}

#[actix_rt::test]
async fn create_nexus_replicas() {
    let size = 10 * 1024 * 1024;
    let cluster = ClusterBuilder::builder()
        .with_mayastors(2)
        .with_pools(1)
        .with_replicas(1, size, v0::Protocol::None)
        .build()
        .await
        .unwrap();

    let local = cluster
        .rest_v00()
        .replicas_api()
        .get_replica(Cluster::replica(0, 0, 0).as_str())
        .await
        .unwrap();
    let remote = cluster
        .rest_v0()
        .share_replica(v0::ShareReplica {
            node: cluster.node(1),
            pool: cluster.pool(1, 0),
            uuid: Cluster::replica(1, 0, 0),
            protocol: v0::ReplicaShareProtocol::Nvmf,
        })
        .await
        .unwrap();

    cluster
        .rest_v0()
        .create_nexus(v0::CreateNexus {
            node: cluster.node(0),
            uuid: v0::NexusId::new(),
            size,
            children: vec![local.uri.into(), remote.into()],
            ..Default::default()
        })
        .await
        .unwrap();
}

#[actix_rt::test]
async fn create_nexus_replica_not_available() {
    let size = 10 * 1024 * 1024;
    let cluster = ClusterBuilder::builder()
        .with_mayastors(2)
        .with_pools(1)
        .with_replicas(1, size, v0::Protocol::None)
        .build()
        .await
        .unwrap();

    let local = cluster
        .rest_v00()
        .replicas_api()
        .get_replica(Cluster::replica(0, 0, 0).as_str())
        .await
        .unwrap();
    let remote = cluster
        .rest_v0()
        .share_replica(v0::ShareReplica {
            node: cluster.node(1),
            pool: cluster.pool(1, 0),
            uuid: Cluster::replica(1, 0, 0),
            protocol: v0::ReplicaShareProtocol::Nvmf,
        })
        .await
        .unwrap();
    cluster
        .rest_v0()
        .unshare_replica(v0::UnshareReplica {
            node: cluster.node(1),
            pool: cluster.pool(1, 0),
            uuid: Cluster::replica(1, 0, 0),
        })
        .await
        .unwrap();

    cluster
        .rest_v0()
        .create_nexus(v0::CreateNexus {
            node: cluster.node(0),
            uuid: v0::NexusId::new(),
            size,
            children: vec![local.uri.into(), remote.into()],
            ..Default::default()
        })
        .await
        .expect_err("One replica is not present so nexus shouldn't be created");
}
