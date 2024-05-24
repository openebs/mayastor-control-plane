use deployer_cluster::{result_either, test_result_grpc, Cluster, ClusterBuilder};
use grpc::operations::nexus::traits::NexusOperations;
use openapi::models;
use stor_port::types::v0::transport as v0;

#[tokio::test]
async fn create_nexus_malloc() {
    let cluster = ClusterBuilder::builder().build().await.unwrap();
    let nexus_client = cluster.grpc_client().nexus();

    nexus_client
        .create(
            &v0::CreateNexus {
                node: cluster.node(0),
                uuid: v0::NexusId::new(),
                size: 10 * 1024 * 1024,
                children: vec![
                    "malloc:///disk?size_mb=100&uuid=281b87d3-0401-459c-a594-60f76d0ce0da".into(),
                ],
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn create_nexus_sizes() {
    let cluster = ClusterBuilder::builder()
        .with_rest_timeout(std::time::Duration::from_secs(2))
        // don't log whilst we have the allow_fail
        .compose_build(|c| c.with_logs(false))
        .await
        .unwrap();
    let nexus_client = cluster.grpc_client().nexus();

    for size_mb in &vec![6, 10, 100] {
        let size = size_mb * 1024 * 1024;
        let disk = || {
            format!("malloc:///disk?size_mb={size_mb}&uuid=281b87d3-0401-459c-a594-60f76d0ce0da")
        };
        let sizes = vec![Ok(size / 2), Ok(size), Err(size + 512)];
        for test in sizes {
            let size = result_either!(test);
            test_result_grpc(&test, async {
                let nexus = nexus_client
                    .create(
                        &v0::CreateNexus {
                            node: cluster.node(0),
                            uuid: v0::NexusId::new(),
                            size,
                            children: vec![disk().into()],
                            ..Default::default()
                        },
                        None,
                    )
                    .await;

                if let Ok(nexus) = &nexus {
                    nexus_client
                        .destroy(&v0::DestroyNexus::from(nexus.clone()), None)
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
            format!("malloc:///disk?size_mb={size_mb}&uuid=281b87d3-0401-459c-a594-60f76d0ce0da")
        };
        let sizes = vec![Err(size / 2), Err(size), Err(size + 512)];
        for test in sizes {
            let size = result_either!(test);
            test_result_grpc(&test, async {
                let nexus = nexus_client
                    .create(
                        &v0::CreateNexus {
                            node: cluster.node(0),
                            uuid: v0::NexusId::new(),
                            size,
                            children: vec![disk().into()],
                            ..Default::default()
                        },
                        None,
                    )
                    .await;
                if let Ok(nexus) = &nexus {
                    nexus_client
                        .destroy(&v0::DestroyNexus::from(nexus.clone()), None)
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

#[tokio::test]
async fn create_nexus_local_replica() {
    let size = 10 * 1024 * 1024;
    let cluster = ClusterBuilder::builder()
        .with_pools(1)
        .with_replicas(1, size, v0::Protocol::None)
        .build()
        .await
        .unwrap();
    let nexus_client = cluster.grpc_client().nexus();

    let replica = cluster
        .rest_v00()
        .replicas_api()
        .get_replica(&Cluster::replica(0, 0, 0))
        .await
        .unwrap();

    nexus_client
        .create(
            &v0::CreateNexus {
                node: cluster.node(0),
                uuid: v0::NexusId::new(),
                size,
                children: vec![replica.uri.into()],
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn create_nexus_replicas() {
    let size = 10 * 1024 * 1024;
    let cluster = ClusterBuilder::builder()
        .with_io_engines(2)
        .with_pools(1)
        .with_replicas(1, size, v0::Protocol::None)
        .build()
        .await
        .unwrap();
    let nexus_client = cluster.grpc_client().nexus();
    let local = cluster
        .rest_v00()
        .replicas_api()
        .get_replica(&Cluster::replica(0, 0, 0))
        .await
        .unwrap();
    let remote = cluster
        .rest_v00()
        .replicas_api()
        .put_node_pool_replica_share(
            cluster.node(1).as_str(),
            cluster.pool(1, 0).as_str(),
            &(Cluster::replica(1, 0, 0).into()),
            None,
        )
        .await
        .unwrap();

    nexus_client
        .create(
            &v0::CreateNexus {
                node: cluster.node(0),
                uuid: v0::NexusId::new(),
                size,
                children: vec![local.uri.into(), remote.into()],
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn create_nexus_replica_not_available() {
    let size = 10 * 1024 * 1024;
    let cluster = ClusterBuilder::builder()
        .with_rest(true)
        //  .with_agents(vec!["core"])
        .with_io_engines(2)
        .with_pools(1)
        .with_replicas(1, size, v0::Protocol::None)
        .build()
        .await
        .unwrap();

    let local = cluster
        .rest_v00()
        .replicas_api()
        .get_replica(&Cluster::replica(0, 0, 0))
        .await
        .unwrap();
    let remote = cluster
        .rest_v00()
        .replicas_api()
        .put_pool_replica_share(
            cluster.pool(1, 0).as_str(),
            &Cluster::replica(1, 0, 0),
            None,
        )
        .await
        .unwrap();
    cluster
        .rest_v00()
        .replicas_api()
        .del_pool_replica_share(cluster.pool(1, 0).as_str(), &Cluster::replica(1, 0, 0))
        .await
        .unwrap();
    cluster
        .rest_v00()
        .nexuses_api()
        .put_node_nexus(
            cluster.node(0).as_str(),
            &v0::NexusId::new(),
            models::CreateNexusBody::new(vec![local.uri, remote], size),
        )
        .await
        .expect_err("One replica is not present so nexus shouldn't be created");
}
