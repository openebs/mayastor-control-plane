use common_lib::types::v0::{transport as v0, transport::strip_queries};
use deployer_cluster::{Cluster, ClusterBuilder};
use std::collections::HashMap;

use openapi::{models, models::PublishVolumeBody};

#[tokio::test]
async fn allowed_hosts() {
    let size = 20 * 1024 * 1024;
    let cluster = ClusterBuilder::builder()
        .with_io_engines(3)
        .with_tmpfs_pool(size * 4)
        .with_options(|o| o.with_io_engine_hostnqn(true))
        .build()
        .await
        .unwrap();
    let rest = cluster.rest_v00();
    let replica_client = rest.replicas_api();
    let nexus_client = rest.nexuses_api();

    let replica = replica_client
        .put_pool_replica(
            cluster.pool(0, 0).as_str(),
            &Cluster::replica(0, 0, 0),
            models::CreateReplicaBody {
                share: Some(models::ReplicaShareProtocol::Nvmf),
                allowed_hosts: Some(vec![cluster.node_nqn(1).to_string()]),
                size,
                thin: false,
            },
        )
        .await
        .unwrap();

    let replica1_uri = strip_queries(replica.uri.to_string(), "hostnqn");
    tracing::debug!("ReplicaUri: {}", replica1_uri);

    let _nexus1 = nexus_client
        .put_node_nexus(
            cluster.node(1).as_str(),
            &v0::NexusId::new(),
            models::CreateNexusBody::new(vec![replica1_uri.clone()], size),
        )
        .await
        .expect("Node 1 is an allowed host!");

    let _nexus2 = nexus_client
        .put_node_nexus(
            cluster.node(2).as_str(),
            &v0::NexusId::new(),
            models::CreateNexusBody::new(vec![replica1_uri.clone()], size),
        )
        .await
        .expect_err("Node 2 is not an allowed host!");

    let replica1_uri = replica_client
        .put_pool_replica_share(
            &replica.pool,
            &replica.uuid,
            Some(vec![
                cluster.node_nqn(1).to_string(),
                cluster.node_nqn(2).to_string(),
            ]),
        )
        .await
        .unwrap();

    let nexus2 = nexus_client
        .put_node_nexus(
            cluster.node(2).as_str(),
            &v0::NexusId::new(),
            models::CreateNexusBody::new(vec![replica1_uri.clone()], size),
        )
        .await
        .expect("Node 2 is now an allowed host!");

    nexus_client.del_nexus(&nexus2.uuid).await.unwrap();

    let mut handle = cluster.grpc_handle(&replica.node).await.unwrap();
    handle
        .share_replica(
            &replica.uuid.to_string(),
            vec![cluster.node_nqn(1).to_string()],
        )
        .await
        .unwrap();

    let _nexus2 = nexus_client
        .put_node_nexus(
            cluster.node(2).as_str(),
            &v0::NexusId::new(),
            models::CreateNexusBody::new(vec![replica1_uri.clone()], size),
        )
        .await
        .expect_err("Node 2 is not an allowed host!");

    let replica1_uri = replica_client
        .put_pool_replica_share(
            &replica.pool,
            &replica.uuid,
            Some(vec![
                cluster.node_nqn(1).to_string(),
                cluster.node_nqn(2).to_string(),
            ]),
        )
        .await
        .unwrap();

    let _nexus2 = nexus_client
        .put_node_nexus(
            cluster.node(2).as_str(),
            &v0::NexusId::new(),
            models::CreateNexusBody::new(vec![replica1_uri.clone()], size),
        )
        .await
        .expect("Node 2 is now an allowed host!");
}

#[tokio::test]
async fn volume_allowed_host() {
    let size = 20 * 1024 * 1024;
    let cluster = ClusterBuilder::builder()
        .with_io_engines(3)
        .with_tmpfs_pool(size * 4)
        .build()
        .await
        .unwrap();
    let rest = cluster.rest_v00();
    let volumes_api = rest.volumes_api();

    let volume_1_size = 10u64 * 1024 * 1024;
    let mut volume_1 = volumes_api
        .put_volume(
            &"ec4e66fd-3b33-4439-b504-d49aba53da26".parse().unwrap(),
            models::CreateVolumeBody::new(models::VolumePolicy::new(true), 2, volume_1_size, true),
        )
        .await
        .unwrap();
    volume_1 = volumes_api
        .put_volume_target(
            &volume_1.spec.uuid,
            PublishVolumeBody::new_all(
                HashMap::new(),
                None,
                Some(cluster.node(0).to_string()),
                models::VolumeShareProtocol::Nvmf,
                None,
            ),
        )
        .await
        .unwrap();

    let _ = volume_1;
}
