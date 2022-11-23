use common_lib::types::v0::transport as v0;
use deployer_cluster::{Cluster, ClusterBuilder};

use openapi::models;

#[tokio::test]
async fn allowed_hosts() {
    let size = 20 * 1024 * 1024;
    let cluster = ClusterBuilder::builder()
        .with_io_engines(3)
        .with_tmpfs_pool(size * 4)
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
    let replica1_uri = replica.uri.to_string();
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
