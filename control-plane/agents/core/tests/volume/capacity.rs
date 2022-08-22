#![cfg(test)]

use common_lib::types::v0::transport::Filter;
use deployer_cluster::ClusterBuilder;
use grpc::operations::node::traits::NodeOperations;
use std::time::Duration;

#[tokio::test]
async fn fault_enospc_child() {
    let cluster = ClusterBuilder::builder()
        .with_rest(true)
        .with_io_engines(1)
        .with_pools(1)
        .with_cache_period("1s")
        .with_reconcile_period(Duration::from_secs(1), Duration::from_secs(1))
        .build()
        .await
        .unwrap();

    let node_client = cluster.grpc_client().node();
    let nodes = node_client.get(Filter::None, None).await.unwrap();
    tracing::info!("Nodes: {:?}", nodes);
}
