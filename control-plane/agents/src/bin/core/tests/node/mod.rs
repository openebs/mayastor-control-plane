use deployer_cluster::ClusterBuilder;
use grpc::operations::node::traits::NodeOperations;
use std::time::Duration;
use stor_port::types::v0::{
    store::node::{NodeLabels, NodeSpec},
    transport::{ApiVersion, Filter, HostNqn, Node, NodeId, NodeState, NodeStatus},
};

/// Get new `Node` from the given parameters
fn new_node(
    id: NodeId,
    endpoint: String,
    status: NodeStatus,
    api_versions: Option<Vec<ApiVersion>>,
    node_nqn: Option<HostNqn>,
) -> Node {
    let endpoint = std::str::FromStr::from_str(&endpoint).unwrap();
    Node::new(
        id.clone(),
        Some(NodeSpec::new(
            id.clone(),
            endpoint,
            NodeLabels::new(),
            None,
            node_nqn.clone(),
        )),
        Some(NodeState::new(id, endpoint, status, api_versions, node_nqn)),
    )
}

#[tokio::test]
async fn node() {
    let cluster = ClusterBuilder::builder()
        .with_rest(false)
        .with_agents(vec!["core"])
        .with_node_deadline("2s")
        .build()
        .await
        .unwrap();
    let rpc_timeout = stor_port::transport_api::TimeoutOptions::default()
        .with_req_timeout(Duration::from_secs(1))
        .with_timeout_backoff(Duration::from_millis(100))
        .with_max_retries(6);

    let maya_name = cluster.node(0);
    let grpc = format!("{}:10124", cluster.node_ip(0));
    let node_client = cluster.grpc_client().node();
    let nodes = node_client.get(Filter::None, None).await.unwrap();
    tracing::info!("Nodes: {:?}", nodes);
    assert_eq!(nodes.0.len(), 1);
    assert_eq!(
        nodes.0.first().unwrap(),
        &new_node(
            maya_name.clone(),
            grpc.clone(),
            NodeStatus::Online,
            None,
            Some(HostNqn::from_nodename(&maya_name.to_string()))
        )
    );
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let nodes = node_client.get(Filter::None, None).await.unwrap();
    tracing::info!("Nodes: {:?}", nodes);
    assert_eq!(nodes.0.len(), 1);
    // still Online because the node is reachable via gRPC!
    assert_eq!(
        nodes.0.first().unwrap(),
        &new_node(
            maya_name.clone(),
            grpc.clone(),
            NodeStatus::Online,
            None,
            Some(HostNqn::from_nodename(&maya_name.to_string()))
        )
    );

    cluster.composer().kill(maya_name.as_str()).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let nodes = node_client.get(Filter::None, None).await.unwrap();
    tracing::info!("Nodes: {:?}", nodes);
    assert_eq!(nodes.0.len(), 1);
    assert_eq!(
        nodes.0.first().unwrap(),
        &new_node(
            maya_name.clone(),
            grpc.clone(),
            NodeStatus::Offline,
            None,
            Some(HostNqn::from_nodename(&maya_name.to_string()))
        )
    );
    cluster.composer().start(maya_name.as_str()).await.unwrap();

    let node = nodes.0.first().cloned().unwrap();
    cluster.restart_core().await;
    cluster
        .node_service_liveness(Some(rpc_timeout.clone()))
        .await
        .expect("Should have restarted by now");

    let nodes = node_client.get(Filter::None, None).await.unwrap();
    tracing::info!("Nodes: {:?}", nodes);
    assert_eq!(nodes.0.len(), 1);
    assert_eq!(
        nodes.0.first().unwrap(),
        &new_node(
            maya_name.clone(),
            grpc.clone(),
            NodeStatus::Online,
            None,
            Some(HostNqn::from_nodename(&maya_name.to_string()))
        )
    );

    cluster.composer().stop(maya_name.as_str()).await.unwrap();
    cluster.restart_core().await;
    cluster
        .node_service_liveness(Some(rpc_timeout.clone()))
        .await
        .expect("Should have restarted by now");

    let nodes = node_client.get(Filter::None, None).await.unwrap();
    tracing::info!("Nodes: {:?}", nodes);
    assert_eq!(nodes.0.len(), 1);
    assert_eq!(
        nodes.0.first().unwrap(),
        &Node::new(maya_name.clone(), node.spec().cloned(), None)
    );
}

#[tokio::test]
async fn large_cluster() {
    let expected_nodes = 2;
    let cluster = ClusterBuilder::builder()
        .with_rest(false)
        .with_agents(vec!["core"])
        .with_io_engines(expected_nodes as u32)
        .with_node_deadline("2s")
        .build()
        .await
        .unwrap();

    let node_client = cluster.grpc_client().node();
    let nodes = node_client.get(Filter::None, None).await.unwrap();
    tracing::info!("Nodes: {:?}", nodes);
    assert_eq!(nodes.0.len(), expected_nodes);

    cluster.restart_core().await;
    cluster
        .node_service_liveness(None)
        .await
        .expect("Should have restarted by now");

    let nodes = node_client.get(Filter::None, None).await.unwrap();
    tracing::info!("Nodes: {:?}", nodes);
    assert_eq!(nodes.0.len(), expected_nodes);
}
