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
    version: &Option<String>,
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
            None,
            None,
            version.clone(),
        )),
        Some(NodeState::new(
            id,
            endpoint,
            status,
            api_versions,
            node_nqn,
            version.clone(),
        )),
    )
}

#[tokio::test]
async fn node() {
    let rpc_timeout = stor_port::transport_api::TimeoutOptions::default()
        .with_req_timeout(Duration::from_secs(1))
        .with_connect_timeout(Duration::from_millis(150))
        .with_timeout_backoff(Duration::from_millis(150))
        .with_max_retries(12);

    let deadline = Duration::from_secs(2);
    let cluster = ClusterBuilder::builder()
        .with_rest(false)
        .with_node_deadline(&humantime::Duration::from(deadline).to_string())
        .with_req_timeouts(rpc_timeout.connect_timeout(), rpc_timeout.base_timeout())
        .build()
        .await
        .unwrap();

    let maya_name = cluster.node(0);
    let grpc = format!("{}:10124", cluster.node_ip(0));
    let node_client = cluster.grpc_client().node();
    let nodes = node_client.get(Filter::None, false, None).await.unwrap();
    tracing::info!("Nodes: {:?}", nodes);

    assert_eq!(nodes.0.len(), 1);
    let node = nodes.0.first().unwrap();

    assert_eq!(
        node,
        &new_node(
            maya_name.clone(),
            grpc.clone(),
            NodeStatus::Online,
            None,
            Some(HostNqn::from_nodename(&maya_name.to_string())),
            &node.state().and_then(|n| n.version.clone())
        )
    );
    // wait for node to miss its deadline
    tokio::time::sleep(deadline).await;

    let nodes = node_client
        .get(Filter::Node(node.id().clone()), false, None)
        .await
        .unwrap();
    tracing::info!("Nodes: {:?}", nodes);
    assert_eq!(nodes.0.len(), 1);

    // still Online because the node is reachable via gRPC!
    // note that we don't currently expose missed deadline :(
    assert_eq!(nodes.0.first().unwrap(), node);

    cluster.composer().kill(maya_name.as_str()).await.unwrap();
    cluster
        .wait_node_status(node.id(), NodeStatus::Offline)
        .await
        .unwrap();
    cluster.composer().start(maya_name.as_str()).await.unwrap();

    cluster.restart_core().await;
    cluster
        .node_service_liveness(Some(rpc_timeout.clone()))
        .await
        .expect("Should have restarted by now");

    cluster
        .wait_node_status(node.id(), NodeStatus::Online)
        .await
        .unwrap();
    cluster.composer().stop(maya_name.as_str()).await.unwrap();
    cluster.restart_core().await;
    cluster
        .node_service_liveness(Some(rpc_timeout.clone()))
        .await
        .expect("Should have restarted by now");

    let nodes = node_client.get(Filter::None, false, None).await.unwrap();
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
        .with_io_engines(expected_nodes as u32)
        .with_node_deadline("2s")
        .build()
        .await
        .unwrap();

    let node_client = cluster.grpc_client().node();
    let nodes = node_client.get(Filter::None, false, None).await.unwrap();
    tracing::info!("Nodes: {:?}", nodes);
    assert_eq!(nodes.0.len(), expected_nodes);

    cluster.restart_core().await;
    cluster
        .node_service_liveness(None)
        .await
        .expect("Should have restarted by now");

    let nodes = node_client.get(Filter::None, false, None).await.unwrap();
    tracing::info!("Nodes: {:?}", nodes);
    assert_eq!(nodes.0.len(), expected_nodes);
}
