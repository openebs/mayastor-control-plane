use deployer_cluster::ClusterBuilder;
use grpc::operations::{frontend_node::traits::FrontendNodeOperations, Pagination};
use stor_port::types::v0::transport::{DeregisterFrontendNode, Filter, RegisterFrontendNode};

/// Test for registration, listing, retrieval, and deregistration of frontend nodes in a cluster.
///
/// Creates a new cluster and registers two frontend nodes.
///
/// It then verifies the listing of nodes with and without pagination, and the retrieval of a
/// specific node.
///
/// The test also checks the persistence of the nodes by restarting the core agent and verifying the
/// nodes are loaded from the database.
///
/// It then deregisters a node and verifies the node count and the non-existence of the deregistered
/// node.
///
/// Finally, it restarts the core again to ensure the deregistered node is removed from the
/// database and not loaded to memory again.
#[tokio::test]
async fn frontend_registration() {
    let cluster = ClusterBuilder::builder()
        .with_rest(false)
        .with_agents(vec!["core"])
        .build()
        .await
        .unwrap();

    let node_client = cluster.grpc_client().frontend_node();

    // Register 2 frontend nodes.
    node_client
        .register_frontend_node(
            &RegisterFrontendNode {
                id: "csi-node-1".into(),
                endpoint: "10.0.0.1:50052".parse().unwrap(),
                labels: None,
            },
            None,
        )
        .await
        .expect("Failed to register frontend node");

    node_client
        .register_frontend_node(
            &RegisterFrontendNode {
                id: "csi-node-2".into(),
                endpoint: "10.0.0.1:50052".parse().unwrap(),
                labels: None,
            },
            None,
        )
        .await
        .expect("Failed to register frontend node");

    // List all the frontend nodes without pagination.
    let frontend_nodes = node_client
        .list(Filter::None, None, None)
        .await
        .expect("Failed to list frontend nodes with no pagination");
    assert_eq!(frontend_nodes.entries.len(), 2);

    // List frontend node with some pagination, fetch from 0th.
    let frontend_nodes = node_client
        .list(Filter::None, Some(Pagination::new(1, 0)), None)
        .await
        .expect("Failed to list frontend nodes starting from 0th");
    assert_eq!(frontend_nodes.entries.len(), 1);
    assert_eq!(frontend_nodes.entries[0].id, "csi-node-1".into());

    // List frontend node with some pagination, fetch from 1th.
    let frontend_nodes = node_client
        .list(Filter::None, Some(Pagination::new(1, 1)), None)
        .await
        .expect("Failed to list frontend nodes starting from 1th");
    assert_eq!(frontend_nodes.entries.len(), 1);
    assert_eq!(frontend_nodes.entries[0].id, "csi-node-2".into());

    // Get a specific frontend node.
    let frontend_node = node_client
        .get(Filter::FrontendNode("csi-node-1".into()), None)
        .await
        .expect("Failed to get frontend node by id");
    assert_eq!(frontend_node.id, "csi-node-1".into());

    // Now restart core, to check if the frontend nodes are loaded from the database.
    cluster.restart_core().await;
    cluster
        .node_service_liveness(None)
        .await
        .expect("Should have restarted by now");

    // Frontend nodes should be loaded from the database.
    let frontend_nodes = node_client
        .list(Filter::None, None, None)
        .await
        .expect("Failed to list frontend nodes after core restart with no pagination");
    assert_eq!(frontend_nodes.entries.len(), 2);

    // Deregister one frontend node.
    node_client
        .deregister_frontend_node(
            &DeregisterFrontendNode {
                id: "csi-node-1".into(),
            },
            None,
        )
        .await
        .expect("Failed to deregister node after core restart");

    // Should be exactly 1 frontend node left.
    let frontend_nodes = node_client
        .list(Filter::None, None, None)
        .await
        .expect("Failed to list nodes after deregistration with no pagination");

    assert_eq!(frontend_nodes.entries.len(), 1);
    assert_eq!(frontend_nodes.entries[0].id, "csi-node-2".into());

    // Get frontend node after deregistration, it should not exist.
    node_client
        .get(Filter::FrontendNode("csi-node-1".into()), None)
        .await
        .expect_err("Frontend still exist after deregistration");

    // Restart core again, to check item removed from database.
    cluster.restart_core().await;
    cluster
        .node_service_liveness(None)
        .await
        .expect("Should have restarted by now");

    // Get frontend node which was deregistered before core restart to check if it gets loaded again
    // or not.
    node_client
        .get(Filter::FrontendNode("csi-node-1".into()), None)
        .await
        .expect_err("Frontend node should not exist after deregistration");

    // Get frontend node which was not deregistered it should exist after restart.
    node_client
        .get(Filter::FrontendNode("csi-node-2".into()), None)
        .await
        .expect("Frontend node should not exist after core restart");
}
