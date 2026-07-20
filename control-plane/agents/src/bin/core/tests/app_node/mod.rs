use deployer_cluster::ClusterBuilder;
use grpc::operations::{app_node::traits::AppNodeOperations, Pagination};
use stor_port::types::v0::{
    store::app_node::TransportCaps,
    transport::{DeregisterAppNode, Filter, RegisterAppNode},
};

/// Test for registration, listing, retrieval, and deregistration of app nodes in a cluster.
///
/// Creates a new cluster and registers two app nodes.
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
async fn app_node_registration() {
    let cluster = ClusterBuilder::builder()
        .with_rest(false)
        .build()
        .await
        .unwrap();

    let node_client = cluster.grpc_client().app_node();

    // Register 2 app nodes. csi-node-1 mimics an older csi-node that has no
    // transport_caps field on the wire; csi-node-2 populates it. Both should
    // persist and round-trip cleanly.
    node_client
        .register_app_node(
            &RegisterAppNode {
                id: "csi-node-1".into(),
                endpoint: "10.0.0.1:50052".parse().unwrap(),
                labels: None,
                transport_caps: None,
            },
            None,
        )
        .await
        .expect("Failed to register app node");

    let node_2_caps = TransportCaps {
        rdma_hca_present: true,
        nvme_rdma_module_loaded: true,
        ana_capable: true,
    };
    node_client
        .register_app_node(
            &RegisterAppNode {
                id: "csi-node-2".into(),
                endpoint: "10.0.0.1:50052".parse().unwrap(),
                labels: None,
                transport_caps: Some(node_2_caps.clone()),
            },
            None,
        )
        .await
        .expect("Failed to register app node");

    // List all the app nodes without pagination.
    let app_nodes = node_client
        .list(None, None)
        .await
        .expect("Failed to list app nodes with no pagination");
    assert_eq!(app_nodes.entries.len(), 2);

    // List app node with some pagination, fetch from 0th.
    let app_nodes = node_client
        .list(Some(Pagination::new(1, 0)), None)
        .await
        .expect("Failed to list app nodes starting from 0th");
    assert_eq!(app_nodes.entries.len(), 1);
    assert_eq!(app_nodes.entries[0].id, "csi-node-1".into());

    // List app node with some pagination, fetch from 1th.
    let app_nodes = node_client
        .list(Some(Pagination::new(1, 1)), None)
        .await
        .expect("Failed to list app nodes starting from 1th");
    assert_eq!(app_nodes.entries.len(), 1);
    assert_eq!(app_nodes.entries[0].id, "csi-node-2".into());

    // Get a specific app node.
    let app_node = node_client
        .get(Filter::AppNode("csi-node-1".into()), None)
        .await
        .expect("Failed to get app node by id");
    assert_eq!(app_node.id, "csi-node-1".into());
    assert!(app_node.spec.transport_caps.is_none());

    // csi-node-2 should carry back the caps it registered with.
    let app_node_2 = node_client
        .get(Filter::AppNode("csi-node-2".into()), None)
        .await
        .expect("Failed to get csi-node-2");
    assert_eq!(app_node_2.spec.transport_caps.as_ref(), Some(&node_2_caps));

    // Now restart core, to check if the app nodes are loaded from the database.
    cluster.restart_core().await;
    cluster
        .node_service_liveness(None)
        .await
        .expect("Should have restarted by now");

    // App nodes should be loaded from the database.
    let app_nodes = node_client
        .list(None, None)
        .await
        .expect("Failed to list app nodes after core restart with no pagination");
    assert_eq!(app_nodes.entries.len(), 2);

    // Caps survive an etcd round-trip.
    let app_node_2 = node_client
        .get(Filter::AppNode("csi-node-2".into()), None)
        .await
        .expect("Failed to get csi-node-2 after restart");
    assert_eq!(app_node_2.spec.transport_caps.as_ref(), Some(&node_2_caps));

    // Deregister one app node.
    node_client
        .deregister_app_node(
            &DeregisterAppNode {
                id: "csi-node-1".into(),
            },
            None,
        )
        .await
        .expect("Failed to deregister node after core restart");

    // Should be exactly 1 app node left.
    let app_nodes = node_client
        .list(None, None)
        .await
        .expect("Failed to list nodes after deregistration with no pagination");

    assert_eq!(app_nodes.entries.len(), 1);
    assert_eq!(app_nodes.entries[0].id, "csi-node-2".into());

    // Get app node after deregistration, it should not exist.
    node_client
        .get(Filter::AppNode("csi-node-1".into()), None)
        .await
        .expect_err("App still exist after deregistration");

    // Restart core again, to check item removed from database.
    cluster.restart_core().await;
    cluster
        .node_service_liveness(None)
        .await
        .expect("Should have restarted by now");

    // Get app node which was deregistered before core restart to check if it gets loaded again
    // or not.
    node_client
        .get(Filter::AppNode("csi-node-1".into()), None)
        .await
        .expect_err("App node should not exist after deregistration");

    // Get app node which was not deregistered it should exist after restart.
    node_client
        .get(Filter::AppNode("csi-node-2".into()), None)
        .await
        .expect("App node should not exist after core restart");
}
