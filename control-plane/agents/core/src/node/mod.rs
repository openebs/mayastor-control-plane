mod registry;
/// Node Service
pub(super) mod service;
mod specs;
/// node watchdog to keep track of a node's liveness
pub(crate) mod watchdog;

use super::{controller::registry::Registry, CliArgs};
use common::Service;
use common_lib::{
    transport_api::{v0::*, *},
    types::v0::transport::{GetBlockDevices, GetNodes},
};
use grpc::operations::{node::server::NodeServer, registration::server::RegistrationServer};
use std::sync::Arc;

/// Configure the Service and return the builder.
pub(crate) async fn configure(builder: Service) -> Service {
    let node_service = create_node_service(&builder).await;
    let node_grpc_service = NodeServer::new(Arc::new(node_service.clone()));
    let registration_service = RegistrationServer::new(Arc::new(node_service.clone()));
    builder
        .with_shared_state(node_service)
        .with_shared_state(node_grpc_service)
        .with_shared_state(registration_service)
}

async fn create_node_service(builder: &Service) -> service::Service {
    let registry = builder.shared_state::<Registry>().clone();
    let deadline = CliArgs::args().deadline.into();
    let request = CliArgs::args().request_timeout.into();
    let connect = CliArgs::args().connect_timeout.into();
    let no_min = CliArgs::args().no_min_timeouts;

    service::Service::new(registry.clone(), deadline, request, connect, no_min).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use common_lib::types::v0::{
        store::node::{NodeLabels, NodeSpec},
        transport::{APIVersion, Filter, Node, NodeId, NodeState, NodeStatus},
    };
    use deployer_cluster::ClusterBuilder;
    use grpc::operations::node::traits::NodeOperations;
    use std::time::Duration;

    /// Get new `Node` from the given parameters
    fn new_node(
        id: NodeId,
        endpoint: String,
        status: NodeStatus,
        api_versions: Option<Vec<APIVersion>>,
    ) -> Node {
        Node::new(
            id.clone(),
            Some(NodeSpec::new(
                id.clone(),
                endpoint.clone(),
                NodeLabels::new(),
                None,
            )),
            Some(NodeState::new(id, endpoint, status, api_versions)),
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
        let bus_timeout = TimeoutOptions::default()
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
            &new_node(maya_name.clone(), grpc.clone(), NodeStatus::Online, None)
        );
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let nodes = node_client.get(Filter::None, None).await.unwrap();
        tracing::info!("Nodes: {:?}", nodes);
        assert_eq!(nodes.0.len(), 1);
        // still Online because the node is reachable via gRPC!
        assert_eq!(
            nodes.0.first().unwrap(),
            &new_node(maya_name.clone(), grpc.clone(), NodeStatus::Online, None)
        );

        cluster.composer().kill(maya_name.as_str()).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let nodes = node_client.get(Filter::None, None).await.unwrap();
        tracing::info!("Nodes: {:?}", nodes);
        assert_eq!(nodes.0.len(), 1);
        assert_eq!(
            nodes.0.first().unwrap(),
            &new_node(maya_name.clone(), grpc.clone(), NodeStatus::Offline, None)
        );
        cluster.composer().start(maya_name.as_str()).await.unwrap();

        let node = nodes.0.first().cloned().unwrap();
        cluster.restart_core().await;
        cluster
            .node_service_liveness(Some(bus_timeout.clone()))
            .await
            .expect("Should have restarted by now");

        let nodes = node_client.get(Filter::None, None).await.unwrap();
        tracing::info!("Nodes: {:?}", nodes);
        assert_eq!(nodes.0.len(), 1);
        assert_eq!(
            nodes.0.first().unwrap(),
            &new_node(maya_name.clone(), grpc.clone(), NodeStatus::Online, None)
        );

        cluster.composer().stop(maya_name.as_str()).await.unwrap();
        cluster.restart_core().await;
        cluster
            .node_service_liveness(Some(bus_timeout.clone()))
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
}
