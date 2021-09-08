pub(crate) mod registry;
pub(super) mod service;
mod specs;
/// node watchdog to keep track of a node's liveness
pub(crate) mod watchdog;

use super::{
    core::registry::Registry, handler, handler_publish, impl_publish_handler, impl_request_handler,
    CliArgs,
};
use common::{errors::SvcError, Service};
use common_lib::mbus_api::{v0::*, *};

use async_trait::async_trait;
use common_lib::types::v0::message_bus::{
    ChannelVs, Deregister, GetBlockDevices, GetNodes, GetSpecs, GetStates, Register,
};
use std::{convert::TryInto, marker::PhantomData};
use structopt::StructOpt;

pub(crate) async fn configure(builder: Service) -> Service {
    let node_service = create_node_service(&builder).await;
    builder
        .with_shared_state(node_service)
        .with_channel(ChannelVs::Registry)
        .with_subscription(handler_publish!(Register))
        .with_subscription(handler_publish!(Deregister))
        .with_subscription(handler!(GetSpecs))
        .with_subscription(handler!(GetStates))
        .with_channel(ChannelVs::Node)
        .with_subscription(handler!(GetNodes))
        .with_subscription(handler!(GetBlockDevices))
        .with_default_liveness()
}

async fn create_node_service(builder: &Service) -> service::Service {
    let registry = builder.get_shared_state::<Registry>().clone();
    let deadline = CliArgs::from_args().deadline.into();
    let request = CliArgs::from_args().request.into();
    let connect = CliArgs::from_args().connect.into();
    let service = service::Service::new(registry.clone(), deadline, request, connect);

    // attempt to reload the node state based on the specification
    for node in registry.specs().get_nodes() {
        service
            .register_state(&Register {
                id: node.id().clone(),
                grpc_endpoint: node.endpoint().to_string(),
            })
            .await;
    }

    service
}

#[cfg(test)]
mod tests {
    use super::*;
    use common_lib::types::v0::{
        message_bus::{Liveness, Node, NodeId, NodeState, NodeStatus},
        store::node::{NodeLabels, NodeSpec},
    };
    use std::time::Duration;
    use testlib::ClusterBuilder;

    /// Get new `Node` from the given parameters
    fn new_node(id: NodeId, endpoint: String, status: NodeStatus) -> Node {
        Node::new(
            id.clone(),
            Some(NodeSpec::new(
                id.clone(),
                endpoint.clone(),
                NodeLabels::new(),
            )),
            Some(NodeState::new(id, endpoint, status)),
        )
    }

    #[actix_rt::test]
    async fn node() {
        let cluster = ClusterBuilder::builder()
            .with_rest(false)
            .with_agents(vec!["core"])
            .with_node_deadline("2s")
            .build()
            .await
            .unwrap();
        let bus_timeout = TimeoutOptions::default()
            .with_timeout(Duration::from_secs(1))
            .with_timeout_backoff(Duration::from_millis(100));

        let maya_name = cluster.node(0);
        let grpc = format!("{}:10124", cluster.node_ip(0));

        let nodes = GetNodes::default().request().await.unwrap();
        tracing::info!("Nodes: {:?}", nodes);
        assert_eq!(nodes.0.len(), 1);
        assert_eq!(
            nodes.0.first().unwrap(),
            &new_node(maya_name.clone(), grpc.clone(), NodeStatus::Online)
        );
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let nodes = GetNodes::default().request().await.unwrap();
        tracing::info!("Nodes: {:?}", nodes);
        assert_eq!(nodes.0.len(), 1);
        assert_eq!(
            nodes.0.first().unwrap(),
            &new_node(maya_name.clone(), grpc.clone(), NodeStatus::Offline)
        );

        let node = nodes.0.first().cloned().unwrap();
        cluster.composer().restart("core").await.unwrap();
        Liveness {}
            .request_on_ext(ChannelVs::Node, bus_timeout.clone())
            .await
            .unwrap();

        let nodes = GetNodes::default().request().await.unwrap();
        tracing::info!("Nodes: {:?}", nodes);
        assert_eq!(nodes.0.len(), 1);
        assert_eq!(
            nodes.0.first().unwrap(),
            &new_node(maya_name.clone(), grpc.clone(), NodeStatus::Online)
        );

        cluster.composer().stop(maya_name.as_str()).await.unwrap();
        cluster.composer().restart("core").await.unwrap();
        Liveness {}
            .request_on_ext(ChannelVs::Node, bus_timeout)
            .await
            .unwrap();

        let nodes = GetNodes::default().request().await.unwrap();
        tracing::info!("Nodes: {:?}", nodes);
        assert_eq!(nodes.0.len(), 1);
        assert_eq!(
            nodes.0.first().unwrap(),
            &Node::new(maya_name.clone(), node.spec().cloned(), None)
        );
    }
}
