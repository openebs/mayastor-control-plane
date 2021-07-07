pub(super) mod service;
/// node watchdog to keep track of a node's liveness
pub(crate) mod watchdog;

use super::{
    core::registry, handler, handler_publish, impl_publish_handler, impl_request_handler, CliArgs,
};
use common::{errors::SvcError, Service};
use common_lib::mbus_api::{v0::*, *};

use async_trait::async_trait;
use common_lib::types::v0::message_bus::{
    ChannelVs, Deregister, GetBlockDevices, GetNodes, GetSpecs, GetStates, Register,
};
use std::{convert::TryInto, marker::PhantomData};
use structopt::StructOpt;

pub(crate) fn configure(builder: Service) -> Service {
    let node_service = create_node_service(&builder);
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

fn create_node_service(builder: &Service) -> service::Service {
    let registry = builder.get_shared_state::<registry::Registry>().clone();
    let deadline = CliArgs::from_args().deadline.into();
    let request = CliArgs::from_args().request.into();
    let connect = CliArgs::from_args().connect.into();
    service::Service::new(registry, deadline, request, connect)
}

#[cfg(test)]
mod tests {
    use super::*;
    use common_lib::types::v0::message_bus::{Node, NodeState};
    use testlib::ClusterBuilder;

    #[actix_rt::test]
    async fn node() {
        let cluster = ClusterBuilder::builder()
            .with_rest(false)
            .with_agents(vec!["core"])
            .with_node_deadline("2s")
            .build()
            .await
            .unwrap();

        let maya_name = cluster.node(0);
        let grpc = format!("{}:10124", cluster.node_ip(0));

        let nodes = GetNodes {}.request().await.unwrap();
        tracing::info!("Nodes: {:?}", nodes);
        assert_eq!(nodes.0.len(), 1);
        assert_eq!(
            nodes.0.first().unwrap(),
            &Node {
                id: maya_name.clone(),
                grpc_endpoint: grpc.clone(),
                state: NodeState::Online,
            }
        );
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let nodes = GetNodes {}.request().await.unwrap();
        tracing::info!("Nodes: {:?}", nodes);
        assert_eq!(nodes.0.len(), 1);
        assert_eq!(
            nodes.0.first().unwrap(),
            &Node {
                id: maya_name.clone(),
                grpc_endpoint: grpc.clone(),
                state: NodeState::Offline,
            }
        );
    }
}
