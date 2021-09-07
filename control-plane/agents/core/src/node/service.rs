use super::*;
use crate::core::{registry::Registry, wrapper::NodeWrapper};

use crate::core::reconciler::PollTriggerEvent;
use common::{
    errors::{GrpcRequestError, SvcError},
    v0::msg_translation::RpcToMessageBus,
};
use common_lib::types::v0::message_bus::{
    Filter, GetSpecs, Node, NodeId, NodeState, NodeStatus, Specs, States,
};
use rpc::mayastor::ListBlockDevicesRequest;
use snafu::ResultExt;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

/// Node's Service
#[derive(Debug, Clone)]
pub(crate) struct Service {
    registry: Registry,
    /// deadline for receiving keepalive/Register messages
    deadline: std::time::Duration,
    /// node communication timeouts
    comms_timeouts: NodeCommsTimeout,
}

/// Node communication Timeouts for establishing the connection to a node and
/// the request itself
#[derive(Debug, Clone)]
pub(crate) struct NodeCommsTimeout {
    /// node gRPC connection timeout
    connect: std::time::Duration,
    /// gRPC request timeout
    request: std::time::Duration,
}

impl NodeCommsTimeout {
    fn new(connect: std::time::Duration, request: std::time::Duration) -> Self {
        Self { connect, request }
    }
    /// timeout to establish connection to the node
    pub fn connect(&self) -> std::time::Duration {
        self.connect
    }
    /// timeout for the request itself
    pub fn request(&self) -> std::time::Duration {
        self.request
    }
}

impl Service {
    /// New Node Service which uses the `registry` as its node cache and sets
    /// the `deadline` to each node's watchdog
    pub(super) fn new(
        registry: Registry,
        deadline: std::time::Duration,
        request: std::time::Duration,
        connect: std::time::Duration,
    ) -> Self {
        Self {
            registry,
            deadline,
            comms_timeouts: NodeCommsTimeout::new(connect, request),
        }
    }

    /// Callback to be called when a node's watchdog times out
    pub(super) async fn on_timeout(service: &Service, id: &NodeId) {
        let registry = service.registry.clone();
        let state = registry.nodes.read().await;
        if let Some(node) = state.get(id) {
            let mut node = node.lock().await;
            if node.is_online() {
                tracing::error!(
                    "Node id '{}' missed the registration deadline of {:?}",
                    id,
                    service.deadline
                );
                node.update();
            }
        }
    }

    /// Register a new node through the register information
    pub(super) async fn register(&self, registration: &Register) {
        self.registry.register_node_spec(registration).await;
        self.register_state(registration).await;
    }

    /// Attempt to Register a new node state through the register information
    pub(super) async fn register_state(&self, registration: &Register) {
        let node = NodeState {
            id: registration.id.clone(),
            grpc_endpoint: registration.grpc_endpoint.clone(),
            status: NodeStatus::Online,
        };

        let mut send_event = true;
        let mut nodes = self.registry.nodes.write().await;
        match nodes.get_mut(&node.id) {
            None => {
                let mut node = NodeWrapper::new(&node, self.deadline, self.comms_timeouts.clone());
                if node.load().await.is_ok() {
                    node.watchdog_mut().arm(self.clone());
                    nodes.insert(node.id.clone(), Arc::new(Mutex::new(node)));
                }
            }
            Some(node) => {
                if node.lock().await.status() == &NodeStatus::Online {
                    send_event = false;
                }
                node.lock().await.on_register().await;
            }
        }

        if send_event {
            self.registry
                .notify(PollTriggerEvent::NodeStateChangeOnline)
                .await;
        }
    }

    /// Deregister a node through the deregister information
    pub(super) async fn deregister(&self, node: &Deregister) {
        let nodes = self.registry.nodes.read().await;
        match nodes.get(&node.id) {
            None => {}
            // ideally we want this node to disappear completely when it's not
            // part of the daemonset, but we just don't have that kind of
            // information at this level :(
            // maybe nodes should also be registered/deregistered via REST?
            Some(node) => {
                node.lock().await.set_status(NodeStatus::Unknown);
            }
        }
    }

    /// Get nodes by filter
    pub(crate) async fn get_nodes(&self, request: &GetNodes) -> Result<Nodes, SvcError> {
        match request.filter() {
            Filter::None => {
                let node_states = self.registry.get_node_states().await;
                let node_specs = self.registry.specs.get_nodes();
                let mut nodes = HashMap::new();

                node_states.into_iter().for_each(|state| {
                    let spec = node_specs.iter().find(|s| s.id() == &state.id);
                    nodes.insert(
                        state.id.clone(),
                        Node::new(state.id.clone(), spec.cloned(), Some(state)),
                    );
                });
                node_specs.into_iter().for_each(|spec| {
                    if nodes.get(spec.id()).is_none() {
                        nodes.insert(
                            spec.id().clone(),
                            Node::new(spec.id().clone(), Some(spec), None),
                        );
                    }
                });

                Ok(Nodes(nodes.values().cloned().collect()))
            }
            Filter::Node(node_id) => {
                let node_state = self.registry.get_node_state(node_id).await.ok();
                let node_spec = self.registry.specs.get_node(node_id).ok();
                if node_state.is_none() && node_spec.is_none() {
                    Err(SvcError::NodeNotFound {
                        node_id: node_id.to_owned(),
                    })
                } else {
                    Ok(Nodes(vec![Node::new(
                        node_id.clone(),
                        node_spec,
                        node_state,
                    )]))
                }
            }
            _ => Err(SvcError::InvalidFilter {
                filter: request.filter().clone(),
            }),
        }
    }

    /// Get block devices from a node
    pub(crate) async fn get_block_devices(
        &self,
        request: &GetBlockDevices,
    ) -> Result<BlockDevices, SvcError> {
        let node = self.registry.get_node_wrapper(&request.node).await?;

        let grpc = node.lock().await.grpc_context()?;
        let mut client = grpc.connect().await?;

        let result = client
            .client
            .list_block_devices(ListBlockDevicesRequest { all: request.all })
            .await;

        let response = result
            .context(GrpcRequestError {
                resource: ResourceKind::Block,
                request: "list_block_devices",
            })?
            .into_inner();

        let bdevs = response
            .devices
            .iter()
            .map(|rpc_bdev| rpc_bdev.to_mbus())
            .collect();
        Ok(BlockDevices(bdevs))
    }

    /// Get specs from the registry
    pub(crate) async fn get_specs(&self, _request: &GetSpecs) -> Result<Specs, SvcError> {
        let specs = self.registry.specs.write();
        Ok(Specs {
            volumes: specs.get_volumes(),
            nexuses: specs.get_nexuses(),
            replicas: specs.get_replicas(),
            pools: specs.get_pools(),
        })
    }

    /// Get state information for all resources.
    pub(crate) async fn get_states(&self, _request: &GetStates) -> Result<States, SvcError> {
        let mut nexuses = vec![];
        let mut pools = vec![];
        let mut replicas = vec![];

        // Aggregate the state information from each node.
        let nodes = self.registry.nodes.read().await;
        for (_node_id, locked_node_wrapper) in nodes.iter() {
            let node_wrapper = locked_node_wrapper.lock().await;
            nexuses.extend(node_wrapper.nexus_states());
            pools.extend(node_wrapper.pool_states());
            replicas.extend(node_wrapper.replica_states());
        }

        Ok(States {
            nexuses,
            pools,
            replicas,
        })
    }
}
