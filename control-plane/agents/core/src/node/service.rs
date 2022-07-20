use super::*;
use crate::core::{
    reconciler::PollTriggerEvent, registry::Registry, specs::ResourceSpecsLocked,
    wrapper::NodeWrapper,
};
use common::{
    errors::{GrpcRequestError, SvcError},
    v0::msg_translation::RpcToMessageBus,
};
use common_lib::types::v0::message_bus::{
    Deregister, Filter, Node, NodeId, NodeState, NodeStatus, Register,
};

use crate::core::wrapper::InternalOps;
use grpc::{
    context::Context,
    operations::{
        node::traits::{GetBlockDeviceInfo, NodeOperations},
        registration::traits::{DeregisterInfo, RegisterInfo, RegistrationOperations},
    },
};
use rpc::io_engine::ListBlockDevicesRequest;
use snafu::ResultExt;
use std::{collections::HashMap, sync::Arc};

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
    /// return a new `Self` with the connect and request timeouts
    pub(crate) fn new(connect: std::time::Duration, request: std::time::Duration) -> Self {
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

#[tonic::async_trait]
impl NodeOperations for Service {
    async fn get(&self, filter: Filter, _ctx: Option<Context>) -> Result<Nodes, ReplyError> {
        let req = GetNodes::new(filter);
        let nodes = self.get_nodes(&req).await?;
        Ok(nodes)
    }
    async fn probe(&self, _ctx: Option<Context>) -> Result<bool, ReplyError> {
        return Ok(true);
    }

    async fn get_block_devices(
        &self,
        get_blockdevice: &dyn GetBlockDeviceInfo,
        _ctx: Option<Context>,
    ) -> Result<BlockDevices, ReplyError> {
        let req = get_blockdevice.into();
        let blockdevices = self.get_block_devices(&req).await?;
        Ok(blockdevices)
    }

    async fn cordon(&self, id: NodeId, label: String) -> Result<Node, ReplyError> {
        let node = self.cordon(id, label).await?;
        Ok(node)
    }

    async fn uncordon(&self, id: NodeId, label: String) -> Result<Node, ReplyError> {
        let node = self.uncordon(id, label).await?;
        Ok(node)
    }
}

#[tonic::async_trait]
impl RegistrationOperations for Service {
    async fn register(&self, req: &dyn RegisterInfo) -> Result<(), ReplyError> {
        let register = req.into();
        let service = self.clone();
        Context::spawn(async move { service.register(&register).await }).await?;
        Ok(())
    }

    async fn deregister(&self, req: &dyn DeregisterInfo) -> Result<(), ReplyError> {
        let deregister = req.into();
        let service = self.clone();
        Context::spawn(async move { service.deregister(&deregister).await }).await?;
        Ok(())
    }
}

impl Service {
    /// New Node Service which uses the `registry` as its node cache and sets
    /// the `deadline` to each node's watchdog
    pub(super) async fn new(
        registry: Registry,
        deadline: std::time::Duration,
        request: std::time::Duration,
        connect: std::time::Duration,
    ) -> Self {
        let service = Self {
            registry,
            deadline,
            comms_timeouts: NodeCommsTimeout::new(connect, request),
        };
        // attempt to reload the node state based on the specification
        for node in service.registry.specs().get_nodes() {
            service
                .register_state(
                    &Register {
                        id: node.id().clone(),
                        grpc_endpoint: node.endpoint().to_string(),
                    },
                    true,
                )
                .await;
        }
        service
    }
    fn specs(&self) -> &ResourceSpecsLocked {
        self.registry.specs()
    }

    /// Callback to be called when a node's watchdog times out
    pub(super) async fn on_timeout(service: &Service, id: &NodeId) {
        let registry = service.registry.clone();
        let node = {
            let state = registry.nodes().read().await;
            state.get(id).cloned()
        };

        if let Some(node) = node {
            let mut node = node.write().await;
            if node.is_online() {
                node.update_liveness().await;
            }
        }
    }

    /// Register a new node through the register information
    pub(super) async fn register(&self, registration: &Register) {
        self.registry.register_node_spec(registration).await;
        self.register_state(registration, false).await;
    }

    /// Attempt to Register a new node state through the register information.
    /// todo: if we enable concurrent registrations when we move to gRPC, we'll want
    /// to make sure we don't process registrations for the same node in parallel.
    pub(super) async fn register_state(&self, registration: &Register, startup: bool) {
        let node_state = NodeState {
            id: registration.id.clone(),
            grpc_endpoint: registration.grpc_endpoint.clone(),
            status: NodeStatus::Online,
        };

        let nodes = self.registry.nodes();
        let node = nodes.write().await.get_mut(&node_state.id).cloned();
        let send_event = match node {
            None => {
                let mut node =
                    NodeWrapper::new(&node_state, self.deadline, self.comms_timeouts.clone());

                let mut result = node.liveness_probe().await;
                if result.is_ok() {
                    result = node.load().await;
                }
                match result {
                    Ok(_) => {
                        let mut nodes = self.registry.nodes().write().await;
                        if nodes.get_mut(&node_state.id).is_none() {
                            node.watchdog_mut().arm(self.clone());
                            let node = Arc::new(tokio::sync::RwLock::new(node));
                            nodes.insert(node_state.id().clone(), node);
                            true
                        } else {
                            false
                        }
                    }
                    Err(error) => {
                        tracing::warn!(
                            node = %node_state.id(),
                            error = %error,
                            "Failed to register node"
                        );
                        false
                    }
                }
            }
            Some(node) => matches!(node.on_register().await, Ok(true)),
        };

        // don't send these events on startup as the reconciler will start working afterwards anyway
        if send_event && !startup {
            self.registry
                .notify(PollTriggerEvent::NodeStateChangeOnline)
                .await;
        }
    }

    /// Deregister a node through the deregister information
    pub(super) async fn deregister(&self, node: &Deregister) {
        let nodes = self.registry.nodes().read().await;
        match nodes.get(&node.id) {
            None => {}
            // ideally we want this node to disappear completely when it's not
            // part of the daemonset, but we just don't have that kind of
            // information at this level :(
            // maybe nodes should also be registered/deregistered via REST?
            Some(node) => {
                node.write().await.set_status(NodeStatus::Unknown);
            }
        }
    }

    /// Get nodes by filter
    pub(crate) async fn get_nodes(&self, request: &GetNodes) -> Result<Nodes, SvcError> {
        match request.filter() {
            Filter::None => {
                let node_states = self.registry.get_node_states().await;
                let node_specs = self.specs().get_nodes();
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
                let node_spec = self.specs().get_node(node_id).ok();
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

        let grpc = node.read().await.grpc_context()?;
        let mut client = grpc.connect().await?;

        let result = client
            .io_engine
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

    async fn cordon(&self, id: NodeId, label: String) -> Result<Node, SvcError> {
        let spec = self
            .registry
            .specs()
            .cordon_node(&self.registry, &id, label)
            .await?;
        let state = self.registry.get_node_state(&id).await.ok();
        Ok(Node::new(id, Some(spec), state))
    }

    async fn uncordon(&self, id: NodeId, label: String) -> Result<Node, SvcError> {
        let spec = self
            .registry
            .specs()
            .uncordon_node(&self.registry, &id, label)
            .await?;
        let state = self.registry.get_node_state(&id).await.ok();
        Ok(Node::new(id, Some(spec), state))
    }
}
