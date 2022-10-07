use super::*;
use crate::controller::{
    reconciler::PollTriggerEvent, registry::Registry,
    resources::operations_helper::ResourceSpecsLocked, wrapper::NodeWrapper,
};
use agents::errors::SvcError;
use common_lib::types::v0::transport::{
    Deregister, Filter, Node, NodeId, NodeState, NodeStatus, Register,
};

use crate::controller::wrapper::InternalOps;
use grpc::{
    context::Context,
    operations::{
        node::traits::{GetBlockDeviceInfo, NodeOperations},
        registration::traits::{DeregisterInfo, RegisterInfo, RegistrationOperations},
    },
};
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
    /// timeout options
    opts: TimeoutOptions,
}

impl NodeCommsTimeout {
    /// return a new `Self` with the connect and request timeouts
    pub(crate) fn new(
        connect: std::time::Duration,
        request: std::time::Duration,
        no_min: bool,
    ) -> Self {
        let opts = TimeoutOptions::new()
            .with_req_timeout(request)
            .with_connect_timeout(connect)
            .with_min_req_timeout(if no_min {
                None
            } else {
                TimeoutOptions::default().request_min_timeout().cloned()
            });

        Self { opts }
    }
    /// timeout to establish connection to the node
    pub(crate) fn connect(&self) -> std::time::Duration {
        self.opts.connect_timeout()
    }
    /// timeout for the request itself
    pub(crate) fn request(&self) -> std::time::Duration {
        self.opts.base_timeout()
    }
    /// timeout opts.
    pub(crate) fn opts(&self) -> &TimeoutOptions {
        &self.opts
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

    async fn drain(&self, id: NodeId, label: String) -> Result<Node, ReplyError> {
        let node = self.drain(id, label).await?;
        Ok(node)
    }
}

#[tonic::async_trait]
impl RegistrationOperations for Service {
    async fn register(&self, req: &dyn RegisterInfo) -> Result<(), ReplyError> {
        let register = req.try_into()?;
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
        no_min: bool,
    ) -> Self {
        let service = Self {
            registry,
            deadline,
            comms_timeouts: NodeCommsTimeout::new(connect, request, no_min),
        };
        // attempt to reload the node state based on the specification
        for node in service.registry.specs().get_nodes() {
            service
                .register_state(
                    &Register {
                        id: node.id().clone(),
                        grpc_endpoint: node.endpoint(),
                        api_versions: None,
                        instance_uuid: None,
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
    #[tracing::instrument(level = "trace", skip(self), fields(node.id = %registration.id))]
    pub(super) async fn register(&self, registration: &Register) {
        self.registry.register_node_spec(registration).await;
        self.register_state(registration, false).await;
    }

    /// Attempt to Register a new node state through the register information.
    /// todo: if we enable concurrent registrations when we move to gRPC, we'll want
    /// to make sure we don't process registrations for the same node in parallel.
    pub(super) async fn register_state(&self, registration: &Register, startup: bool) {
        let node_state = NodeState::from(registration);
        let nodes = self.registry.nodes();
        let node = nodes.write().await.get_mut(&node_state.id).cloned();
        let send_event = match node {
            None => {
                let mut node =
                    NodeWrapper::new(&node_state, self.deadline, self.comms_timeouts.clone());

                // On startup api version is not known, thus probe all apiversions
                let result = match startup {
                    true => node.liveness_probe_all().await,
                    false => node.liveness_probe().await,
                };

                let result = match result {
                    Ok(mut result) => {
                        // old v0 doesn't have the instance uuid
                        if result.instance_uuid().is_none() && node_state.instance_uuid().is_some()
                        {
                            result.instance_uuid = *node_state.instance_uuid();
                        }
                        node.load(startup).await.map(|_| result)
                    }
                    Err(e) => Err(e),
                };
                match result {
                    Ok(data) => {
                        let mut nodes = self.registry.nodes().write().await;
                        if nodes.get_mut(&node_state.id).is_none() {
                            node.set_startup_creation(NodeState::from(data));
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
                            "Failed to register node state"
                        );
                        false
                    }
                }
            }
            Some(node) => matches!(node.on_register(node_state).await, Ok(true)),
        };

        // don't send these events on startup as the reconciler will start working afterwards anyway
        if send_event && !startup {
            self.registry
                .notify(PollTriggerEvent::NodeStateChangeOnline)
                .await;
        }
    }

    /// Deregister a node through the deregister information
    #[tracing::instrument(level = "trace", skip(self), fields(node.id = %node.id))]
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
        let client = grpc.connect().await?;
        client.list_blockdevices(request).await
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

    async fn drain(&self, id: NodeId, label: String) -> Result<Node, SvcError> {
        let spec = self
            .registry
            .specs()
            .drain_node(&self.registry, &id, label)
            .await?;
        let state = self.registry.get_node_state(&id).await.ok();
        Ok(Node::new(id, Some(spec), state))
    }
}
