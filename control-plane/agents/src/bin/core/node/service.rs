use super::*;
use crate::controller::{
    reconciler::PollTriggerEvent,
    registry::Registry,
    resources::{
        operations::{ResourceCordon, ResourceDrain, ResourceLabel},
        operations_helper::ResourceSpecsLocked,
    },
    wrapper::NodeWrapper,
};
use agents::errors::SvcError;
use stor_port::types::v0::transport::{
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
    /// Deadline for receiving keepalive/Register messages.
    deadline: std::time::Duration,
    /// Node communication timeouts.
    comms_timeouts: NodeCommsTimeout,
}

/// Node communication Timeouts for establishing the connection to a node and
/// the request itself.
#[derive(Debug, Clone)]
pub(crate) struct NodeCommsTimeout {
    /// Timeout options.
    opts: TimeoutOptions,
}

impl NodeCommsTimeout {
    /// Return a new `Self` with the connect and request timeouts.
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
    /// Timeout to establish connection to the node.
    pub(crate) fn connect(&self) -> std::time::Duration {
        self.opts.connect_timeout()
    }
    /// Timeout for the request itself.
    pub(crate) fn request(&self) -> std::time::Duration {
        self.opts.base_timeout()
    }
    /// Timeout opts.
    pub(crate) fn opts(&self) -> &TimeoutOptions {
        &self.opts
    }
}

#[tonic::async_trait]
impl NodeOperations for Service {
    /// Get the nodes as specified in the filter.
    async fn get(
        &self,
        filter: Filter,
        ignore_notfound: bool,
        _ctx: Option<Context>,
    ) -> Result<Nodes, ReplyError> {
        let req = GetNodes::new(filter, ignore_notfound);
        let nodes = self.get_nodes(&req).await?;
        Ok(nodes)
    }
    /// Handle probe requests.
    async fn probe(&self, _ctx: Option<Context>) -> Result<bool, ReplyError> {
        return Ok(true);
    }

    /// Get block devices from the node specified in the request.
    async fn get_block_devices(
        &self,
        get_blockdevice: &dyn GetBlockDeviceInfo,
        _ctx: Option<Context>,
    ) -> Result<BlockDevices, ReplyError> {
        let req = get_blockdevice.into();
        let blockdevices = self.get_block_devices(&req).await?;
        Ok(blockdevices)
    }

    /// Cordon the specified node.
    async fn cordon(&self, id: NodeId, label: String) -> Result<Node, ReplyError> {
        let node = self.cordon(id, label).await?;
        Ok(node)
    }

    /// Uncordon the specified node.
    async fn uncordon(&self, id: NodeId, label: String) -> Result<Node, ReplyError> {
        let node = self.uncordon(id, label).await?;
        Ok(node)
    }

    /// Apply a drain label to the specified node. The reconciler will perform the drain.
    async fn drain(&self, id: NodeId, label: String) -> Result<Node, ReplyError> {
        let node = self.drain(id, label).await?;
        Ok(node)
    }

    /// Apply the label to node.
    async fn label(
        &self,
        id: NodeId,
        label: HashMap<String, String>,
        overwrite: bool,
    ) -> Result<Node, ReplyError> {
        let node = self.label(id, label, overwrite).await?;
        Ok(node)
    }
    /// Remove the specified label key from the node.
    async fn unlabel(&self, id: NodeId, label_key: String) -> Result<Node, ReplyError> {
        if label_key.is_empty() {
            return Err(SvcError::InvalidLabel {
                labels: label_key,
                resource_kind: ResourceKind::Node,
            }
            .into());
        }
        let node = self.unlabel(id, label_key).await?;
        Ok(node)
    }
}

#[tonic::async_trait]
impl RegistrationOperations for Service {
    /// Register the node as specified in the request.
    async fn register(&self, req: &dyn RegisterInfo) -> Result<(), ReplyError> {
        let register = req.try_into()?;
        let service = self.clone();
        Context::spawn(async move { service.register(&register).await }).await?;
        Ok(())
    }

    /// Deregister the node as specified in the request.
    async fn deregister(&self, req: &dyn DeregisterInfo) -> Result<(), ReplyError> {
        let deregister = req.into();
        let service = self.clone();
        Context::spawn(async move { service.deregister(&deregister).await }).await?;
        Ok(())
    }
}

impl Service {
    /// New Node Service which uses the `registry` as its node cache and sets
    /// the `deadline` to each node's watchdog.
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
        for node in service.registry.specs().nodes() {
            service
                .register_state(
                    &Register {
                        id: node.id().clone(),
                        grpc_endpoint: node.endpoint(),
                        api_versions: None,
                        instance_uuid: None,
                        node_nqn: node.node_nqn().clone(),
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

    /// Callback to be called when a node's watchdog times out.
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

    /// Register a new node through the register information.
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
                let ha_disabled = self.registry.ha_disabled();
                let mut node = NodeWrapper::new(
                    &node_state,
                    self.deadline,
                    self.comms_timeouts.clone(),
                    ha_disabled,
                );

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

    /// Deregister a node through the deregister information.
    #[tracing::instrument(level = "debug", skip(self), fields(node.id = %node.id))]
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

    /// Get nodes by filter.
    pub(crate) async fn get_nodes(&self, request: &GetNodes) -> Result<Nodes, SvcError> {
        match request.filter() {
            Filter::None => {
                let node_states = self.registry.node_states().await;
                let node_specs = self.specs().nodes();
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
                let node_state = self.registry.node_state(node_id).await.ok();
                let node_spec = self.specs().node(node_id).ok();
                if node_state.is_none() && node_spec.is_none() {
                    if request.ignore_notfound() {
                        Ok(Nodes::default())
                    } else {
                        Err(SvcError::NodeNotFound {
                            node_id: node_id.to_owned(),
                        })
                    }
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

    /// Get block devices from a node.
    pub(crate) async fn get_block_devices(
        &self,
        request: &GetBlockDevices,
    ) -> Result<BlockDevices, SvcError> {
        let node = self.registry.node_wrapper(&request.node).await?;

        let grpc = node.read().await.grpc_context()?;
        let client = grpc.connect().await?;
        client.list_blockdevices(request).await
    }

    /// Cordon the specified node.
    async fn cordon(&self, id: NodeId, label: String) -> Result<Node, SvcError> {
        let mut guarded_node = self.specs().guarded_node(&id).await?;

        let spec = guarded_node.cordon(&self.registry, label).await?;
        let state = self.registry.node_state(&id).await.ok();
        Ok(Node::new(id, Some(spec), state))
    }

    /// Uncordon the specified node.
    async fn uncordon(&self, id: NodeId, label: String) -> Result<Node, SvcError> {
        let mut guarded_node = self.specs().guarded_node(&id).await?;

        let spec = guarded_node.uncordon(&self.registry, label).await?;
        let state = self.registry.node_state(&id).await.ok();
        Ok(Node::new(id, Some(spec), state))
    }

    /// Apply a drain label to the specified node. The reconciler will perform the drain.
    async fn drain(&self, id: NodeId, label: String) -> Result<Node, SvcError> {
        // Don't allow draining if HA_ENABLED is false. If it is undefined we treat it as true.
        if self.registry.ha_disabled() {
            return Err(SvcError::DrainNotAllowedWhenHAisDisabled {});
        }

        let mut guarded_node = self.specs().guarded_node(&id).await?;

        let spec = guarded_node.drain(&self.registry, label.clone()).await?;
        let state = self.registry.node_state(&id).await.ok();

        self.registry.notify(PollTriggerEvent::NodeDrain).await;

        Ok(Node::new(id, Some(spec), state))
    }

    /// Label the specified node.
    async fn label(
        &self,
        id: NodeId,
        label: HashMap<String, String>,
        overwrite: bool,
    ) -> Result<Node, SvcError> {
        let mut guarded_node = self.specs().guarded_node(&id).await?;
        let spec = guarded_node.label(&self.registry, label, overwrite).await?;
        let state = self.registry.node_state(&id).await.ok();
        Ok(Node::new(id, Some(spec), state))
    }

    /// Remove the specified label from  the specified node.
    async fn unlabel(&self, id: NodeId, label: String) -> Result<Node, SvcError> {
        let mut guarded_node = self.specs().guarded_node(&id).await?;
        let spec = guarded_node.unlabel(&self.registry, label).await?;
        let state = self.registry.node_state(&id).await.ok();
        Ok(Node::new(id, Some(spec), state))
    }
}
