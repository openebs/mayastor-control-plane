use super::{super::node::watchdog::Watchdog, grpc::GrpcContext};
use crate::{
    core::{
        grpc::{GrpcClient, GrpcClientLocked},
        states::{ResourceStates, ResourceStatesLocked},
    },
    node::service::NodeCommsTimeout,
};

use common::{
    errors::{GrpcRequestError, SvcError},
    v0::msg_translation::{MessageBusToRpc, RpcToMessageBus, TryRpcToMessageBus},
};
use common_lib::{
    mbus_api::{Message, MessageId, MessageIdTimeout, ResourceKind},
    types::v0::{
        message_bus::{
            AddNexusChild, Child, CreateNexus, CreatePool, CreateReplica, DestroyNexus,
            DestroyPool, DestroyReplica, MessageIdVs, Nexus, NexusId, NodeId, NodeState,
            NodeStatus, PoolId, PoolState, PoolStatus, Protocol, RemoveNexusChild, Replica,
            ReplicaId, ShareNexus, ShareReplica, UnshareNexus, UnshareReplica,
        },
        store,
        store::{nexus::NexusState, replica::ReplicaState},
    },
};

use async_trait::async_trait;
use rpc::mayastor::Null;
use snafu::ResultExt;
use std::{
    cmp::Ordering,
    ops::{Deref, DerefMut},
    sync::Arc,
};

type NodeResourceStates = (Vec<Replica>, Vec<PoolState>, Vec<Nexus>);
/// Default timeout for GET* gRPC requests (ex: GetPools, GetNexuses, etc..)
const GETS_TIMEOUT: MessageIdVs = MessageIdVs::Default;

/// Wrapper over a `Node` plus a few useful methods/properties. Includes:
/// all pools and replicas from the node
/// a watchdog to keep track of the node's liveness
/// a lock to serialize mutating gRPC calls
/// The Node may still be considered online even when the watchdog times out if it still is
/// responding to gRPC liveness probes.
#[derive(Debug, Clone)]
pub(crate) struct NodeWrapper {
    /// inner Node state
    node_state: NodeState,
    /// watchdog to track the node state
    watchdog: Watchdog,
    /// indicates whether the node has already missed its deadline and in such case we don't
    /// need to keep posting duplicate error events
    missed_deadline: bool,
    /// gRPC CRUD lock
    lock: Arc<tokio::sync::Mutex<()>>,
    /// node communication timeouts
    comms_timeouts: NodeCommsTimeout,
    /// runtime state information
    states: ResourceStatesLocked,
}

impl NodeWrapper {
    /// Create a new wrapper for a `Node` with a `deadline` for its watchdog
    pub(crate) fn new(
        node: &NodeState,
        deadline: std::time::Duration,
        comms_timeouts: NodeCommsTimeout,
    ) -> Self {
        tracing::debug!("Creating new node {:?}", node);
        Self {
            node_state: node.clone(),
            watchdog: Watchdog::new(&node.id, deadline),
            missed_deadline: false,
            lock: Default::default(),
            comms_timeouts,
            states: ResourceStatesLocked::new(),
        }
    }

    /// Get `GrpcClient` for this node
    async fn grpc_client(&self) -> Result<GrpcClient, SvcError> {
        GrpcClient::new(&self.grpc_context()?).await
    }

    /// Get `GrpcClient` for this node, and specify the comms timeout
    async fn grpc_client_timeout(&self, timeout: NodeCommsTimeout) -> Result<GrpcClient, SvcError> {
        GrpcClient::new(&self.grpc_context_timeout(timeout)?).await
    }

    /// Get `GrpcContext` for this node
    /// It will be used to execute the `request` operation
    pub(crate) fn grpc_context_ext(
        &self,
        request: impl MessageIdTimeout,
    ) -> Result<GrpcContext, SvcError> {
        GrpcContext::new(
            self.lock.clone(),
            self.id(),
            &self.endpoint_str(),
            &self.comms_timeouts,
            Some(request),
        )
    }

    /// Get `GrpcContext` for this node using the specified timeout
    pub(crate) fn grpc_context_timeout(
        &self,
        timeout: NodeCommsTimeout,
    ) -> Result<GrpcContext, SvcError> {
        GrpcContext::new(
            self.lock.clone(),
            self.id(),
            &self.endpoint_str(),
            &timeout,
            None::<MessageId>,
        )
    }

    /// Get `GrpcContext` for this node
    pub(crate) fn grpc_context(&self) -> Result<GrpcContext, SvcError> {
        GrpcContext::new(
            self.lock.clone(),
            self.id(),
            &self.endpoint_str(),
            &self.comms_timeouts,
            None::<MessageId>,
        )
    }

    /// Get the `NodeStateFetcher` to fetch information from the data-plane.
    pub(crate) fn fetcher(&self) -> NodeStateFetcher {
        NodeStateFetcher::new(self.node_state.clone())
    }

    /// Whether the watchdog deadline has expired
    pub(crate) fn registration_expired(&self) -> bool {
        self.watchdog.timestamp().elapsed() > self.watchdog.deadline()
    }

    /// "Pet" the node to meet the node's watchdog timer deadline
    pub(crate) async fn pet(&mut self) {
        self.watchdog.pet().await.ok();
        if self.missed_deadline {
            tracing::info!(node.uuid=%self.id(), "The node had missed the heartbeat deadline but it's now re-registered itself");
        }
        self.missed_deadline = false;
    }

    /// Update the node liveness if the watchdog's registration expired
    /// If the node is still responding to gRPC then consider it as online and reset the watchdog.
    pub(crate) async fn update_liveness(&mut self) {
        if self.registration_expired() {
            if !self.missed_deadline {
                tracing::error!(
                    "Node id '{}' missed the registration deadline of {:?}",
                    self.id(),
                    self.watchdog.deadline()
                );
            }

            if self.is_online()
                && self.liveness_probe().await.is_ok()
                && self.watchdog.pet().await.is_ok()
            {
                if !self.missed_deadline {
                    tracing::warn!(node.uuid=%self.id(), "The node missed the heartbeat deadline but it's still responding to gRPC so we're considering it online");
                }
            } else {
                if self.missed_deadline {
                    tracing::error!(
                        "Node id '{}' missed the registration deadline of {:?}",
                        self.id(),
                        self.watchdog.deadline()
                    );
                }
                self.set_status(NodeStatus::Offline);
            }
            self.missed_deadline = true;
        }
    }

    /// Probe the node for liveness
    pub(crate) async fn liveness_probe(&mut self) -> Result<(), SvcError> {
        // use the connect timeout for liveness
        let timeouts =
            NodeCommsTimeout::new(self.comms_timeouts.connect(), self.comms_timeouts.connect());

        let mut ctx = self.grpc_client_timeout(timeouts).await?;
        let _ = ctx
            .mayastor
            .get_mayastor_info(rpc::mayastor::Null {})
            .await
            .map_err(|_| SvcError::NodeNotOnline {
                node: self.id().to_owned(),
            })?;
        Ok(())
    }

    /// Set the node status and return the previous status
    pub(crate) fn set_status(&mut self, next: NodeStatus) -> NodeStatus {
        let previous = self.status();
        if previous != next {
            if next == NodeStatus::Online {
                tracing::info!(
                    "Node '{}' changing from {} to {}",
                    self.id(),
                    previous.to_string(),
                    next.to_string(),
                );
            } else {
                tracing::warn!(
                    "Node '{}' changing from {} to {}",
                    self.id(),
                    previous.to_string(),
                    next.to_string(),
                );
            }

            self.node_state.status = next;
            if self.node_state.status == NodeStatus::Unknown {
                self.watchdog_mut().disarm()
            }
        }
        // Clear the states, otherwise we could temporarily return pools/nexus as online, even
        // though we report the node otherwise.
        // We take the approach that no information is better than inconsistent information.
        if !self.is_online() {
            self.clear_states();
        }
        previous
    }

    /// Clear all states from the node
    fn clear_states(&mut self) {
        self.resources_mut().clear_all();
    }

    /// Get the inner states
    fn resources(&self) -> parking_lot::RwLockReadGuard<ResourceStates> {
        self.states.read()
    }

    /// Get the inner resource states
    fn resources_mut(&self) -> parking_lot::RwLockWriteGuard<ResourceStates> {
        self.states.write()
    }

    /// Get a mutable reference to the node's watchdog
    pub(crate) fn watchdog_mut(&mut self) -> &mut Watchdog {
        &mut self.watchdog
    }
    /// Get the inner node
    pub(crate) fn node_state(&self) -> &NodeState {
        &self.node_state
    }
    /// Get the node `NodeId`
    pub(crate) fn id(&self) -> &NodeId {
        self.node_state().id()
    }
    /// Get the node `NodeStatus`
    pub(crate) fn status(&self) -> NodeStatus {
        self.node_state().status().clone()
    }

    /// Get the node grpc endpoint as string.
    pub(crate) fn endpoint_str(&self) -> String {
        self.node_state().grpc_endpoint.clone()
    }
    /// Get all pools
    pub(crate) fn pools(&self) -> Vec<PoolState> {
        self.resources()
            .get_pool_states()
            .iter()
            .map(|p| p.pool.clone())
            .collect()
    }
    /// Get all pool wrappers
    pub(crate) fn pool_wrappers(&self) -> Vec<PoolWrapper> {
        let pools = self.resources().get_pool_states();
        let replicas = self.resources().get_replica_states();
        pools
            .into_iter()
            .map(|p| {
                let replicas = replicas
                    .iter()
                    .filter(|r| r.replica.pool == p.pool.id)
                    .map(|r| r.replica.clone())
                    .collect::<Vec<Replica>>();
                PoolWrapper::new(&p.pool, &replicas)
            })
            .collect()
    }
    /// Get all pool states
    pub(crate) fn pool_states(&self) -> Vec<store::pool::PoolState> {
        self.resources().get_pool_states()
    }
    /// Get pool from `pool_id` or None
    pub(crate) fn pool(&self, pool_id: &PoolId) -> Option<PoolState> {
        self.resources().get_pool_state(pool_id).map(|p| p.pool)
    }
    /// Get a PoolWrapper for the pool ID.
    pub(crate) fn pool_wrapper(&self, pool_id: &PoolId) -> Option<PoolWrapper> {
        let r = self.resources();
        match r.get_pool_states().iter().find(|p| &p.pool.id == pool_id) {
            Some(pool_state) => {
                let replicas: Vec<Replica> = self
                    .replicas()
                    .into_iter()
                    .filter(|r| &r.pool == pool_id)
                    .collect();
                Some(PoolWrapper::new(&pool_state.pool, &replicas))
            }
            None => None,
        }
    }
    /// Get all replicas
    pub(crate) fn replicas(&self) -> Vec<Replica> {
        self.resources()
            .get_replica_states()
            .iter()
            .map(|r| r.replica.clone())
            .collect()
    }
    /// Get all replica states
    pub(crate) fn replica_states(&self) -> Vec<ReplicaState> {
        self.resources().get_replica_states()
    }
    /// Get all nexuses
    fn nexuses(&self) -> Vec<Nexus> {
        self.resources()
            .get_nexus_states()
            .iter()
            .map(|nexus_state| nexus_state.nexus.clone())
            .collect()
    }
    /// Get all nexus states
    pub(crate) fn nexus_states(&self) -> Vec<NexusState> {
        self.resources().get_nexus_states()
    }
    /// Get nexus
    fn nexus(&self, nexus_id: &NexusId) -> Option<Nexus> {
        self.resources().get_nexus_state(nexus_id).map(|s| s.nexus)
    }
    /// Get replica from `replica_id`
    pub(crate) fn replica(&self, replica_id: &ReplicaId) -> Option<Replica> {
        self.resources()
            .get_replica_state(replica_id)
            .map(|r| r.replica)
    }
    /// Is the node online
    pub(crate) fn is_online(&self) -> bool {
        self.status() == NodeStatus::Online
    }

    /// Load the node by fetching information from mayastor
    pub(crate) async fn load(&mut self) -> Result<(), SvcError> {
        tracing::info!(
            "Preloading node '{}' on endpoint '{}'",
            self.id(),
            self.endpoint_str()
        );

        let mut client = self.grpc_client().await?;
        match self.fetcher().fetch_resources(&mut client).await {
            Ok((replicas, pools, nexuses)) => {
                let mut states = self.resources_mut();
                states.update(pools, replicas, nexuses);
                Ok(())
            }
            Err(error) => {
                self.set_status(NodeStatus::Unknown);
                tracing::error!(
                    "Preloading of node '{}' on endpoint '{}' failed with error: {:?}",
                    self.id(),
                    self.endpoint_str(),
                    error
                );
                Err(error)
            }
        }
    }

    /// Update the node by updating its state from the states fetched from mayastor
    fn update(
        &mut self,
        setting_online: bool,
        fetch_result: Result<NodeResourceStates, SvcError>,
    ) -> Result<(), SvcError> {
        if self.is_online() || setting_online {
            tracing::trace!(
                "Reloading node '{}' on endpoint '{}'",
                self.id(),
                self.endpoint_str()
            );

            match fetch_result {
                Ok((replicas, pools, nexuses)) => {
                    self.resources_mut().update(pools, replicas, nexuses);
                    if setting_online {
                        // we only set it as online after we've updated the resource states
                        // so an online node should be "up-to-date"
                        self.set_status(NodeStatus::Online);
                    }
                    Ok(())
                }
                Err(error) => {
                    self.set_status(NodeStatus::Unknown);
                    tracing::trace!("Failed to reload node {}. Error {:?}.", self.id(), error);
                    Err(error)
                }
            }
        } else {
            tracing::trace!(
                "Skipping reload of node '{}' since it's '{:?}'",
                self.id(),
                self.node_state()
            );
            // should already be cleared
            self.clear_states();
            Err(SvcError::NodeNotOnline {
                node: self.id().to_owned(),
            })
        }
    }

    /// Update all the nexus states.
    async fn update_nexus_states(
        node: &Arc<tokio::sync::RwLock<NodeWrapper>>,
        client: &mut GrpcClient,
    ) -> Result<(), SvcError> {
        let nexuses = node.read().await.fetcher().fetch_nexuses(client).await?;
        node.write().await.resources_mut().update_nexuses(nexuses);
        Ok(())
    }

    /// Update all the pool states.
    async fn update_pool_states(
        node: &Arc<tokio::sync::RwLock<NodeWrapper>>,
        client: &mut GrpcClient,
    ) -> Result<(), SvcError> {
        let pools = node.read().await.fetcher().fetch_pools(client).await?;
        node.write().await.resources_mut().update_pools(pools);
        Ok(())
    }

    /// Update all the replica states.
    async fn update_replica_states(
        node: &Arc<tokio::sync::RwLock<NodeWrapper>>,
        client: &mut GrpcClient,
    ) -> Result<(), SvcError> {
        let replicas = node.read().await.fetcher().fetch_replicas(client).await?;
        node.write().await.resources_mut().update_replicas(replicas);
        Ok(())
    }
}

/// Fetches node state from the dataplane.
#[derive(Debug, Clone)]
pub(crate) struct NodeStateFetcher {
    /// inner Node state
    node_state: NodeState,
}
impl NodeStateFetcher {
    /// Get new `Self` from the `NodeState`.
    fn new(node_state: NodeState) -> Self {
        Self { node_state }
    }
    fn id(&self) -> &NodeId {
        self.node_state.id()
    }
    /// Fetch the various resources from Mayastor.
    async fn fetch_resources(
        &self,
        client: &mut GrpcClient,
    ) -> Result<NodeResourceStates, SvcError> {
        let replicas = self.fetch_replicas(client).await?;
        let pools = self.fetch_pools(client).await?;
        let nexuses = self.fetch_nexuses(client).await?;
        Ok((replicas, pools, nexuses))
    }

    /// Fetch all replicas from this node via gRPC
    pub(crate) async fn fetch_replicas(
        &self,
        client: &mut GrpcClient,
    ) -> Result<Vec<Replica>, SvcError> {
        let rpc_replicas =
            client
                .mayastor
                .list_replicas_v2(Null {})
                .await
                .context(GrpcRequestError {
                    resource: ResourceKind::Replica,
                    request: "list_replicas",
                })?;

        let rpc_replicas = &rpc_replicas.get_ref().replicas;
        let pools = rpc_replicas
            .iter()
            .filter_map(|p| match rpc_replica_to_bus(p, self.id()) {
                Ok(r) => Some(r),
                Err(error) => {
                    tracing::error!(error=%error, "Could not convert rpc replica");
                    None
                }
            })
            .collect();
        Ok(pools)
    }
    /// Fetch all pools from this node via gRPC
    pub(crate) async fn fetch_pools(
        &self,
        client: &mut GrpcClient,
    ) -> Result<Vec<PoolState>, SvcError> {
        let rpc_pools = client
            .mayastor
            .list_pools(Null {})
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Pool,
                request: "list_pools",
            })?;
        let rpc_pools = &rpc_pools.get_ref().pools;
        let pools = rpc_pools
            .iter()
            .map(|p| rpc_pool_to_bus(p, self.id()))
            .collect();
        Ok(pools)
    }
    /// Fetch all nexuses from the node via gRPC
    pub(crate) async fn fetch_nexuses(
        &self,
        client: &mut GrpcClient,
    ) -> Result<Vec<Nexus>, SvcError> {
        let rpc_nexuses =
            client
                .mayastor
                .list_nexus_v2(Null {})
                .await
                .context(GrpcRequestError {
                    resource: ResourceKind::Nexus,
                    request: "list_nexus",
                })?;
        let rpc_nexuses = &rpc_nexuses.get_ref().nexus_list;
        let nexuses = rpc_nexuses
            .iter()
            .filter_map(|n| match rpc_nexus_v2_to_bus(n, self.id()) {
                Ok(n) => Some(n),
                Err(error) => {
                    tracing::error!(error=%error, "Could not convert rpc nexus");
                    None
                }
            })
            .collect();
        Ok(nexuses)
    }
}

/// CRUD Operations on a locked mayastor `NodeWrapper` such as:
/// pools, replicas, nexuses and their children
#[async_trait]
pub(crate) trait ClientOps {
    /// Get the grpc lock and client pair to execute the provided `request`
    /// NOTE: Only available when the node status is online
    async fn grpc_client_locked<T: MessageIdTimeout>(
        &self,
        request: T,
    ) -> Result<GrpcClientLocked, SvcError>;
    /// Create a pool on the node via gRPC
    async fn create_pool(&self, request: &CreatePool) -> Result<PoolState, SvcError>;
    /// Destroy a pool on the node via gRPC
    async fn destroy_pool(&self, request: &DestroyPool) -> Result<(), SvcError>;
    /// Create a replica on the pool via gRPC
    async fn create_replica(&self, request: &CreateReplica) -> Result<Replica, SvcError>;
    /// Share a replica on the pool via gRPC
    async fn share_replica(&self, request: &ShareReplica) -> Result<String, SvcError>;
    /// Unshare a replica on the pool via gRPC
    async fn unshare_replica(&self, request: &UnshareReplica) -> Result<String, SvcError>;
    /// Destroy a replica on the pool via gRPC
    async fn destroy_replica(&self, request: &DestroyReplica) -> Result<(), SvcError>;

    /// Create a nexus on a node via gRPC or MBUS
    async fn create_nexus(&self, request: &CreateNexus) -> Result<Nexus, SvcError>;
    /// Destroy a nexus on a node via gRPC or MBUS
    async fn destroy_nexus(&self, request: &DestroyNexus) -> Result<(), SvcError>;
    /// Share a nexus on the node via gRPC
    async fn share_nexus(&self, request: &ShareNexus) -> Result<String, SvcError>;
    /// Unshare a nexus on the node via gRPC
    async fn unshare_nexus(&self, request: &UnshareNexus) -> Result<(), SvcError>;
    /// Add a child to a nexus via gRPC
    async fn add_child(&self, request: &AddNexusChild) -> Result<Child, SvcError>;
    /// Remove a child from its parent nexus via gRPC
    async fn remove_child(&self, request: &RemoveNexusChild) -> Result<(), SvcError>;
}

/// Internal Operations on a mayastor locked `NodeWrapper` for the implementor
/// of the `ClientOps` trait and the `Registry` itself
#[async_trait]
pub(crate) trait InternalOps {
    /// Get the inner lock, typically used to sync mutating gRPC operations
    async fn grpc_lock(&self) -> Arc<tokio::sync::Mutex<()>>;
    /// Update the node's nexus state information
    async fn update_nexus_states(&self, mut ctx: &mut GrpcClient) -> Result<(), SvcError>;
    /// Update the node's pool state information
    async fn update_pool_states(&self, mut ctx: &mut GrpcClient) -> Result<(), SvcError>;
    /// Update the node's replica state information
    async fn update_replica_states(&self, mut ctx: &mut GrpcClient) -> Result<(), SvcError>;
    /// Update all node state information
    async fn update_all(&self, setting_online: bool) -> Result<(), SvcError>;
    /// OnRegister callback when a node is re-registered with the registry via its heartbeat
    /// On success returns where it's reset the node as online or not.
    async fn on_register(&self) -> Result<bool, SvcError>;
}

/// Getter operations on a mayastor locked `NodeWrapper` to get copies of its
/// resources, such as pools, replicas and nexuses
#[async_trait]
pub(crate) trait GetterOps {
    async fn pools(&self) -> Vec<PoolState>;
    async fn pool_wrappers(&self) -> Vec<PoolWrapper>;
    async fn pool(&self, pool_id: &PoolId) -> Option<PoolState>;
    async fn pool_wrapper(&self, pool_id: &PoolId) -> Option<PoolWrapper>;

    async fn replicas(&self) -> Vec<Replica>;
    async fn replica(&self, replica: &ReplicaId) -> Option<Replica>;

    async fn nexuses(&self) -> Vec<Nexus>;
    async fn nexus(&self, nexus_id: &NexusId) -> Option<Nexus>;
}

#[async_trait]
impl GetterOps for Arc<tokio::sync::RwLock<NodeWrapper>> {
    async fn pools(&self) -> Vec<PoolState> {
        let node = self.read().await;
        node.pools()
    }
    async fn pool_wrappers(&self) -> Vec<PoolWrapper> {
        let node = self.read().await;
        node.pool_wrappers()
    }
    async fn pool(&self, pool_id: &PoolId) -> Option<PoolState> {
        let node = self.read().await;
        node.pool(pool_id)
    }
    async fn pool_wrapper(&self, pool_id: &PoolId) -> Option<PoolWrapper> {
        let node = self.read().await;
        node.pool_wrapper(pool_id)
    }
    async fn replicas(&self) -> Vec<Replica> {
        let node = self.read().await;
        node.replicas()
    }
    async fn replica(&self, replica: &ReplicaId) -> Option<Replica> {
        let node = self.read().await;
        node.replica(replica)
    }
    async fn nexuses(&self) -> Vec<Nexus> {
        let node = self.read().await;
        node.nexuses()
    }
    async fn nexus(&self, nexus_id: &NexusId) -> Option<Nexus> {
        let node = self.read().await;
        node.nexus(nexus_id)
    }
}

#[async_trait]
impl InternalOps for Arc<tokio::sync::RwLock<NodeWrapper>> {
    async fn grpc_lock(&self) -> Arc<tokio::sync::Mutex<()>> {
        self.write().await.lock.clone()
    }

    async fn update_nexus_states(&self, mut ctx: &mut GrpcClient) -> Result<(), SvcError> {
        NodeWrapper::update_nexus_states(self, ctx.deref_mut()).await
    }

    async fn update_pool_states(&self, mut ctx: &mut GrpcClient) -> Result<(), SvcError> {
        NodeWrapper::update_pool_states(self, ctx.deref_mut()).await
    }

    async fn update_replica_states(&self, mut ctx: &mut GrpcClient) -> Result<(), SvcError> {
        NodeWrapper::update_replica_states(self, ctx.deref_mut()).await
    }

    async fn update_all(&self, setting_online: bool) -> Result<(), SvcError> {
        let ctx = self.read().await.grpc_context_ext(GETS_TIMEOUT)?;
        match ctx.connect_locked().await {
            Ok(mut lock) => {
                let node_fetcher = self.read().await.fetcher();
                let results = node_fetcher.fetch_resources(lock.deref_mut()).await;

                let mut node = self.write().await;
                node.update(setting_online, results)
            }
            Err((_guard, error)) => {
                self.write().await.set_status(NodeStatus::Unknown);
                Err(error)
            }
        }
    }

    async fn on_register(&self) -> Result<bool, SvcError> {
        let setting_online = {
            let mut node = self.write().await;
            node.pet().await;
            !node.is_online()
        };
        // if the node was not previously online then let's update all states right away
        if setting_online {
            self.update_all(setting_online).await.map(|_| true)
        } else {
            Ok(false)
        }
    }
}

#[async_trait]
impl ClientOps for Arc<tokio::sync::RwLock<NodeWrapper>> {
    async fn grpc_client_locked<T: MessageIdTimeout>(
        &self,
        request: T,
    ) -> Result<GrpcClientLocked, SvcError> {
        if !self.read().await.is_online() {
            return Err(SvcError::NodeNotOnline {
                node: self.read().await.id().clone(),
            });
        }
        let ctx = self.read().await.grpc_context_ext(request)?;
        ctx.connect_locked().await.map_err(|(_, error)| error)
    }

    async fn create_pool(&self, request: &CreatePool) -> Result<PoolState, SvcError> {
        let mut ctx = self.grpc_client_locked(request.id()).await?;
        let rpc_pool =
            ctx.mayastor
                .create_pool(request.to_rpc())
                .await
                .context(GrpcRequestError {
                    resource: ResourceKind::Pool,
                    request: "create_pool",
                })?;
        let pool = rpc_pool_to_bus(&rpc_pool.into_inner(), &request.node);
        let mut ctx = ctx.reconnect(GETS_TIMEOUT).await?;
        self.update_pool_states(ctx.deref_mut()).await?;
        self.update_replica_states(ctx.deref_mut()).await?;
        Ok(pool)
    }
    /// Destroy a pool on the node via gRPC
    async fn destroy_pool(&self, request: &DestroyPool) -> Result<(), SvcError> {
        let mut ctx = self.grpc_client_locked(request.id()).await?;
        let _ = ctx
            .mayastor
            .destroy_pool(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Pool,
                request: "destroy_pool",
            })?;
        let mut ctx = ctx.reconnect(GETS_TIMEOUT).await?;
        self.update_pool_states(ctx.deref_mut()).await?;
        Ok(())
    }

    /// Create a replica on the pool via gRPC
    async fn create_replica(&self, request: &CreateReplica) -> Result<Replica, SvcError> {
        if request.uuid == ReplicaId::default() {
            return Err(SvcError::InvalidUuid {
                uuid: request.uuid.to_string(),
                kind: ResourceKind::Replica,
            });
        }
        let mut ctx = self.grpc_client_locked(request.id()).await?;
        let rpc_replica = ctx
            .mayastor
            .create_replica_v2(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Replica,
                request: "create_replica",
            })?;

        let replica = rpc_replica_to_bus(&rpc_replica.into_inner(), &request.node)?;
        let mut ctx = ctx.reconnect(GETS_TIMEOUT).await?;
        self.update_replica_states(ctx.deref_mut()).await?;
        self.update_pool_states(ctx.deref_mut()).await?;
        Ok(replica)
    }

    /// Share a replica on the pool via gRPC
    async fn share_replica(&self, request: &ShareReplica) -> Result<String, SvcError> {
        let mut ctx = self.grpc_client_locked(request.id()).await?;
        let share = ctx
            .mayastor
            .share_replica(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Replica,
                request: "share_replica",
            })?
            .into_inner()
            .uri;
        let mut ctx = ctx.reconnect(GETS_TIMEOUT).await?;
        self.update_replica_states(ctx.deref_mut()).await?;
        Ok(share)
    }

    /// Unshare a replica on the pool via gRPC
    async fn unshare_replica(&self, request: &UnshareReplica) -> Result<String, SvcError> {
        let mut ctx = self.grpc_client_locked(request.id()).await?;
        let local_uri = ctx
            .mayastor
            .share_replica(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Replica,
                request: "unshare_replica",
            })?
            .into_inner()
            .uri;
        let mut ctx = ctx.reconnect(GETS_TIMEOUT).await?;
        self.update_replica_states(ctx.deref_mut()).await?;
        Ok(local_uri)
    }

    /// Destroy a replica on the pool via gRPC
    async fn destroy_replica(&self, request: &DestroyReplica) -> Result<(), SvcError> {
        let mut ctx = self.grpc_client_locked(request.id()).await?;
        let _ = ctx
            .mayastor
            .destroy_replica(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Replica,
                request: "destroy_replica",
            })?;
        let mut ctx = ctx.reconnect(GETS_TIMEOUT).await?;
        self.update_replica_states(ctx.deref_mut()).await?;
        // todo: remove when CAS-1107 is resolved
        if let Some(replica) = self.read().await.replica(&request.uuid) {
            if replica.pool == request.pool {
                return Err(SvcError::Internal {
                    details: "replica was not destroyed by mayastor".to_string(),
                });
            }
        }
        self.update_pool_states(ctx.deref_mut()).await?;
        Ok(())
    }

    /// Create a nexus on the node via gRPC
    async fn create_nexus(&self, request: &CreateNexus) -> Result<Nexus, SvcError> {
        if request.uuid == NexusId::default() {
            return Err(SvcError::InvalidUuid {
                uuid: request.uuid.to_string(),
                kind: ResourceKind::Nexus,
            });
        }
        let mut ctx = self.grpc_client_locked(request.id()).await?;
        let rpc_nexus = ctx
            .mayastor
            .create_nexus_v2(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Nexus,
                request: "create_nexus",
            })?;
        let mut nexus = rpc_nexus_to_bus(&rpc_nexus.into_inner(), &request.node)?;
        // CAS-1107 - create_nexus_v2 returns NexusV1...
        nexus.name = request.name();
        nexus.uuid = request.uuid.clone();
        let mut ctx = ctx.reconnect(GETS_TIMEOUT).await?;
        self.update_nexus_states(ctx.deref_mut()).await?;
        Ok(nexus)
    }

    /// Destroy a nexus on the node via gRPC
    async fn destroy_nexus(&self, request: &DestroyNexus) -> Result<(), SvcError> {
        let mut ctx = self.grpc_client_locked(request.id()).await?;
        let _ = ctx
            .mayastor
            .destroy_nexus(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Nexus,
                request: "destroy_nexus",
            })?;
        let mut ctx = ctx.reconnect(GETS_TIMEOUT).await?;
        self.update_nexus_states(ctx.deref_mut()).await?;
        Ok(())
    }

    /// Share a nexus on the node via gRPC
    async fn share_nexus(&self, request: &ShareNexus) -> Result<String, SvcError> {
        let mut ctx = self.grpc_client_locked(request.id()).await?;
        let share =
            ctx.mayastor
                .publish_nexus(request.to_rpc())
                .await
                .context(GrpcRequestError {
                    resource: ResourceKind::Nexus,
                    request: "publish_nexus",
                })?;
        let share = share.into_inner().device_uri;
        let mut ctx = ctx.reconnect(GETS_TIMEOUT).await?;
        self.update_nexus_states(ctx.deref_mut()).await?;
        Ok(share)
    }

    /// Unshare a nexus on the node via gRPC
    async fn unshare_nexus(&self, request: &UnshareNexus) -> Result<(), SvcError> {
        let mut ctx = self.grpc_client_locked(request.id()).await?;
        let _ = ctx
            .mayastor
            .unpublish_nexus(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Nexus,
                request: "unpublish_nexus",
            })?;
        let mut ctx = ctx.reconnect(GETS_TIMEOUT).await?;
        self.update_nexus_states(ctx.deref_mut()).await?;
        Ok(())
    }

    /// Add a child to a nexus via gRPC
    async fn add_child(&self, request: &AddNexusChild) -> Result<Child, SvcError> {
        let mut ctx = self.grpc_client_locked(request.id()).await?;
        let result = ctx.mayastor.add_child_nexus(request.to_rpc()).await;
        let mut ctx = ctx.reconnect(GETS_TIMEOUT).await?;
        self.update_nexus_states(ctx.deref_mut()).await?;
        let rpc_child = match result {
            Ok(child) => Ok(child),
            Err(error) => {
                if error.code() == tonic::Code::AlreadyExists {
                    if let Some(nexus) = self.read().await.nexus(&request.nexus) {
                        if let Some(child) = nexus.children.iter().find(|c| c.uri == request.uri) {
                            tracing::warn!(
                                "Trying to add Child '{}' which is already part of nexus '{}'. Ok",
                                request.uri,
                                request.nexus
                            );
                            return Ok(child.clone());
                        }
                    }
                }
                Err(error)
            }
        }
        .context(GrpcRequestError {
            resource: ResourceKind::Child,
            request: "add_child_nexus",
        })?;
        let child = rpc_child.into_inner().to_mbus();
        Ok(child)
    }

    /// Remove a child from its parent nexus via gRPC
    async fn remove_child(&self, request: &RemoveNexusChild) -> Result<(), SvcError> {
        let mut ctx = self.grpc_client_locked(request.id()).await?;
        let result = ctx.mayastor.remove_child_nexus(request.to_rpc()).await;

        let mut ctx = ctx.reconnect(GETS_TIMEOUT).await?;
        self.update_nexus_states(ctx.deref_mut()).await?;
        match result {
            Ok(_) => Ok(()),
            Err(error) => {
                if let Some(nexus) = self.read().await.nexus(&request.nexus) {
                    if !nexus.contains_child(&request.uri) {
                        tracing::warn!(
                            "Forgetting about Child '{}' which is no longer part of nexus '{}'",
                            request.uri,
                            request.nexus
                        );
                        return Ok(());
                    }
                }
                Err(error)
            }
        }
        .context(GrpcRequestError {
            resource: ResourceKind::Child,
            request: "remove_child_nexus",
        })
    }
}

/// convert rpc pool to a message bus pool
fn rpc_pool_to_bus(rpc_pool: &rpc::mayastor::Pool, id: &NodeId) -> PoolState {
    let mut pool = rpc_pool.to_mbus();
    pool.node = id.clone();
    pool
}

/// convert rpc replica to a message bus replica
fn rpc_replica_to_bus(
    rpc_replica: &rpc::mayastor::ReplicaV2,
    id: &NodeId,
) -> Result<Replica, SvcError> {
    let mut replica = rpc_replica.try_to_mbus()?;
    replica.node = id.clone();
    Ok(replica)
}

fn rpc_nexus_v2_to_bus(rpc_nexus: &rpc::mayastor::NexusV2, id: &NodeId) -> Result<Nexus, SvcError> {
    let mut nexus = rpc_nexus.try_to_mbus()?;
    nexus.node = id.clone();
    Ok(nexus)
}
fn rpc_nexus_to_bus(rpc_nexus: &rpc::mayastor::Nexus, id: &NodeId) -> Result<Nexus, SvcError> {
    let mut nexus = rpc_nexus.try_to_mbus()?;
    nexus.node = id.clone();
    Ok(nexus)
}

/// Wrapper over the message bus `Pool` which includes all the replicas
/// and Ord traits to aid pool selection for volume replicas
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PoolWrapper {
    state: PoolState,
    replicas: Vec<Replica>,
}

impl Deref for PoolWrapper {
    type Target = PoolState;
    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl PoolWrapper {
    /// New Pool wrapper with the pool and replicas
    pub fn new(pool: &PoolState, replicas: &[Replica]) -> Self {
        Self {
            state: pool.clone(),
            replicas: replicas.into(),
        }
    }

    /// Get all the replicas
    pub fn replicas(&self) -> Vec<Replica> {
        self.replicas.clone()
    }
    /// Get the specified replica
    pub fn replica(&self, replica: &ReplicaId) -> Option<&Replica> {
        self.replicas.iter().find(|r| &r.uuid == replica)
    }
    /// Get the state
    pub fn state(&self) -> &PoolState {
        &self.state
    }

    /// Get the free space
    pub fn free_space(&self) -> u64 {
        if self.state.capacity >= self.state.used {
            self.state.capacity - self.state.used
        } else {
            // odd, let's report no free space available
            tracing::error!(
                "Pool '{}' has a capacity of '{} B' but is using '{} B'",
                self.state.id,
                self.state.capacity,
                self.state.used
            );
            0
        }
    }

    /// Set pool state as unknown
    pub fn set_unknown(&mut self) {
        self.state.status = PoolStatus::Unknown;
    }

    /// Add replica to list
    pub fn add_replica(&mut self, replica: &Replica) {
        self.replicas.push(replica.clone())
    }
    /// Remove replica from list
    pub fn remove_replica(&mut self, uuid: &ReplicaId) {
        self.replicas.retain(|replica| &replica.uuid != uuid)
    }
    /// update replica from list
    pub fn update_replica(&mut self, uuid: &ReplicaId, share: &Protocol, uri: &str) {
        if let Some(replica) = self
            .replicas
            .iter_mut()
            .find(|replica| &replica.uuid == uuid)
        {
            replica.share = *share;
            replica.uri = uri.to_string();
        }
    }
}

impl From<&NodeWrapper> for NodeState {
    fn from(node: &NodeWrapper) -> Self {
        node.node_state().clone()
    }
}

impl From<PoolWrapper> for PoolState {
    fn from(pool: PoolWrapper) -> Self {
        pool.state
    }
}
impl From<&PoolWrapper> for PoolState {
    fn from(pool: &PoolWrapper) -> Self {
        pool.state.clone()
    }
}
impl From<PoolWrapper> for Vec<Replica> {
    fn from(pool: PoolWrapper) -> Self {
        pool.replicas
    }
}
impl From<&PoolWrapper> for Vec<Replica> {
    fn from(pool: &PoolWrapper) -> Self {
        pool.replicas.clone()
    }
}

// 1. state ( online > degraded )
// 2. smaller n replicas
// (here we should have pool IO stats over time so we can pick less active
// pools rather than the number of replicas which is useless if the volumes
// are not active)
impl PartialOrd for PoolWrapper {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.state.status.partial_cmp(&other.state.status) {
            Some(Ordering::Greater) => Some(Ordering::Greater),
            Some(Ordering::Less) => Some(Ordering::Less),
            Some(Ordering::Equal) => match self.replicas.len().cmp(&other.replicas.len()) {
                Ordering::Greater => Some(Ordering::Greater),
                Ordering::Less => Some(Ordering::Less),
                Ordering::Equal => Some(self.free_space().cmp(&other.free_space())),
            },
            None => None,
        }
    }
}

impl Ord for PoolWrapper {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.state.status.partial_cmp(&other.state.status) {
            Some(Ordering::Greater) => Ordering::Greater,
            Some(Ordering::Less) => Ordering::Less,
            Some(Ordering::Equal) => match self.replicas.len().cmp(&other.replicas.len()) {
                Ordering::Greater => Ordering::Greater,
                Ordering::Less => Ordering::Less,
                Ordering::Equal => self.free_space().cmp(&other.free_space()),
            },
            None => Ordering::Equal,
        }
    }
}
