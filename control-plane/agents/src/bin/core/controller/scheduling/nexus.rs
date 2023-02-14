use crate::controller::{
    registry::Registry,
    resources::ResourceMutex,
    scheduling::{
        resources::{ChildItem, NodeItem},
        ChildInfoFilters, ChildItemSorters, NodeFilters, NodeSorters, ReplicaFilters,
        ResourceFilter,
    },
};
use agents::errors::SvcError;
use itertools::Itertools;
use std::{collections::HashMap, ops::Deref};
use stor_port::types::v0::{
    store::{nexus::NexusSpec, nexus_persistence::NexusInfo, volume::VolumeSpec},
    transport::{ChildUri, NexusId, NodeId, VolumeId},
};

/// Request to retrieve a list of healthy nexus children which is used for nexus creation
/// used by `CreateVolumeNexus`.
#[derive(Clone)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum GetPersistedNexusChildren {
    Create((VolumeSpec, NodeId)),
    ReCreate(NexusSpec),
}

impl GetPersistedNexusChildren {
    /// Retrieve a list of children for a volume nexus creation.
    pub(crate) fn new_create(spec: &VolumeSpec, target_node: &NodeId) -> Self {
        Self::Create((spec.clone(), target_node.clone()))
    }
    /// Retrieve a list of children for a nexus REcreation.
    pub(crate) fn new_recreate(spec: &NexusSpec) -> Self {
        Self::ReCreate(spec.clone())
    }
    /// Get the optional volume spec (used for nexus creation).
    pub(crate) fn vol_spec(&self) -> Option<&VolumeSpec> {
        match self {
            Self::Create((spec, _)) => Some(spec),
            Self::ReCreate(_) => None,
        }
    }
    /// Get the target node where the nexus will be created/recreated on.
    pub(crate) fn target_node(&self) -> &NodeId {
        match self {
            Self::Create((_, node)) => node,
            Self::ReCreate(nexus) => &nexus.node,
        }
    }
    /// Get the current nexus persistent information Id.
    pub(crate) fn nexus_info_id(&self) -> Option<&NexusId> {
        match self {
            Self::Create((vol, _)) => vol.health_info_id(),
            Self::ReCreate(nexus) => Some(&nexus.uuid),
        }
    }

    /// Get the volume ID associated with the persisted nexus info.
    pub(crate) fn volume_id(&self) -> Option<&VolumeId> {
        match self {
            GetPersistedNexusChildren::Create((vol, _)) => Some(&vol.uuid),
            GetPersistedNexusChildren::ReCreate(nexus) => match nexus.owner.as_ref() {
                Some(volume_id) => Some(volume_id),
                None => None,
            },
        }
    }
}

/// `GetPersistedNexusChildren` context used by the filter fn's for `GetPersistedNexusChildren`.
#[derive(Clone)]
pub(crate) struct GetPersistedNexusChildrenCtx {
    request: GetPersistedNexusChildren,
    registry: Registry,
    nexus_info: Option<NexusInfo>,
    shutdown_failed_nexuses: Vec<ResourceMutex<NexusSpec>>,
}

impl GetPersistedNexusChildrenCtx {
    /// Get the optional volume spec (used for nexus creation).
    pub(crate) fn vol_spec(&self) -> Option<&VolumeSpec> {
        self.request.vol_spec()
    }
    /// Get the target node where the nexus will be created on.
    pub(crate) fn target_node(&self) -> &NodeId {
        self.request.target_node()
    }
    /// Get the current nexus persistent information.
    pub(crate) fn nexus_info(&self) -> &Option<NexusInfo> {
        &self.nexus_info
    }
    /// Get the pending shutdown nexuses associated with the volume.
    pub(crate) fn shutdown_failed_nexuses(&self) -> &Vec<ResourceMutex<NexusSpec>> {
        &self.shutdown_failed_nexuses
    }
}

impl GetPersistedNexusChildrenCtx {
    async fn new(
        registry: &Registry,
        request: &GetPersistedNexusChildren,
    ) -> Result<Self, SvcError> {
        let nexus_info = registry
            .nexus_info(request.volume_id(), request.nexus_info_id(), false)
            .await?;
        let shutdown_pending_nexuses = match request.volume_id() {
            None => Vec::new(),
            Some(id) => registry.specs().volume_failed_shutdown_nexuses(id).await,
        };

        Ok(Self {
            registry: registry.clone(),
            request: request.clone(),
            nexus_info,
            shutdown_failed_nexuses: shutdown_pending_nexuses,
        })
    }
    async fn list(&self) -> Vec<ChildItem> {
        // find all replica status
        let state_replicas = self.registry.get_replicas().await;
        // all pools
        let pool_wrappers = self.registry.get_pool_wrappers().await;

        let spec_replicas = match &self.request {
            GetPersistedNexusChildren::Create((vol_spec, _)) => {
                self.registry.specs().volume_replicas(&vol_spec.uuid)
            }
            GetPersistedNexusChildren::ReCreate(nexus_spec) => {
                // replicas used by the nexus
                // note: if the nexus was somehow created without using replicas (eg: directly using
                // aio:// etc) then we will not recreate it with those devices...
                self.registry.specs().nexus_replicas(nexus_spec)
            }
        };

        spec_replicas
            .into_iter()
            .filter_map(|replica_spec| {
                let replica_spec = replica_spec.lock().clone();
                let replica_state = state_replicas
                    .iter()
                    .find(|state| state.uuid == replica_spec.uuid);
                let child_info = self.nexus_info.as_ref().and_then(|n| {
                    n.children.iter().find(|c| {
                        if let Some(replica_state) = replica_state {
                            ChildUri::from(&replica_state.uri).uuid_str().as_ref() == Some(&c.uuid)
                        } else {
                            false
                        }
                    })
                });
                let pool_id = replica_spec.pool.pool_name().clone();
                pool_wrappers
                    .iter()
                    .find(|p| p.id == pool_id)
                    .and_then(|pool| {
                        replica_state.map(|replica_state| {
                            ChildItem::new(&replica_spec, replica_state, child_info, pool)
                        })
                    })
            })
            .collect()
    }
}

/// Builder used to retrieve a list of healthy nexus children which is used for nexus creation.
#[derive(Clone)]
pub(crate) struct CreateVolumeNexus {
    context: GetPersistedNexusChildrenCtx,
    list: Vec<ChildItem>,
}

impl CreateVolumeNexus {
    async fn builder(
        request: &GetPersistedNexusChildren,
        registry: &Registry,
    ) -> Result<Self, SvcError> {
        let context = GetPersistedNexusChildrenCtx::new(registry, request).await?;
        let list = context.list().await;
        Ok(Self { list, context })
    }

    /// Get the inner context.
    pub(crate) fn context(&self) -> &GetPersistedNexusChildrenCtx {
        &self.context
    }

    /// Get `Self` with a default set of filters for replicas/children according to the following
    /// criteria (any order):
    /// 1. if it's a nexus recreation, then use only children marked as healthy by the io-engine
    /// 2. use only replicas which report the status of online by their state
    /// 3. use only replicas which are large enough for the volume.
    pub(crate) async fn builder_with_defaults(
        request: &GetPersistedNexusChildren,
        registry: &Registry,
    ) -> Result<Self, SvcError> {
        Ok(Self::builder(request, registry)
            .await?
            .filter(ChildInfoFilters::healthy)
            .filter(ReplicaFilters::online)
            .filter(ReplicaFilters::size)
            .filter(ReplicaFilters::reservable)
            .sort_ctx(ChildItemSorters::sort_by_locality))
    }
}

#[async_trait::async_trait(?Send)]
impl ResourceFilter for CreateVolumeNexus {
    type Request = GetPersistedNexusChildrenCtx;
    type Item = ChildItem;

    fn filter<P: FnMut(&Self::Request, &Self::Item) -> bool>(mut self, mut filter: P) -> Self {
        let request = self.context.clone();
        self.list.retain(|v| filter(&request, v));
        self
    }

    fn sort<P: FnMut(&Self::Item, &Self::Item) -> std::cmp::Ordering>(mut self, sort: P) -> Self {
        self.list = self.list.into_iter().sorted_by(sort).collect();
        self
    }

    fn sort_ctx<P: FnMut(&Self::Request, &Self::Item, &Self::Item) -> std::cmp::Ordering>(
        mut self,
        mut sort: P,
    ) -> Self {
        let context = self.context.clone();
        self.list = self
            .list
            .into_iter()
            .sorted_by(|a, b| sort(&context, a, b))
            .collect();
        self
    }

    fn collect(self) -> Vec<Self::Item> {
        self.list
    }

    fn group_by<K, V, F: Fn(&Self::Request, &Vec<Self::Item>) -> HashMap<K, V>>(
        self,
        group: F,
    ) -> HashMap<K, V> {
        group(&self.context, &self.list)
    }
}

/// `GetSuitableNodes` for nexus failover/publish target node selection.
#[derive(Clone)]
pub(crate) struct GetSuitableNodes {
    spec: VolumeSpec,
}

impl From<&VolumeSpec> for GetSuitableNodes {
    fn from(spec: &VolumeSpec) -> Self {
        Self { spec: spec.clone() }
    }
}

/// `GetSuitableNodes` context for filtering and sorting.
#[derive(Clone)]
pub(crate) struct GetSuitableNodesContext {
    registry: Registry,
    spec: VolumeSpec,
}

impl GetSuitableNodesContext {
    /// Get the registry.
    pub(crate) fn registry(&self) -> &Registry {
        &self.registry
    }
}

impl Deref for GetSuitableNodesContext {
    type Target = VolumeSpec;

    fn deref(&self) -> &Self::Target {
        &self.spec
    }
}

impl Deref for GetSuitableNodes {
    type Target = VolumeSpec;

    fn deref(&self) -> &Self::Target {
        &self.spec
    }
}

/// `NexusTargetNode` is the builder used to retrieve a list of suitable nodes for the nexus
/// placement on failover/publish.
#[derive(Clone)]
pub(crate) struct NexusTargetNode {
    context: GetSuitableNodesContext,
    list: Vec<NodeItem>,
}

#[async_trait::async_trait(?Send)]
impl ResourceFilter for NexusTargetNode {
    type Request = GetSuitableNodesContext;
    type Item = NodeItem;

    fn filter<P: FnMut(&Self::Request, &Self::Item) -> bool>(mut self, mut filter: P) -> Self {
        let request = self.context.clone();
        self.list.retain(|v| filter(&request, v));
        self
    }

    fn sort<P: FnMut(&Self::Item, &Self::Item) -> std::cmp::Ordering>(mut self, sort: P) -> Self {
        self.list = self.list.into_iter().sorted_by(sort).collect();
        self
    }

    fn collect(self) -> Vec<Self::Item> {
        self.list
    }
}

impl NexusTargetNode {
    async fn builder(request: impl Into<GetSuitableNodes>, registry: &Registry) -> Self {
        let request = request.into();
        Self {
            context: GetSuitableNodesContext {
                registry: registry.clone(),
                spec: request.spec.clone(),
            },
            list: {
                let nodes = registry.node_wrappers().await;
                let mut node_items = Vec::with_capacity(nodes.len());
                for node in nodes {
                    let node = node.read().await;

                    node_items.push(NodeItem::new(node.clone()));
                }
                node_items
            },
        }
    }

    /// Get `Self` with a default set of filters for nodes following the criteria (any order):
    /// 1. The target node should be online.
    /// 2. Give preference to nodes which have lesser number of active nexuses, for
    /// proper distribution.
    pub(crate) async fn builder_with_defaults(
        request: impl Into<GetSuitableNodes>,
        registry: &Registry,
    ) -> Self {
        Self::builder(request, registry)
            .await
            .filter(NodeFilters::online)
            .filter(NodeFilters::cordoned)
            .filter(NodeFilters::current_target)
            .filter(NodeFilters::no_targets)
            .sort(NodeSorters::number_targets)
    }
}
