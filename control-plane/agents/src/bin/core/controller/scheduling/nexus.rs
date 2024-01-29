use crate::controller::{
    registry::Registry,
    resources::ResourceMutex,
    scheduling::{
        resources::{ChildItem, NodeItem},
        volume_policy::node::{NodeFilters, NodeSorters},
        ChildInfoFilters, ChildItemSorters, ReplicaFilters, ResourceData, ResourceFilter,
    },
};
use agents::errors::SvcError;
use std::collections::HashMap;

use crate::controller::{
    resources::ResourceUid, scheduling::affinity_group::get_node_ag_nexus_count,
};
use std::ops::Deref;
use stor_port::types::v0::{
    store::{nexus::NexusSpec, nexus_persistence::NexusInfo, volume::VolumeSpec},
    transport::{NexusId, NodeId, VolumeId},
};

/// Request to retrieve a list of healthy nexus children which is used for nexus creation
/// used by `CreateVolumeNexus`.
#[derive(Clone)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum GetPersistedNexusChildren {
    Create((VolumeSpec, NodeId)),
    ReCreate(NexusSpec),
    Snapshot(VolumeSpec),
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
    /// Retrieve a list of children for a snapshot.
    pub(crate) fn new_snapshot(spec: &VolumeSpec) -> Self {
        Self::Snapshot(spec.clone())
    }
    /// Get the optional volume spec (used for nexus creation).
    pub(crate) fn vol_spec(&self) -> Option<&VolumeSpec> {
        match self {
            Self::Create((spec, _)) => Some(spec),
            Self::ReCreate(_) => None,
            Self::Snapshot(_) => None,
        }
    }
    /// Get the target node where the nexus will be created/recreated on.
    pub(crate) fn target_node(&self) -> Option<&NodeId> {
        match self {
            Self::Create((_, node)) => Some(node),
            Self::ReCreate(nexus) => Some(&nexus.node),
            Self::Snapshot(_) => None,
        }
    }
    /// Get the current nexus persistent information Id.
    pub(crate) fn nexus_info_id(&self) -> Option<&NexusId> {
        match self {
            Self::Create((vol, _)) => vol.health_info_id(),
            Self::ReCreate(nexus) => Some(&nexus.uuid),
            Self::Snapshot(vol) => vol.health_info_id(),
        }
    }

    /// Get the volume ID associated with the persisted nexus info.
    pub(crate) fn volume_id(&self) -> Option<&VolumeId> {
        match self {
            Self::Create((vol, _)) => Some(&vol.uuid),
            Self::ReCreate(nexus) => match nexus.owner.as_ref() {
                Some(volume_id) => Some(volume_id),
                None => None,
            },
            Self::Snapshot(vol) => Some(&vol.uuid),
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
    pub(crate) fn target_node(&self) -> Option<&NodeId> {
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
        let state_replicas = self.registry.replicas().await;
        // all pools
        let pool_wrappers = self.registry.pool_wrappers().await;

        let spec_replicas = match &self.request {
            GetPersistedNexusChildren::Create((vol_spec, _)) => {
                self.registry.specs().volume_replicas(vol_spec.uid())
            }
            GetPersistedNexusChildren::ReCreate(nexus_spec) => {
                // replicas used by the nexus
                // note: if the nexus was somehow created without using replicas (eg: directly using
                // aio:// etc) then we will not recreate it with those devices...
                self.registry.specs().nexus_replicas(nexus_spec)
            }
            GetPersistedNexusChildren::Snapshot(vol_spec) => {
                self.registry.specs().volume_replicas(vol_spec.uid())
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
                            replica_state.uuid == c.uuid
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
                            ChildItem::new(&replica_spec, replica_state, child_info, pool, None)
                        })
                    })
            })
            .collect()
    }
}

/// Builder used to retrieve a list of healthy nexus children which is used for nexus creation.
#[derive(Clone)]
pub(crate) struct CreateVolumeNexus {
    data: ResourceData<GetPersistedNexusChildrenCtx, ChildItem>,
}

impl CreateVolumeNexus {
    async fn builder(
        request: &GetPersistedNexusChildren,
        registry: &Registry,
    ) -> Result<Self, SvcError> {
        let request = GetPersistedNexusChildrenCtx::new(registry, request).await?;
        let list = request.list().await;
        Ok(Self {
            data: ResourceData::new(request, list),
        })
    }

    /// Get the inner context.
    pub(crate) fn context(&self) -> &GetPersistedNexusChildrenCtx {
        &self.data.context
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

    fn data(&mut self) -> &mut ResourceData<Self::Request, Self::Item> {
        &mut self.data
    }

    fn collect(self) -> Vec<Self::Item> {
        self.data.list
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
    data: ResourceData<GetSuitableNodesContext, NodeItem>,
}

#[async_trait::async_trait(?Send)]
impl ResourceFilter for NexusTargetNode {
    type Request = GetSuitableNodesContext;
    type Item = NodeItem;

    fn data(&mut self) -> &mut ResourceData<Self::Request, Self::Item> {
        &mut self.data
    }

    fn collect(self) -> Vec<Self::Item> {
        self.data.list
    }
}

impl NexusTargetNode {
    async fn builder(
        request: impl Into<GetSuitableNodes>,
        registry: &Registry,
        preferred_node: &Option<NodeId>,
    ) -> Self {
        let request = request.into();
        let request = GetSuitableNodesContext {
            registry: registry.clone(),
            spec: request.spec.clone(),
        };
        let mut node_ag_nexus_count_map: Option<HashMap<NodeId, u64>> = None;
        if let Some(affinity_group) = &request.affinity_group {
            if let Ok(affinity_group_spec) =
                registry.specs().affinity_group_spec(affinity_group.id())
            {
                node_ag_nexus_count_map =
                    Some(get_node_ag_nexus_count(&affinity_group_spec, registry).await);
            }
        }

        let list = {
            let nodes = registry.node_wrappers().await;
            let mut node_items = Vec::with_capacity(nodes.len());
            for node in nodes {
                let node = node.read().await;
                let ag_nexus_count = node_ag_nexus_count_map
                    .as_ref()
                    .and_then(|map| map.get(node.id()).cloned());

                let preferred = preferred_node.as_ref() == Some(node.id());
                node_items.push(NodeItem::new(node.clone(), ag_nexus_count, preferred));
            }
            node_items
        };
        Self {
            data: ResourceData::new(request, list),
        }
    }

    /// Get `Self` with a default set of filters for nodes following the criteria (any order):
    /// 1. The target node should be online.
    /// 2. Give preference to nodes which have lesser number of active nexuses, for
    /// proper distribution.
    pub(crate) async fn builder_with_defaults(
        request: impl Into<GetSuitableNodes>,
        registry: &Registry,
        preferred_node: &Option<NodeId>,
    ) -> Self {
        Self::builder(request, registry, preferred_node)
            .await
            .filter(NodeFilters::online)
            .filter(NodeFilters::cordoned)
            .filter(NodeFilters::current_target)
            .filter(NodeFilters::no_targets)
            .sort(NodeSorters::number_targets)
    }
}
