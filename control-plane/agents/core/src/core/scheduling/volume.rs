use crate::core::{
    registry::Registry,
    scheduling::{
        resources::{PoolItem, PoolItemLister, ReplicaItem},
        ChildSorters, NodeFilters, PoolFilters, PoolSorters, ResourceFilter,
    },
};

use crate::core::scheduling::{
    resources::{ChildItem, NexusChildItem},
    AddReplicaFilters, AddReplicaSorters, NexusChildSorter,
};
use common::errors::SvcError;
use common_lib::types::v0::{
    message_bus::{ChildUri, CreateVolume, VolumeState},
    store::{
        nexus::NexusSpec,
        nexus_persistence::{NexusInfo, NexusInfoKey},
        volume::VolumeSpec,
    },
};
use itertools::Itertools;
use std::{collections::HashMap, ops::Deref};

#[derive(Clone)]
pub(crate) struct GetSuitablePools {
    spec: VolumeSpec,
}

impl From<&CreateVolume> for GetSuitablePools {
    fn from(create: &CreateVolume) -> Self {
        Self {
            spec: create.into(),
        }
    }
}
impl From<&VolumeSpec> for GetSuitablePools {
    fn from(spec: &VolumeSpec) -> Self {
        Self { spec: spec.clone() }
    }
}

#[derive(Clone)]
pub(crate) struct GetSuitablePoolsContext {
    registry: Registry,
    spec: VolumeSpec,
}
impl GetSuitablePoolsContext {
    /// Get the registry
    pub(crate) fn registry(&self) -> &Registry {
        &self.registry
    }
}

impl Deref for GetSuitablePoolsContext {
    type Target = VolumeSpec;

    fn deref(&self) -> &Self::Target {
        &self.spec
    }
}
impl Deref for GetSuitablePools {
    type Target = VolumeSpec;

    fn deref(&self) -> &Self::Target {
        &self.spec
    }
}

/// Add replicas to a volume
/// Selects the best pool candidates to create lvol replicas on
#[derive(Clone)]
pub(crate) struct AddVolumeReplica {
    context: GetSuitablePoolsContext,
    list: Vec<PoolItem>,
}

impl AddVolumeReplica {
    async fn builder(request: impl Into<GetSuitablePools>, registry: &Registry) -> Self {
        let request = request.into();
        Self {
            context: GetSuitablePoolsContext {
                registry: registry.clone(),
                spec: request.spec.clone(),
            },
            list: PoolItemLister::list(registry).await,
        }
    }
    /// Default rules for pool selection when creating replicas for a volume
    pub(crate) async fn builder_with_defaults(
        request: impl Into<GetSuitablePools>,
        registry: &Registry,
    ) -> Self {
        Self::builder(request, registry)
            .await
            // filter pools according to the following criteria (any order):
            // 1. if allowed_nodes were specified then only pools from those nodes
            // can be used.
            // 2. pools should have enough free space for the
            // volume (do we need to take into account metadata?)
            // 3. ideally use only healthy(online) pools with degraded pools as a
            // fallback
            // 4. only one replica per node
            .filter(NodeFilters::online)
            .filter(NodeFilters::allowed)
            .filter(NodeFilters::unused)
            .filter(PoolFilters::usable)
            .filter(PoolFilters::free_space)
            // sort pools in order of preference (from least to most number of replicas)
            .sort(PoolSorters::sort_by_replica_count)
    }
}

#[async_trait::async_trait(?Send)]
impl ResourceFilter for AddVolumeReplica {
    type Request = GetSuitablePoolsContext;
    type Item = PoolItem;

    fn filter<P: FnMut(&Self::Request, &Self::Item) -> bool>(mut self, mut filter: P) -> Self {
        let request = self.context.clone();
        self.list = self
            .list
            .into_iter()
            .filter(|v| filter(&request, v))
            .collect();
        self
    }

    fn sort<P: FnMut(&Self::Item, &Self::Item) -> std::cmp::Ordering>(mut self, sort: P) -> Self {
        self.list = self.list.into_iter().sorted_by(sort).collect();
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

/// Decrease a volume's replicas when it exceeds the required count
#[derive(Clone)]
pub(crate) struct DecreaseVolumeReplica {
    context: GetChildForRemovalContext,
    list: Vec<ReplicaItem>,
}

/// Request to decrease volume replicas
/// Specifies the volume spec, state and whether to only remove currently unused replicas
#[derive(Clone)]
pub(crate) struct GetChildForRemoval {
    spec: VolumeSpec,
    state: VolumeState,
    /// Used when we have more replicas than we need, so we can be picky and try to remove
    /// unused replicas first (replicas which are not attached to a nexus)
    unused_only: bool,
}

impl GetChildForRemoval {
    /// Return a new `Self` from the provided parameters
    pub(crate) fn new(spec: &VolumeSpec, state: &VolumeState, unused_only: bool) -> Self {
        Self {
            spec: spec.clone(),
            state: state.clone(),
            unused_only,
        }
    }
}

/// Used to filter nexus children in order to choose the best candidates for removal
/// when the volume's replica count is being reduced.
#[derive(Clone)]
pub(crate) struct GetChildForRemovalContext {
    registry: Registry,
    spec: VolumeSpec,
    state: VolumeState,
    unused_only: bool,
}

impl GetChildForRemovalContext {
    async fn list(&self) -> Vec<ReplicaItem> {
        let replicas = self.registry.specs.get_volume_replicas(&self.spec.uuid);
        let nexuses = self.registry.specs.get_volume_nexuses(&self.spec.uuid);
        let replicas = replicas.iter().map(|r| r.lock().clone());

        let replica_states = self.registry.get_replicas().await;
        replicas
            .map(|replica_spec| {
                ReplicaItem::new(
                    replica_spec.clone(),
                    replica_states
                        .iter()
                        .find(|replica_state| replica_state.uuid == replica_spec.uuid)
                        .map(|replica_state| {
                            nexuses
                                .iter()
                                .filter_map(|nexus_spec| {
                                    nexus_spec
                                        .lock()
                                        .children
                                        .iter()
                                        .find(|child| child.uri() == replica_state.uri)
                                        .map(|child| child.uri())
                                })
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default(),
                    replica_states
                        .iter()
                        .find(|replica_state| replica_state.uuid == replica_spec.uuid)
                        .map(|replica_state| {
                            self.state
                                .children
                                .iter()
                                .filter_map(|nexus_state| {
                                    nexus_state
                                        .children
                                        .iter()
                                        .find(|child| child.uri.as_str() == replica_state.uri)
                                })
                                .cloned()
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default(),
                    nexuses
                        .iter()
                        .filter_map(|nexus_spec| {
                            nexus_spec
                                .lock()
                                .children
                                .iter()
                                .find(|child| {
                                    child.as_replica().map(|uri| uri.uuid().clone())
                                        == Some(replica_spec.uuid.clone())
                                })
                                .cloned()
                        })
                        .collect(),
                )
            })
            .collect::<Vec<_>>()
    }
}

impl DecreaseVolumeReplica {
    async fn builder(request: &GetChildForRemoval, registry: &Registry) -> Self {
        let context = GetChildForRemovalContext {
            registry: registry.clone(),
            spec: request.spec.clone(),
            state: request.state.clone(),
            unused_only: request.unused_only,
        };
        Self {
            list: context.list().await,
            context,
        }
    }
    /// Create new `Self` from the given arguments with a default list of filters and sorting rules
    pub(crate) async fn builder_with_defaults(
        request: &GetChildForRemoval,
        registry: &Registry,
    ) -> Self {
        Self::builder(request, registry)
            .await
            // if requested filter for replicas which are not currently used for a nexus
            .filter(|request, item| !(request.unused_only && item.child_spec().is_some()))
            .sort(ChildSorters::sort)
    }
}

#[async_trait::async_trait(?Send)]
impl ResourceFilter for DecreaseVolumeReplica {
    type Request = GetChildForRemovalContext;
    type Item = ReplicaItem;

    fn filter<P: FnMut(&Self::Request, &Self::Item) -> bool>(mut self, mut filter: P) -> Self {
        let request = self.context.clone();
        self.list = self
            .list
            .into_iter()
            .filter(|v| filter(&request, v))
            .collect();
        self
    }

    fn sort<P: FnMut(&Self::Item, &Self::Item) -> std::cmp::Ordering>(mut self, sort: P) -> Self {
        self.list = self.list.into_iter().sorted_by(sort).collect();
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

/// Used to determine the nexus child removal candidates when a nexus has "too many" replicas
#[derive(Clone)]
pub(crate) struct GetNexusChildForRemovalContext {
    registry: Registry,
    vol_spec: VolumeSpec,
    nexus_spec: NexusSpec,
}

/// Decrease a nexus replica count when it has more than required by its volume
#[derive(Clone)]
pub(crate) struct DecreaseNexusReplica {
    context: GetNexusChildForRemovalContext,
    list: Vec<NexusChildItem>,
}

impl GetNexusChildForRemovalContext {
    async fn list(&self) -> Vec<NexusChildItem> {
        let nexus = self.registry.get_nexus(&self.nexus_spec.uuid).await.ok();
        let replicas = self.registry.specs.get_volume_replicas(&self.vol_spec.uuid);
        let mut replicas = replicas.iter().map(|r| r.lock().clone());

        self.nexus_spec
            .children
            .iter()
            .map(|child| {
                let spec = child
                    .as_replica()
                    .map(|r| replicas.find(|s| &s.uuid == r.uuid()))
                    .flatten();
                let child_state = nexus
                    .as_ref()
                    .map(|n| n.children.iter().find(|c| c.uri == child.uri()));

                NexusChildItem::new(spec, child.uri(), child_state.flatten())
            })
            .collect::<Vec<_>>()
    }
}

impl DecreaseNexusReplica {
    async fn builder(vol_spec: &VolumeSpec, nexus_spec: &NexusSpec, registry: &Registry) -> Self {
        let context = GetNexusChildForRemovalContext {
            registry: registry.clone(),
            vol_spec: vol_spec.clone(),
            nexus_spec: nexus_spec.clone(),
        };
        Self {
            list: context.list().await,
            context,
        }
    }
    /// Create a new `Self` from the given arguments
    pub(crate) async fn builder_with_defaults(
        vol_spec: &VolumeSpec,
        nexus_spec: &NexusSpec,
        registry: &Registry,
    ) -> Self {
        Self::builder(vol_spec, nexus_spec, registry)
            .await
            .sort(NexusChildSorter::sort)
    }
}

#[async_trait::async_trait(?Send)]
impl ResourceFilter for DecreaseNexusReplica {
    type Request = GetNexusChildForRemovalContext;
    type Item = NexusChildItem;

    fn filter<P: FnMut(&Self::Request, &Self::Item) -> bool>(mut self, mut filter: P) -> Self {
        let request = self.context.clone();
        self.list = self
            .list
            .into_iter()
            .filter(|v| filter(&request, v))
            .collect();
        self
    }

    fn sort<P: FnMut(&Self::Item, &Self::Item) -> std::cmp::Ordering>(mut self, sort: P) -> Self {
        self.list = self.list.into_iter().sorted_by(sort).collect();
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

/// `VolumeReplicasForNexusCtx` context used by the filter functions for `AddVolumeNexusReplicas`
/// which is used to add replicas to a volume nexus
#[derive(Clone)]
pub(crate) struct VolumeReplicasForNexusCtx {
    registry: Registry,
    vol_spec: VolumeSpec,
    nexus_spec: NexusSpec,
    nexus_info: Option<NexusInfo>,
}

impl VolumeReplicasForNexusCtx {
    /// Get the volume spec
    pub(crate) fn vol_spec(&self) -> &VolumeSpec {
        &self.vol_spec
    }
    /// Get the nexus spec
    pub(crate) fn nexus_spec(&self) -> &NexusSpec {
        &self.nexus_spec
    }
    /// Get the current nexus persistent information
    #[allow(dead_code)]
    pub(crate) fn nexus_info(&self) -> &Option<NexusInfo> {
        &self.nexus_info
    }
    /// Get the registry
    #[allow(dead_code)]
    pub(crate) fn registry(&self) -> &Registry {
        &self.registry
    }
}

impl VolumeReplicasForNexusCtx {
    async fn new(
        registry: &Registry,
        vol_spec: &VolumeSpec,
        nx_spec: &NexusSpec,
    ) -> Result<Self, SvcError> {
        let nexus_info = match registry
            .load_obj::<NexusInfo>(&NexusInfoKey::from(&nx_spec.uuid))
            .await
        {
            Ok(mut info) => {
                info.uuid = nx_spec.uuid.clone();
                Some(info)
            }
            Err(SvcError::StoreMissingEntry { .. }) => None,
            Err(error) => return Err(error),
        };

        Ok(Self {
            registry: registry.clone(),
            vol_spec: vol_spec.clone(),
            nexus_spec: nx_spec.clone(),
            nexus_info,
        })
    }
    async fn list(&self) -> Vec<ChildItem> {
        // find all replica states
        let state_replicas = self.registry.get_replicas().await;
        // find all replica specs which are not yet part of the nexus
        let spec_replicas = self
            .registry
            .specs
            .get_volume_replicas(&self.vol_spec.uuid)
            .into_iter()
            .filter(|r| !self.nexus_spec.contains_replica(&r.lock().uuid));
        let pool_wrappers = self.registry.get_pool_wrappers().await;

        spec_replicas
            .filter_map(|replica_spec| {
                let replica_spec = replica_spec.lock().clone();
                let replica_state = state_replicas
                    .iter()
                    .find(|state| state.uuid == replica_spec.uuid);
                let child_info = self
                    .nexus_info
                    .as_ref()
                    .map(|n| {
                        n.children.iter().find(|c| {
                            if let Some(replica_state) = replica_state {
                                ChildUri::from(&replica_state.uri).uuid_str().as_ref()
                                    == Some(&c.uuid)
                            } else {
                                false
                            }
                        })
                    })
                    .flatten();

                pool_wrappers
                    .iter()
                    .find(|p| p.id == replica_spec.pool)
                    .map(|pool| {
                        replica_state.map(|replica_state| {
                            ChildItem::new(&replica_spec, replica_state, child_info, pool)
                        })
                    })
                    .flatten()
            })
            .collect()
    }
}

/// Retrieve a list of healthy replicas to add to a volume nexus.
#[derive(Clone)]
pub(crate) struct AddVolumeNexusReplicas {
    context: VolumeReplicasForNexusCtx,
    list: Vec<ChildItem>,
}

impl AddVolumeNexusReplicas {
    async fn builder(
        vol_spec: &VolumeSpec,
        nx_spec: &NexusSpec,
        registry: &Registry,
    ) -> Result<Self, SvcError> {
        let context = VolumeReplicasForNexusCtx::new(registry, vol_spec, nx_spec).await?;
        let list = context.list().await;
        Ok(Self { list, context })
    }

    /// Builder used to retrieve a list of healthy replicas to add to a volume nexus.
    /// The list follows a set of filters for replicas according to the following
    /// criteria (any order):
    /// 1. replicas which are not part of the given nexus already
    /// 2. use only replicas which report the status of online by their state
    /// 3. use only replicas which are large enough for the volume
    /// Sorted by:
    /// 1. nexus local replicas
    /// 2. replicas which have never been marked as faulted by mayastor
    /// 3. replicas from pools with more free space
    pub(crate) async fn builder_with_defaults(
        vol_spec: &VolumeSpec,
        nx_spec: &NexusSpec,
        registry: &Registry,
    ) -> Result<Self, SvcError> {
        Ok(Self::builder(vol_spec, nx_spec, registry)
            .await?
            .filter(AddReplicaFilters::online)
            .filter(AddReplicaFilters::size)
            .sort_ctx(AddReplicaSorters::sort))
    }
}

#[async_trait::async_trait(?Send)]
impl ResourceFilter for AddVolumeNexusReplicas {
    type Request = VolumeReplicasForNexusCtx;
    type Item = ChildItem;

    fn filter<P: FnMut(&Self::Request, &Self::Item) -> bool>(mut self, mut filter: P) -> Self {
        let request = self.context.clone();
        self.list = self
            .list
            .into_iter()
            .filter(|v| filter(&request, v))
            .collect();
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
