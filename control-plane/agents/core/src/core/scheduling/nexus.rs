use crate::core::{
    registry::Registry,
    scheduling::{
        resources::ChildItem, ChildInfoFilters, ChildItemSorters, ReplicaFilters, ResourceFilter,
    },
};
use common::errors::SvcError;
use common_lib::types::v0::{
    message_bus::{ChildUri, NodeId},
    store::{
        nexus_persistence::{NexusInfo, NexusInfoKey},
        volume::VolumeSpec,
    },
};
use itertools::Itertools;
use std::collections::HashMap;

/// Request to retrieve a list of healthy nexus children which is used for nexus creation
/// used by `CreateVolumeNexus`
#[derive(Clone)]
pub(crate) struct GetPersistedNexusChildren {
    spec: VolumeSpec,
    target_node: NodeId,
}

impl GetPersistedNexusChildren {
    pub(crate) fn new(spec: &VolumeSpec, target_node: &NodeId) -> Self {
        Self {
            spec: spec.clone(),
            target_node: target_node.clone(),
        }
    }
}

/// `GetPersistedNexusChildren` context used by the filter functions for `GetPersistedNexusChildren`
#[derive(Clone)]
pub(crate) struct GetPersistedNexusChildrenCtx {
    registry: Registry,
    spec: VolumeSpec,
    target_node: NodeId,
    nexus_info: Option<NexusInfo>,
}

impl GetPersistedNexusChildrenCtx {
    /// Get the volume spec
    pub(crate) fn spec(&self) -> &VolumeSpec {
        &self.spec
    }
    /// Get the target node where the nexus will be created on
    pub(crate) fn target_node(&self) -> &NodeId {
        &self.target_node
    }
    /// Get the current nexus persistent information
    pub(crate) fn nexus_info(&self) -> &Option<NexusInfo> {
        &self.nexus_info
    }
}

impl GetPersistedNexusChildrenCtx {
    async fn new(
        registry: &Registry,
        request: &GetPersistedNexusChildren,
    ) -> Result<Self, SvcError> {
        let spec = request.spec.clone();
        let nexus_info = if let Some(nexus_id) = &spec.last_nexus_id {
            let mut nexus_info: NexusInfo =
                registry.load_obj(&NexusInfoKey::from(nexus_id)).await?;
            nexus_info.uuid = nexus_id.clone();
            Some(nexus_info)
        } else {
            None
        };

        Ok(Self {
            registry: registry.clone(),
            spec,
            nexus_info,
            target_node: request.target_node.clone(),
        })
    }
    async fn list(&self) -> Vec<ChildItem> {
        // find all replica status
        let state_replicas = self.registry.get_replicas().await;
        // find all replica specs for this volume
        let spec_replicas = self.registry.specs.get_volume_replicas(&self.spec.uuid);
        // all pools
        let pool_wrappers = self.registry.get_pool_wrappers().await;

        spec_replicas
            .into_iter()
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

/// Builder used to retrieve a list of healthy nexus children which is used for nexus creation
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

    /// Get the inner context
    pub(crate) fn context(&self) -> &GetPersistedNexusChildrenCtx {
        &self.context
    }

    /// Get `Self` with a default set of filters for replicas/children according to the following
    /// criteria (any order):
    /// 1. if it's a nexus recreation, then use only children marked as healthy by mayastor
    /// 2. use only replicas which report the status of online by their state
    /// 3. use only replicas which are large enough for the volume
    pub(crate) async fn builder_with_defaults(
        request: &GetPersistedNexusChildren,
        registry: &Registry,
    ) -> Result<Self, SvcError> {
        Ok(Self::builder(request, registry)
            .await?
            .filter(ChildInfoFilters::healthy)
            .filter(ReplicaFilters::online)
            .filter(ReplicaFilters::size)
            .sort_ctx(ChildItemSorters::sort_by_locality))
    }
}

#[async_trait::async_trait(?Send)]
impl ResourceFilter for CreateVolumeNexus {
    type Request = GetPersistedNexusChildrenCtx;
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
