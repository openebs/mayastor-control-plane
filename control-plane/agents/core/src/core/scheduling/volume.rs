use crate::core::{
    registry::Registry,
    scheduling::{
        resources::{PoolItem, PoolItemLister, ReplicaItem, ReplicaItemLister},
        ChildSorters, NodeFilters, PoolFilters, PoolSorters, ResourceFilter,
    },
};
use common_lib::types::v0::{
    message_bus::{CreateVolume, Volume},
    store::volume::VolumeSpec,
};
use itertools::Itertools;
use std::{collections::HashMap, future::Future, ops::Deref};

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
    pub(crate) registry: Registry,
    spec: VolumeSpec,
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

#[derive(Clone)]
pub(crate) struct IncreaseVolumeReplica {
    context: GetSuitablePoolsContext,
    list: Vec<PoolItem>,
}

impl IncreaseVolumeReplica {
    pub(crate) async fn builder(request: impl Into<GetSuitablePools>, registry: &Registry) -> Self {
        Self {
            context: GetSuitablePoolsContext {
                registry: registry.clone(),
                spec: request.into().spec,
            },
            list: PoolItemLister::list(registry).await,
        }
    }
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
            .filter(NodeFilters::online_nodes)
            .filter(NodeFilters::allowed_nodes)
            .filter(NodeFilters::unused_nodes)
            .filter(PoolFilters::usable_pools)
            .filter(PoolFilters::enough_free_space)
            // sort pools in order of preference (from least to most number of replicas)
            .sort(PoolSorters::sort_by_replica_count)
    }
}

#[async_trait::async_trait(?Send)]
impl ResourceFilter for IncreaseVolumeReplica {
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
    async fn filter_async<Fn, Fut>(mut self, mut filter: Fn) -> Self
    where
        Fn: FnMut(&Self::Request, &Self::Item) -> Fut,
        Fut: Future<Output = bool>,
    {
        let mut self_clone = self.clone();
        self.list.clear();

        for i in self.list {
            if filter(&self_clone.context, &i).await {
                self_clone.list.push(i);
            }
        }

        self_clone
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

#[derive(Clone)]
pub(crate) struct DecreaseVolumeReplica {
    context: GetChildForRemovalContext,
    list: Vec<ReplicaItem>,
}

#[derive(Clone)]
pub(crate) struct GetChildForRemoval {
    spec: VolumeSpec,
    status: Volume,
}

impl GetChildForRemoval {
    pub(crate) fn new(spec: &VolumeSpec, status: &Volume) -> Self {
        Self {
            spec: spec.clone(),
            status: status.clone(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct GetChildForRemovalContext {
    pub(crate) registry: Registry,
    spec: VolumeSpec,
}

impl DecreaseVolumeReplica {
    pub(crate) async fn builder(request: &GetChildForRemoval, registry: &Registry) -> Self {
        Self {
            context: GetChildForRemovalContext {
                registry: registry.clone(),
                spec: request.spec.clone(),
            },
            list: ReplicaItemLister::list(registry, &request.spec, &request.status).await,
        }
    }
    pub(crate) async fn builder_with_defaults(
        request: &GetChildForRemoval,
        registry: &Registry,
    ) -> Self {
        Self::builder(request, registry)
            .await
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
    async fn filter_async<Fn, Fut>(mut self, mut filter: Fn) -> Self
    where
        Fn: FnMut(&Self::Request, &Self::Item) -> Fut,
        Fut: Future<Output = bool>,
    {
        let mut self_clone = self.clone();
        self.list.clear();

        for i in self.list {
            if filter(&self_clone.context, &i).await {
                self_clone.list.push(i);
            }
        }

        self_clone
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
