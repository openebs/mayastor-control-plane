use super::{ResourceMutex, ResourceUid};
use crate::controller::resources::Resource;
use indexmap::{map::Values, IndexMap};
use std::{collections::HashMap, fmt::Debug, hash::Hash};
use stor_port::IntoVec;

/// ResourceMap for read-only resources.
pub(crate) type ResourceMap<I, S> = ResourceBaseMap<I, Resource<S>>;
/// ResourceMap for mutable resources.
pub(crate) type ResourceMutexMap<I, S> = ResourceBaseMap<I, ResourceMutex<S>>;

/// Base Resource Map.
#[derive(Debug)]
pub(crate) struct ResourceBaseMap<I, S: Clone> {
    map: IndexMap<I, S>,
}

impl<I, S> Default for ResourceBaseMap<I, ResourceMutex<S>>
where
    I: Eq + Hash + Clone,
    S: Clone + ResourceUid<Uid = I> + Debug,
{
    fn default() -> Self {
        Self {
            map: IndexMap::new(),
        }
    }
}

impl<I, S> ResourceBaseMap<I, ResourceMutex<S>>
where
    I: Eq + Hash + Clone,
    S: Clone + ResourceUid<Uid = I> + Debug,
{
    /// Get the resource with the given key.
    pub(crate) fn get(&self, key: &I) -> Option<&ResourceMutex<S>> {
        self.map.get(key)
    }

    /// Insert an element or update an existing entry in the map.
    pub(crate) fn insert(&mut self, value: S) -> ResourceMutex<S> {
        match self.map.get(value.uid()) {
            Some(entry) => {
                let mut e = entry.lock();
                *e = value;
                entry.clone()
            }
            None => {
                let key = value.uid().clone();
                let resource: ResourceMutex<S> = value.into();
                self.map.insert(key, resource.clone());
                resource
            }
        }
    }

    /// Remove an element from the map.
    pub(crate) fn remove(&mut self, key: &I) {
        self.map.remove(key);
    }

    /// Populate the resource map.
    /// Should only be called if the map is empty because a new Arc is created thereby invalidating
    /// any references to the previous value.
    pub(crate) fn populate(&mut self, values: impl IntoVec<S>) {
        assert!(self.map.is_empty());
        for value in values.into_vec() {
            self.map.insert(value.uid().clone(), value.into());
        }
    }

    /// Get all the resources as a vector.
    pub(crate) fn to_vec(&self) -> Vec<ResourceMutex<S>> {
        self.map.values().cloned().collect()
    }

    /// Return the maps values.
    pub(crate) fn values(&self) -> Values<'_, I, ResourceMutex<S>> {
        self.map.values()
    }

    /// Return the length of the map.
    pub(crate) fn len(&self) -> usize {
        self.map.len()
    }

    /// Return a subset of the map (i.e. a paginated result).
    pub(crate) fn paginate(&self, offset: u64, len: u64) -> Vec<S> {
        self.paginate_filter(offset, len, |_| true)
    }

    /// Apply the closure on the elements and return a subset of the map (i.e. a paginated result),
    /// containing the elements for which closure returns true.
    pub(crate) fn paginate_filter<F: Fn(&&ResourceMutex<S>) -> bool>(
        &self,
        offset: u64,
        len: u64,
        seive: F,
    ) -> Vec<S> {
        self.map
            .values()
            .filter(seive)
            .skip(offset as usize)
            .take(len as usize)
            .map(|v| v.lock().clone())
            .collect()
    }
}

impl<I, S> Default for ResourceBaseMap<I, Resource<S>>
where
    I: Eq + Hash + Clone,
    S: Clone + ResourceUid<Uid = I> + Debug,
{
    fn default() -> Self {
        Self {
            map: IndexMap::new(),
        }
    }
}

impl<I, S> ResourceBaseMap<I, Resource<S>>
where
    I: Eq + Hash + Clone,
    S: Clone + ResourceUid<Uid = I> + Debug,
{
    /// Get the resource with the given key.
    pub(crate) fn get(&self, key: &I) -> Option<&Resource<S>> {
        self.map.get(key)
    }

    /// Clear the contents of the map.
    pub(crate) fn clear(&mut self) {
        self.map.clear();
    }

    /// Insert an element or update an existing entry in the map.
    pub(crate) fn insert(&mut self, value: S) -> Resource<S> {
        let key = value.uid().clone();
        let resource: Resource<S> = value.into();
        self.map.insert(key, resource.clone());
        resource
    }

    /// Remove an element from the map.
    pub(crate) fn remove(&mut self, key: &I) {
        self.map.remove(key);
    }

    /// Populate the resource map.
    /// Should only be called if the map is empty because a new Arc is created thereby invalidating
    /// any references to the previous value.
    pub(crate) fn populate(&mut self, values: impl IntoVec<S>) {
        assert!(self.map.is_empty());
        for value in values.into_vec() {
            self.map.insert(value.uid().clone(), value.into());
        }
    }

    /// Update the resource map.
    pub(crate) fn update<U: Fn(&mut Resource<S>, S)>(&mut self, values: HashMap<I, S>, merge: U) {
        self.map.retain(|k, _| values.contains_key(k));

        for (key, value) in values {
            match self.map.get_mut(&key) {
                Some(existing) => merge(existing, value),
                None => {
                    self.map.insert(key, value.into());
                }
            }
        }
    }

    /// Return the maps values.
    pub(crate) fn values(&self) -> Values<'_, I, Resource<S>> {
        self.map.values()
    }

    /// Return the length of the map.
    #[allow(unused)]
    pub(crate) fn len(&self) -> usize {
        self.map.len()
    }
}
