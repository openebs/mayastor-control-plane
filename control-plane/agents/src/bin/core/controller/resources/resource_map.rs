use super::{ResourceMutex, ResourceUid};
use common_lib::IntoVec;
use indexmap::{map::Values, IndexMap};
use std::{fmt::Debug, hash::Hash};

#[derive(Debug)]
pub(crate) struct ResourceMap<I, S: Clone> {
    map: IndexMap<I, ResourceMutex<S>>,
}

impl<I, S> Default for ResourceMap<I, S>
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

impl<I, S> ResourceMap<I, S>
where
    I: Eq + Hash + Clone,
    S: Clone + ResourceUid<Uid = I> + Debug,
{
    /// Get the resource with the given key.
    pub(crate) fn get(&self, key: &I) -> Option<&ResourceMutex<S>> {
        self.map.get(key)
    }

    /// Clear the contents of the map.
    pub(crate) fn clear(&mut self) {
        self.map.clear();
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
        self.map
            .values()
            .skip(offset as usize)
            .take(len as usize)
            .map(|v| v.lock().clone())
            .collect()
    }
}
