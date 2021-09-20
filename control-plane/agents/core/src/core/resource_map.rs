use common_lib::{types::v0::store::ResourceUuid, IntoVec};
use parking_lot::Mutex;
use std::{
    collections::{hash_map::Values, HashMap},
    hash::Hash,
    sync::Arc,
};

#[derive(Default, Debug)]
pub struct ResourceMap<I, S> {
    map: HashMap<I, Arc<Mutex<S>>>,
}

impl<I, S> ResourceMap<I, S>
where
    I: Eq + Hash,
    S: Clone + ResourceUuid<Id = I>,
{
    /// Get the resource with the given key.
    pub fn get(&self, key: &I) -> Option<&Arc<Mutex<S>>> {
        self.map.get(key)
    }

    /// Clear the contents of the map.
    pub fn clear(&mut self) {
        self.map.clear();
    }

    /// Insert an element or update an existing entry in the map.
    pub fn insert(&mut self, value: S) -> Arc<Mutex<S>> {
        let key = value.uuid();
        match self.map.get(&key) {
            Some(entry) => {
                let mut e = entry.lock();
                *e = value;
                entry.clone()
            }
            None => {
                let v = Arc::new(Mutex::new(value));
                self.map.insert(key, v.clone());
                v
            }
        }
    }

    /// Remove an element from the map.
    pub fn remove(&mut self, key: &I) {
        self.map.remove(key);
    }

    /// Populate the resource map.
    /// Should only be called if the map is empty because a new Arc is created thereby invalidating
    /// any references to the previous value.
    pub fn populate(&mut self, values: impl IntoVec<S>) {
        assert!(self.map.is_empty());
        for value in values.into_vec() {
            self.map.insert(value.uuid(), Arc::new(Mutex::new(value)));
        }
    }

    /// Get all the resources as a vector.
    pub fn to_vec(&self) -> Vec<Arc<Mutex<S>>> {
        self.map.values().cloned().collect()
    }

    /// Return the maps values.
    pub fn values(&self) -> Values<'_, I, Arc<Mutex<S>>> {
        self.map.values()
    }
}
