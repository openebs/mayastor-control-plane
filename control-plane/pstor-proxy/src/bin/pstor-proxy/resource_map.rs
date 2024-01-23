use indexmap::{map::Values, IndexMap};
use parking_lot::Mutex;
use pstor_proxy::types::frontend_node::{FrontendNodeId, FrontendNodeSpec, IntoVec};
use std::{
    fmt::{Debug, Display},
    hash::Hash,
    ops::Deref,
    sync::Arc,
};

/// Trait which exposes a resource Uid for various types of resources.
pub(crate) trait ResourceUid {
    /// The associated type for Id.
    type Uid: Clone + Display;
    /// The id of the resource.
    fn uid(&self) -> &Self::Uid;
    fn uid_str(&self) -> String {
        self.uid().to_string()
    }
}

/// Ref-counted resource wrapped with a mutex.
#[derive(Debug, Clone)]
pub(crate) struct ResourceMutex<T> {
    inner: Arc<ResourceMutexInner<T>>,
}
impl<T: Clone + ResourceUid> ResourceUid for ResourceMutex<T> {
    type Uid = T::Uid;

    fn uid(&self) -> &Self::Uid {
        self.immutable_ref().uid()
    }
}
impl<T: Clone + ResourceUid> ResourceUid for Resource<T> {
    type Uid = T::Uid;

    fn uid(&self) -> &Self::Uid {
        self.inner.uid()
    }
}
/// Inner Resource which holds the mutex and an immutable value for peeking
/// into immutable fields such as identification fields.
#[derive(Debug)]
pub(crate) struct ResourceMutexInner<T> {
    resource: Mutex<T>,
    immutable_peek: Arc<T>,
}
impl<T: Clone> From<T> for ResourceMutex<T> {
    fn from(resource: T) -> Self {
        let immutable_peek = Arc::new(resource.clone());
        let resource = Mutex::new(resource);
        Self {
            inner: Arc::new(ResourceMutexInner {
                resource,
                immutable_peek,
            }),
        }
    }
}
impl<T> Deref for ResourceMutex<T> {
    type Target = Mutex<T>;
    fn deref(&self) -> &Self::Target {
        &self.inner.resource
    }
}
impl<T: Clone> ResourceMutex<T> {
    /// Peek the initial resource value without locking.
    /// # Note:
    /// This is only useful for immutable fields, such as the resource identifier.
    pub(crate) fn immutable_ref(&self) -> &Arc<T> {
        &self.inner.immutable_peek
    }
}

/// Ref-counted resource.
#[derive(Debug, Clone)]
pub(crate) struct Resource<T> {
    inner: Arc<T>,
}
impl<T> Deref for Resource<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
impl<T: Clone> From<T> for Resource<T> {
    fn from(resource: T) -> Self {
        Self {
            inner: Arc::new(resource),
        }
    }
}

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

impl ResourceUid for FrontendNodeSpec {
    type Uid = FrontendNodeId;
    fn uid(&self) -> &Self::Uid {
        &self.id
    }
}
