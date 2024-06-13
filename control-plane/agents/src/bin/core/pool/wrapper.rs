use stor_port::types::v0::{
    store::replica::PoolRef,
    transport::{CtrlPoolState, PoolState, PoolStatus, Protocol, Replica, ReplicaId},
};

use std::{cmp::Ordering, collections::HashMap, ops::Deref};

/// Wrapper over a `Pool` state and all the replicas
/// with Ord traits to aid pool selection for volume replicas (legacy).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct PoolWrapper {
    state: PoolState,
    replicas: Vec<Replica>,
    labels: Option<HashMap<String, String>>,
    /// The accrued size/capacity of all replicas which means the pool usage could grow up to this
    /// size if < pool capacity. If this size is > pool capacity, then we can say the pool is
    /// overcommited by the size difference.
    committed: u64,
    free_space: u64,
}

impl Deref for PoolWrapper {
    type Target = PoolState;
    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl PoolWrapper {
    /// New Pool wrapper with the pool and replicas.
    pub(crate) fn new(
        mut pool: PoolState,
        labels: Option<HashMap<String, String>>,
        replicas: Vec<Replica>,
    ) -> Self {
        let free_space = if pool.capacity >= pool.used {
            pool.capacity - pool.used
        } else {
            // odd, let's report no free space available
            tracing::error!(
                "Pool '{}' has a capacity of '{} B' but is using '{} B'",
                pool.id,
                pool.capacity,
                pool.used
            );
            0
        };
        let committed = pool.committed.unwrap_or_else(|| {
            let committed = replicas.iter().map(|r| &r.size).sum();
            pool.committed = Some(committed);
            committed
        });

        Self {
            state: pool,
            replicas,
            labels,
            committed,
            free_space,
        }
    }

    /// Get the labels.
    pub(crate) fn labels(&self) -> Option<HashMap<String, String>> {
        self.labels.clone()
    }
    /// Get all the replicas.
    pub(crate) fn replicas(&self) -> &Vec<Replica> {
        &self.replicas
    }
    /// Get all the replicas.
    pub(crate) fn move_replicas(self) -> Vec<Replica> {
        self.replicas
    }
    /// Get the specified replica.
    pub(crate) fn replica(&self, replica: &ReplicaId) -> Option<&Replica> {
        self.replicas.iter().find(|r| &r.uuid == replica)
    }
    /// Get the pool data-plane state.
    pub(crate) fn state(&self) -> &PoolState {
        &self.state
    }
    /// Get the pool reference as name.
    /// todo: add uuid to pool state.
    pub(crate) fn pool_ref(&self) -> PoolRef {
        PoolRef::Named(self.id.clone())
    }

    /// Get the controller pool state (state + metadata).
    pub(crate) fn ctrl_state(&self) -> CtrlPoolState {
        CtrlPoolState::new(self.state.clone())
    }

    /// Get the over commitment in bytes.
    pub(crate) fn over_commitment(&self) -> u64 {
        if self.committed > self.state.capacity {
            self.committed - self.state.capacity
        } else {
            0
        }
    }
    /// Get the pool allocation growth potential in bytes.
    /// Would be allocation if all replicas are fully allocated.
    #[allow(unused)]
    pub(crate) fn max_growth(&self) -> u64 {
        if self.committed > self.used {
            self.committed - self.used
        } else {
            // should not happen...
            0
        }
    }
    /// Get the commitment in bytes.
    pub(crate) fn commitment(&self) -> u64 {
        self.committed
    }

    /// Get the free space.
    pub(crate) fn free_space(&self) -> u64 {
        self.free_space
    }

    /// Set pool state as unknown.
    #[allow(dead_code)]
    pub(crate) fn set_unknown(&mut self) {
        self.state.status = PoolStatus::Unknown;
    }

    /// Set labels in PoolWrapper.
    pub(crate) fn set_labels(&mut self, labels: Option<HashMap<String, String>>) {
        self.labels = labels;
    }

    /// Add replica to list.
    #[allow(dead_code)]
    pub(crate) fn add_replica(&mut self, replica: &Replica) {
        self.replicas.push(replica.clone())
    }
    /// Remove replica from list.
    #[allow(dead_code)]
    pub(crate) fn remove_replica(&mut self, uuid: &ReplicaId) {
        self.replicas.retain(|replica| &replica.uuid != uuid)
    }
    /// Update replica from list.
    #[allow(dead_code)]
    pub(crate) fn update_replica(&mut self, uuid: &ReplicaId, share: &Protocol, uri: &str) {
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
    // todo: change code to support using cmp
    #[allow(clippy::non_canonical_partial_ord_impl)]
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
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}
