use stor_port::types::v0::transport::{PoolState, PoolStatus, Protocol, Replica, ReplicaId};

use std::{cmp::Ordering, ops::Deref};

/// Wrapper over a `Pool` state and all the replicas
/// with Ord traits to aid pool selection for volume replicas (legacy).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct PoolWrapper {
    state: PoolState,
    replicas: Vec<Replica>,
    /// The accrued size/capacity of all replicas which means the pool usage could grow up to this
    /// size if < pool capacity. If this size is > pool capacity, then we can say the pool is
    /// overcommited by the size difference.
    commitment: u64,
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
    pub(crate) fn new(pool: PoolState, replicas: Vec<Replica>) -> Self {
        let commitment = replicas
            .iter()
            .flat_map(|r| &r.space)
            .map(|r| r.capacity_bytes)
            .sum();
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
        Self {
            state: pool,
            replicas,
            commitment,
            free_space,
        }
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
    /// Get the state.
    pub(crate) fn state(&self) -> &PoolState {
        &self.state
    }

    /// Get the over commitment in bytes.
    pub(crate) fn over_commitment(&self) -> u64 {
        if self.commitment > self.state.capacity {
            self.commitment - self.state.capacity
        } else {
            0
        }
    }
    /// Get the pool allocation growth potential in bytes.
    /// Would be allocation if all replicas are fully allocated.
    #[allow(unused)]
    pub(crate) fn max_growth(&self) -> u64 {
        if self.commitment > self.used {
            self.commitment - self.used
        } else {
            // should not happen...
            0
        }
    }
    /// Get the commitment in bytes.
    pub(crate) fn commitment(&self) -> u64 {
        self.commitment
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
        match self.state.status.partial_cmp(&other.state.status) {
            Some(Ordering::Greater) => Ordering::Greater,
            Some(Ordering::Less) => Ordering::Less,
            Some(Ordering::Equal) => match self.replicas.len().cmp(&other.replicas.len()) {
                Ordering::Greater => Ordering::Greater,
                Ordering::Less => Ordering::Less,
                Ordering::Equal => self.free_space().cmp(&other.free_space()),
            },
            None => Ordering::Equal,
        }
    }
}
