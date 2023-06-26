use crate::controller::resources::{resource_map::ResourceMap, Resource};
use stor_port::types::v0::{
    store::{
        nexus::NexusState, pool::PoolState, replica::ReplicaState, snapshots::ReplicaSnapshotState,
    },
    transport::{
        self, Nexus, NexusId, PoolId, RebuildHistory, Replica, ReplicaId, ReplicaSnapshot,
        SnapshotId,
    },
};

use indexmap::map::Values;
use itertools::Itertools;
use parking_lot::RwLock;
use std::{collections::HashMap, ops::Deref, sync::Arc};

/// Locked Resource States.
#[derive(Clone, Default, Debug)]
pub(crate) struct ResourceStatesLocked(Arc<RwLock<ResourceStates>>);

impl ResourceStatesLocked {
    /// Return a new empty `Self`.
    pub(crate) fn new() -> Self {
        Default::default()
    }
}

impl Deref for ResourceStatesLocked {
    type Target = Arc<RwLock<ResourceStates>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Resource States.
#[derive(Default, Debug)]
pub(crate) struct ResourceStates {
    nexuses: ResourceMap<NexusId, NexusState>,
    pools: ResourceMap<PoolId, PoolState>,
    replicas: ResourceMap<ReplicaId, ReplicaState>,
    snapshots: ResourceMap<SnapshotId, ReplicaSnapshotState>,
    rebuild_history: ResourceMap<NexusId, RebuildHistory>,
    rebuild_history_since: Option<prost_types::Timestamp>,
}

/// Add/Update or remove resource from the registry.
pub(crate) enum Either<R, I> {
    /// Insert the resource `R` in the registry.
    Insert(R),
    /// Removes the resource `R` by it's `I`dentifier.
    Remove(I),
}

impl ResourceStates {
    /// Update the various resource states.
    pub(crate) fn update(
        &mut self,
        pools: Vec<transport::PoolState>,
        replicas: Vec<Replica>,
        nexuses: Vec<Nexus>,
        snapshots: Vec<ReplicaSnapshot>,
        rebuild_histories: RebuildHistoryState,
    ) {
        self.update_replicas(replicas);
        self.update_pools(pools);
        self.update_nexuses(nexuses);
        self.update_snapshots(snapshots);
        self.update_rebuild_history(rebuild_histories);
    }

    /// Update nexus states.
    pub(crate) fn update_nexuses(&mut self, nexuses: Vec<Nexus>) {
        self.nexuses.clear();
        self.nexuses.populate(nexuses);
    }

    /// Update nexus state.
    pub(crate) fn update_nexus(&mut self, state: Either<Nexus, NexusId>) {
        match state {
            Either::Insert(nexus) => {
                self.nexuses.insert(nexus.into());
            }
            Either::Remove(nexus) => {
                self.nexuses.remove(&nexus);
            }
        }
    }

    /// Returns a vector of cloned nexus states.
    pub(crate) fn nexus_states_cloned(&self) -> Vec<NexusState> {
        Self::cloned_inner_states(self.nexuses.values())
    }

    /// Returns an iterator of nexus states.
    pub(crate) fn nexus_states(&self) -> Values<NexusId, Resource<NexusState>> {
        self.nexuses.values()
    }

    /// Returns the nexus state for the nexus with the given ID.
    pub(crate) fn nexus_state(&self, id: &NexusId) -> Option<NexusState> {
        self.nexuses.get(id).map(|state| state.inner().clone())
    }

    /// Update pool states.
    pub(crate) fn update_pools(&mut self, pools: Vec<transport::PoolState>) {
        self.pools.clear();
        self.pools.populate(pools);
    }

    /// Update pool state.
    pub(crate) fn update_pool(&mut self, state: Either<transport::PoolState, PoolId>) {
        match state {
            Either::Insert(pool) => {
                self.pools.insert(pool.into());
            }
            Either::Remove(pool) => {
                self.pools.remove(&pool);
            }
        }
    }

    /// Returns a vector of cloned pool states.
    pub(crate) fn pool_states_cloned(&self) -> Vec<PoolState> {
        Self::cloned_inner_states(self.pools.values())
    }

    /// Returns an iterator of pool states.
    pub(crate) fn pool_states(&self) -> Values<PoolId, Resource<PoolState>> {
        self.pools.values()
    }

    /// Get a pool with the given ID.
    pub(crate) fn pool_state(&self, id: &PoolId) -> Option<PoolState> {
        let pool_state = self.pools.get(id)?;
        Some(pool_state.inner().clone())
    }

    /// Update replica states.
    pub(crate) fn update_replicas(&mut self, replicas: Vec<Replica>) {
        self.replicas.clear();
        self.replicas.populate(replicas);
    }

    /// Update replica state.
    pub(crate) fn update_replica(&mut self, state: Either<Replica, ReplicaId>) {
        match state {
            Either::Insert(replica) => {
                self.replicas.insert(replica.into());
            }
            Either::Remove(replica) => {
                self.replicas.remove(&replica);
            }
        }
    }

    /// Update snapshot states.
    pub(crate) fn update_snapshots(&mut self, snapshots: Vec<ReplicaSnapshot>) {
        self.snapshots.clear();
        self.snapshots.populate(snapshots);
    }

    /// Update snapshot state.
    pub(crate) fn update_snapshot(&mut self, state: Either<ReplicaSnapshot, SnapshotId>) {
        match state {
            Either::Insert(snapshot) => {
                self.snapshots.insert(snapshot.into());
            }
            Either::Remove(snapshot) => {
                self.snapshots.remove(&snapshot);
            }
        }
    }

    /// Returns a vector of cloned replica states.
    pub(crate) fn replica_states_cloned(&self) -> Vec<ReplicaState> {
        Self::cloned_inner_states(self.replicas.values())
    }

    /// Returns an iterator of replica states.
    pub(crate) fn replica_states(&self) -> Values<ReplicaId, Resource<ReplicaState>> {
        self.replicas.values()
    }

    /// Get a replica with the given ID.
    pub(crate) fn replica_state(&self, id: &ReplicaId) -> Option<&Resource<ReplicaState>> {
        self.replicas.get(id)
    }

    /// Get a snapshot with the given ID.
    pub(crate) fn snapshot_state(
        &self,
        id: &SnapshotId,
    ) -> Option<&Resource<ReplicaSnapshotState>> {
        self.snapshots.get(id)
    }

    /// This updates rebuild history resource map.
    pub(crate) fn update_rebuild_history(&mut self, state: RebuildHistoryState) {
        if state.start_time.is_none() {
            self.rebuild_history.clear();
            self.rebuild_history
                .populate(state.history.into_values().collect::<Vec<_>>());
        } else {
            let max_entries = state.max_entries as usize;

            // for now, retain only if the nexus is still alive, in the future we may
            // want to persist even after the nexus is gone so we can keep track of
            // rebuilds per volume.
            self.rebuild_history.update(state.history, |existing, new| {
                let len = existing.records.len() + new.records.len();
                let records = existing.records.iter().cloned().chain(new.records);
                let records = if len > max_entries {
                    records
                        .sorted_by(|a, b| b.end_time.cmp(&a.end_time))
                        .take(max_entries)
                        .collect::<Vec<_>>()
                } else {
                    records.collect::<Vec<_>>()
                };
                *existing = RebuildHistory {
                    uuid: existing.uuid.clone(),
                    name: existing.name.clone(),
                    records,
                }
                .into();
            });
        }
        self.rebuild_history_since = state.end_time;
    }

    /// Get a rebuild history with the given ID.
    pub(crate) fn rebuild_history(&self, id: &NexusId) -> Option<&Resource<RebuildHistory>> {
        self.rebuild_history.get(id)
    }

    /// Get a rebuild history with the given ID.
    pub(crate) fn rebuild_history_time(&self) -> Option<prost_types::Timestamp> {
        self.rebuild_history_since.clone()
    }

    /// Clear all state information.
    pub(crate) fn clear_all(&mut self) {
        self.nexuses.clear();
        self.pools.clear();
        self.replicas.clear();
        self.snapshots.clear();
        self.rebuild_history_since = None;
        self.rebuild_history.clear();
    }

    /// Takes an iterator of resources resourced by an 'Arc' and 'Mutex' and returns a vector of
    /// unprotected resources.
    fn cloned_inner_states<I, S>(locked_states: Values<I, Resource<S>>) -> Vec<S>
    where
        S: Clone,
    {
        locked_states
            .into_iter()
            .map(|s| s.inner().clone())
            .collect()
    }
}

/// Rebuild History State fetched from dataplane.
#[derive(Default, Clone)]
pub(crate) struct RebuildHistoryState {
    /// Maximum number of entries to keep per nexus.
    pub(crate) max_entries: u32,
    /// The start_time used to fetch the rebuilds.
    pub(crate) start_time: Option<prost_types::Timestamp>,
    /// The end_time returned by the dataplane.
    /// Future fetches must use this as start_time.
    pub(crate) end_time: Option<prost_types::Timestamp>,
    /// The actual rebuild history.
    pub(crate) history: HashMap<NexusId, RebuildHistory>,
}

#[tokio::test]
async fn rebuild_updates() {
    use std::ops::Add;
    use stor_port::types::v0::transport::RebuildRecord;

    fn nexus_id(id: &str) -> NexusId {
        id.try_into().unwrap()
    }
    fn from_time(end_time: std::time::SystemTime) -> RebuildRecord {
        RebuildRecord {
            child_uri: Default::default(),
            src_uri: Default::default(),
            state: Default::default(),
            blocks_total: 0,
            blocks_recovered: 0,
            blocks_transferred: 0,
            blocks_remaining: 0,
            blocks_per_task: 0,
            block_size: 0,
            is_partial: false,
            start_time: std::time::SystemTime::now(),
            end_time,
        }
    }

    fn make_rebuilds(
        start_time: bool,
        nexus: &NexusId,
        records: Vec<RebuildRecord>,
        nexus2: &NexusId,
    ) -> RebuildHistoryState {
        let time = std::time::SystemTime::now();
        let start_time = start_time.then(|| prost_types::Timestamp::from(time));
        RebuildHistoryState {
            max_entries: 8,
            start_time,
            end_time: Some(time.into()),
            history: HashMap::from([
                (
                    nexus.clone(),
                    RebuildHistory {
                        uuid: nexus.clone(),
                        name: "".to_string(),
                        records,
                    },
                ),
                (
                    nexus2.clone(),
                    RebuildHistory {
                        uuid: nexus2.clone(),
                        name: "".to_string(),
                        records: vec![],
                    },
                ),
            ]),
        }
    }

    let mut states = ResourceStates::default();
    assert_eq!(states.rebuild_history.len(), 0);

    let nexus = nexus_id("f8aabd1b-14fc-4680-b68f-bff71d779b00");
    let nexus2 = nexus_id("f8aabd1b-14fc-4680-b68f-bff71d779b01");

    let rebuilds = RebuildHistoryState {
        max_entries: 8,
        start_time: None,
        end_time: None,
        history: HashMap::from([
            (
                nexus.clone(),
                RebuildHistory {
                    uuid: nexus.clone(),
                    ..Default::default()
                },
            ),
            (
                nexus2.clone(),
                RebuildHistory {
                    uuid: nexus2.clone(),
                    ..Default::default()
                },
            ),
        ]),
    };

    states.update_rebuild_history(rebuilds.clone());
    assert_eq!(states.rebuild_history.len(), 2);
    states.update_rebuild_history(rebuilds);
    let history = states.rebuild_history(&nexus).cloned().unwrap();
    assert_eq!(history.inner().records.len(), 0);
    let history = states.rebuild_history(&nexus2).cloned().unwrap();
    assert_eq!(history.inner().records.len(), 0);

    let time = std::time::SystemTime::now();
    let rebuilds = make_rebuilds(true, &nexus, vec![from_time(time)], &nexus2);
    states.update_rebuild_history(rebuilds);
    assert_eq!(states.rebuild_history.len(), 2);
    let history = states.rebuild_history(&nexus).cloned().unwrap();
    assert_eq!(history.inner().records.len(), 1);

    let time = std::time::SystemTime::now();
    let rebuilds = make_rebuilds(true, &nexus, vec![from_time(time)], &nexus2);
    states.update_rebuild_history(rebuilds);
    assert_eq!(states.rebuild_history.len(), 2);
    let history = states.rebuild_history(&nexus).cloned().unwrap();
    assert_eq!(history.inner().records.len(), 2);

    let time = std::time::SystemTime::now();
    let rebuilds = make_rebuilds(
        true,
        &nexus,
        vec![from_time(time), from_time(time)],
        &nexus2,
    );
    states.update_rebuild_history(rebuilds);
    assert_eq!(states.rebuild_history.len(), 2);
    let history = states.rebuild_history(&nexus).cloned().unwrap();
    assert_eq!(history.inner().records.len(), 4);

    let rebuilds = make_rebuilds(true, &nexus, vec![], &nexus2);
    states.update_rebuild_history(rebuilds);
    assert_eq!(states.rebuild_history.len(), 2);
    let history = states.rebuild_history(&nexus).cloned().unwrap();
    assert_eq!(history.inner().records.len(), 4);

    let time = std::time::SystemTime::now();
    let rebuilds = make_rebuilds(
        true,
        &nexus,
        vec![
            from_time(time),
            from_time(time.add(std::time::Duration::from_millis(1))),
            from_time(time.add(std::time::Duration::from_millis(2))),
            from_time(time.add(std::time::Duration::from_millis(3))),
            from_time(time.add(std::time::Duration::from_millis(4))),
            from_time(time.add(std::time::Duration::from_millis(5))),
        ],
        &nexus2,
    );
    states.update_rebuild_history(rebuilds);
    assert_eq!(states.rebuild_history.len(), 2);
    let history = states.rebuild_history(&nexus).cloned().unwrap();
    assert_eq!(history.inner().records.len(), 8);

    std::thread::sleep(std::time::Duration::from_millis(20));
    let time = std::time::SystemTime::now();
    let rebuilds = make_rebuilds(
        true,
        &nexus,
        vec![from_time(time)].into_iter().cycle().take(10).collect(),
        &nexus2,
    );
    states.update_rebuild_history(rebuilds);
    assert_eq!(states.rebuild_history.len(), 2);
    let history = states.rebuild_history(&nexus).cloned().unwrap();
    assert_eq!(history.inner().records.len(), 8);
    history.inner().records.iter().for_each(|r| {
        assert_eq!(r.end_time, time);
    });

    let time = std::time::SystemTime::now();
    let rebuilds = RebuildHistoryState {
        max_entries: 8,
        start_time: Some(time.into()),
        end_time: Some(time.into()),
        history: HashMap::from([(
            nexus2.clone(),
            RebuildHistory {
                uuid: nexus2,
                name: "".to_string(),
                records: vec![],
            },
        )]),
    };
    states.update_rebuild_history(rebuilds);
    assert_eq!(states.rebuild_history.len(), 1);
}
