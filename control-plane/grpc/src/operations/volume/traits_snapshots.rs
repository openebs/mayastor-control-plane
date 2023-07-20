use crate::{
    common, misc::traits::ValidateRequestTypes, operations::snapshot::SnapshotInfo, snapshot,
    volume, volume::get_snapshots_request,
};

use stor_port::{
    transport_api::{ReplyError, ResourceKind},
    types::v0::{
        self,
        store::{
            snapshots::{
                replica::{ReplicaSnapshotSource, ReplicaSnapshotSpec},
                ReplicaSnapshotState,
            },
            SpecStatus,
        },
        transport::{
            self, Filter, PoolId, PoolUuid, ReplicaId, SnapshotId, SnapshotTxId, VolumeId,
        },
    },
    IntoOption,
};

use std::{collections::HashMap, convert::TryFrom, time::UNIX_EPOCH};

/// Volume snapshot creation information.
pub trait CreateVolumeSnapshotInfo: Send + Sync + std::fmt::Debug {
    /// Snapshot creation information.
    fn info(&self) -> SnapshotInfo<VolumeId>;
}

/// Volume snapshot deletion information.
pub trait DestroyVolumeSnapshotInfo: Send + Sync + std::fmt::Debug {
    /// Snapshot creation information.
    fn info(&self) -> SnapshotInfo<Option<VolumeId>>;
}

/// A volume snapshot.
#[derive(Debug)]
pub struct VolumeSnapshot {
    spec: VolumeSnapshotSpec,
    meta: VolumeSnapshotMeta,
    state: VolumeSnapshotState,
}
impl VolumeSnapshot {
    /// Create a new `Self` from the given definition and state.
    pub fn new(def: impl Into<VolumeSnapshotDef>, state: VolumeSnapshotState) -> Self {
        let def = def.into();
        Self {
            spec: def.spec,
            meta: def.meta,
            state,
        }
    }
    /// Get the volume snapshot specification.
    pub fn spec(&self) -> &VolumeSnapshotSpec {
        &self.spec
    }
    /// Get the volume snapshot metadata.
    pub fn meta(&self) -> &VolumeSnapshotMeta {
        &self.meta
    }
    /// Get the volume snapshot state.
    pub fn state(&self) -> &VolumeSnapshotState {
        &self.state
    }
}

impl From<&VolumeSnapshot> for DestroyVolumeSnapshot {
    fn from(snapshot: &VolumeSnapshot) -> Self {
        Self::new(
            &Some(snapshot.spec().source_id.clone()),
            snapshot.spec().snap_id.clone(),
        )
    }
}

/// A volume snapshot definition.
#[derive(Debug)]
pub struct VolumeSnapshotDef {
    spec: VolumeSnapshotSpec,
    meta: VolumeSnapshotMeta,
}

impl From<&stor_port::types::v0::store::snapshots::volume::VolumeSnapshot> for VolumeSnapshotDef {
    fn from(value: &stor_port::types::v0::store::snapshots::volume::VolumeSnapshot) -> Self {
        let transactions = value
            .metadata()
            .transactions()
            .iter()
            .map(|(k, v)| (k.to_string(), v.iter().map(From::from).collect::<Vec<_>>()))
            .collect::<HashMap<String, Vec<ReplicaSnapshot>>>();
        Self {
            spec: SnapshotInfo {
                source_id: value.spec().source_id().clone(),
                snap_id: value.spec().uuid().clone(),
            },
            meta: VolumeSnapshotMeta {
                status: value.status().clone(),
                timestamp: value
                    .metadata()
                    .timestamp()
                    .map(|t| std::time::SystemTime::from(t).into()),
                size: value.metadata().size(),
                spec_size: value.metadata().spec_size(),
                total_allocated_size: value.metadata().total_allocated_size(),
                txn_id: value.metadata().txn_id().clone(),
                transactions,
            },
        }
    }
}

/// Volume snapshot specification.
pub type VolumeSnapshotSpec = SnapshotInfo<VolumeId>;

/// Volume snapshot meta information.
#[derive(Debug)]
#[allow(unused)]
pub struct VolumeSnapshotMeta {
    /// Status of the snapshot.
    status: SpecStatus<()>,

    /// Creation timestamp of the snapshot (set after creation time).
    timestamp: Option<prost_types::Timestamp>,
    /// Size of the snapshot (typically follows source size).
    size: u64,
    /// Spec size of the snapshot (typically follows source spec size).
    spec_size: u64,
    /// Total allocated size of the snapshot and its predecessors.
    total_allocated_size: u64,
    /// Transaction Id that defines this snapshot when it is created.
    txn_id: SnapshotTxId,
    /// Replicas which "reference" to this snapshot as its parent, indexed by the transaction
    /// id when they were attempted.
    /// The "actual" snapshots can be accessed by the key `txn_id`.
    /// Failed transactions are any other key.
    transactions: HashMap<String, Vec<ReplicaSnapshot>>,
}
impl VolumeSnapshotMeta {
    /// Get the volume snapshot status.
    pub fn status(&self) -> &SpecStatus<()> {
        &self.status
    }
    /// Get the volume snapshot timestamp.
    pub fn timestamp(&self) -> Option<&prost_types::Timestamp> {
        self.timestamp.as_ref()
    }
    /// Get the volume snapshot transaction id.
    pub fn txn_id(&self) -> &SnapshotTxId {
        &self.txn_id
    }
    /// Get a reference to the transactions hashmap.
    pub fn transactions(&self) -> &HashMap<String, Vec<ReplicaSnapshot>> {
        &self.transactions
    }
    /// Get the snapshot size in bytes.
    pub fn size(&self) -> u64 {
        self.size
    }
    /// Get the snapshot spec size in bytes.
    pub fn spec_size(&self) -> u64 {
        self.spec_size
    }
    /// Get the snapshot total allocated size in bytes.
    pub fn total_allocated_size(&self) -> u64 {
        self.total_allocated_size
    }
}

/// Volume replica snapshot information.
#[derive(Debug)]
#[allow(unused)]
pub struct ReplicaSnapshot {
    /// Status of the snapshot.
    status: SpecStatus<()>,
    /// The id of the snapshot.
    uuid: SnapshotId,
    /// Creation timestamp of the snapshot (set after creation time).
    timestamp: Option<prost_types::Timestamp>,
    /// A transaction id for this request.
    txn_id: SnapshotTxId,
    source_id: ReplicaId,
}
impl ReplicaSnapshot {
    /// Get the status of this replica snapshot.
    pub fn status(&self) -> SpecStatus<()> {
        self.status.clone()
    }
    /// Get the uuid of this replica snapshot.
    pub fn uuid(&self) -> &uuid::Uuid {
        self.uuid.uuid()
    }
    /// Get the source id of this replica snapshot.
    pub fn source_id(&self) -> &uuid::Uuid {
        self.source_id.uuid()
    }
}
impl From<&v0::store::snapshots::replica::ReplicaSnapshot> for ReplicaSnapshot {
    fn from(value: &v0::store::snapshots::replica::ReplicaSnapshot) -> ReplicaSnapshot {
        ReplicaSnapshot {
            status: value.status().clone(),
            uuid: value.spec().uuid().clone(),
            timestamp: value.meta().timestamp().map(|t| prost_types::Timestamp {
                seconds: t.timestamp(),
                nanos: t.timestamp_subsec_nanos() as i32,
            }),
            txn_id: value
                .meta()
                .txn_id()
                .cloned()
                .unwrap_or(SnapshotTxId::default()),
            source_id: value.spec().source_id().replica_id().clone(),
        }
    }
}

/// Volume replica snapshot state information.
#[derive(Debug)]
pub enum VolumeReplicaSnapshotState {
    /// When the replica snapshot is available.
    Online {
        /// The pool hosting the replica snapshot.
        pool_id: PoolId,
        /// The replica snapshot state.
        state: transport::ReplicaSnapshot,
    },
    /// When the replica snapshot is unavailable.
    Offline {
        /// The source id of the replica snapshot.
        replica_id: ReplicaId,
        /// The pool hosting the replica snapshot.
        pool_id: PoolId,
        /// The uuid of the pool hosting the replica snapshot.
        pool_uuid: PoolUuid,
        /// The replica snapshot id.
        snapshot_id: SnapshotId,
    },
}
impl VolumeReplicaSnapshotState {
    /// Create a new volume replica snapshot state when the state is present.
    pub fn new_online(spec: &ReplicaSnapshotSpec, state: transport::ReplicaSnapshot) -> Self {
        Self::Online {
            pool_id: spec.source_id().pool_id().clone(),
            state,
        }
    }
    /// Create a new volume replica snapshot state when the state is unavailable.
    pub fn new_offline(spec: &ReplicaSnapshotSpec) -> Self {
        Self::Offline {
            pool_uuid: spec.source_id().pool_uuid().clone(),
            pool_id: spec.source_id().pool_id().clone(),
            replica_id: spec.source_id().replica_id().clone(),
            snapshot_id: spec.uuid().clone(),
        }
    }
    /// Get the snapshot state, if the snapshot is online.
    pub fn state(&self) -> Option<&transport::ReplicaSnapshot> {
        match self {
            Self::Online { state, .. } => Some(state),
            _ => None,
        }
    }
}

/// Volume snapshot state information.
#[derive(Debug)]
pub struct VolumeSnapshotState {
    info: SnapshotInfo<VolumeId>,
    allocated_size: Option<u64>,
    timestamp: Option<prost_types::Timestamp>,
    repl_snapshots: Vec<VolumeReplicaSnapshotState>,
}
impl VolumeSnapshotState {
    /// Create a new `Self` with the given parameters.
    pub fn new(
        volume_snapshot: &v0::store::snapshots::volume::VolumeSnapshot,
        allocated_size: Option<u64>,
        repl_snapshots: Vec<VolumeReplicaSnapshotState>,
    ) -> Self {
        let spec = volume_snapshot.spec();
        let info = CreateVolumeSnapshot::new(spec.source_id(), spec.uuid().clone());
        let timestamp = volume_snapshot
            .metadata()
            .timestamp()
            .map(|t| prost_types::Timestamp {
                seconds: t.timestamp(),
                nanos: t.timestamp_subsec_nanos() as i32,
            });
        Self {
            info,
            allocated_size,
            timestamp,
            repl_snapshots,
        }
    }
    /// Get the volume snapshot uuid.
    pub fn uuid(&self) -> &SnapshotId {
        self.info.snap_id()
    }
    /// Get the volume snapshot source id.
    pub fn source_id(&self) -> &VolumeId {
        self.info.source_id()
    }
    /// Get the volume snapshot allocated size.
    pub fn allocated_size(&self) -> Option<u64> {
        self.allocated_size
    }
    /// Get the volume snapshot creation timestamp.
    pub fn timestamp(&self) -> Option<&prost_types::Timestamp> {
        self.timestamp.as_ref()
    }
    /// Get the volume snapshot readiness to be used as a source.
    pub fn ready_as_source(&self) -> bool {
        match self.repl_snapshots.as_slice() {
            [VolumeReplicaSnapshotState::Online { state, .. }] => state.ready_as_source(),
            _ => false, // todo: handle more than one replica for multi-replica snapshots
        }
    }
    /// Get a reference to the replica snapshots.
    pub fn repl_snapshots(&self) -> &Vec<VolumeReplicaSnapshotState> {
        &self.repl_snapshots
    }
}

/// Collection of volume snapshots.
// next_token field to be added here for pagination.
#[derive(Default, Debug)]
pub struct VolumeSnapshots {
    /// Snapshot entries.
    pub entries: Vec<VolumeSnapshot>,
    /// Next token for the paginated response.
    pub next_token: Option<u64>,
}

impl VolumeSnapshots {
    /// Get a refernce to all the snapshot entries.
    pub fn entries(&self) -> &Vec<VolumeSnapshot> {
        &self.entries
    }

    /// Get the next token for pagination.
    pub fn next_token(&self) -> Option<u64> {
        self.next_token
    }
}

/// Validated create/delete volume snapshot parameters.
pub type CreateVolumeSnapshot = SnapshotInfo<VolumeId>;

/// Validated delete volume snapshot parameters.
pub type DestroyVolumeSnapshot = SnapshotInfo<Option<VolumeId>>;

impl ValidateRequestTypes for volume::CreateSnapshotRequest {
    type Validated = CreateVolumeSnapshot;
    fn validated(self) -> Result<Self::Validated, ReplyError> {
        Ok(CreateVolumeSnapshot {
            source_id: self
                .volume_id
                .try_into_id(ResourceKind::VolumeSnapshot, "volume_id")?,
            snap_id: self
                .snapshot_id
                .try_into_id(ResourceKind::VolumeSnapshot, "volume_id")?,
        })
    }
}
impl ValidateRequestTypes for volume::DestroySnapshotRequest {
    type Validated = DestroyVolumeSnapshot;
    fn validated(self) -> Result<Self::Validated, ReplyError> {
        Ok(DestroyVolumeSnapshot {
            source_id: match self.volume_id {
                None => None,
                Some(id) => Some(id.try_into_id(ResourceKind::VolumeSnapshot, "volume_id")?),
            },
            snap_id: self
                .snapshot_id
                .try_into_id(ResourceKind::VolumeSnapshot, "snapshot_id")?,
        })
    }
}

impl CreateVolumeSnapshotInfo for CreateVolumeSnapshot {
    fn info(&self) -> CreateVolumeSnapshot {
        self.clone()
    }
}
impl DestroyVolumeSnapshotInfo for DestroyVolumeSnapshot {
    fn info(&self) -> DestroyVolumeSnapshot {
        self.clone()
    }
}

impl From<&dyn CreateVolumeSnapshotInfo> for volume::CreateSnapshotRequest {
    fn from(value: &dyn CreateVolumeSnapshotInfo) -> Self {
        let info = value.info();
        Self {
            volume_id: info.source_id.to_string(),
            snapshot_id: info.snap_id.to_string(),
        }
    }
}
impl From<&dyn DestroyVolumeSnapshotInfo> for volume::DestroySnapshotRequest {
    fn from(value: &dyn DestroyVolumeSnapshotInfo) -> Self {
        let info = value.info();
        Self {
            volume_id: info.source_id.map(|id| id.to_string()),
            snapshot_id: info.snap_id.to_string(),
        }
    }
}

impl TryFrom<volume::VolumeSnapshots> for VolumeSnapshots {
    type Error = ReplyError;
    fn try_from(value: volume::VolumeSnapshots) -> Result<Self, Self::Error> {
        Ok(Self {
            entries: value
                .snapshots
                .into_iter()
                .map(TryFrom::try_from)
                .collect::<Result<Vec<VolumeSnapshot>, ReplyError>>()?,
            next_token: value.next_token,
        })
    }
}

impl TryFrom<volume::VolumeSnapshot> for VolumeSnapshot {
    type Error = ReplyError;
    fn try_from(value: volume::VolumeSnapshot) -> Result<Self, Self::Error> {
        let spec = value
            .spec
            .ok_or_else(|| ReplyError::missing_argument(ResourceKind::VolumeSnapshot, "spec"))?;
        let meta = value
            .meta
            .ok_or_else(|| ReplyError::missing_argument(ResourceKind::VolumeSnapshot, "meta"))?;
        let state = value
            .state
            .ok_or_else(|| ReplyError::missing_argument(ResourceKind::VolumeSnapshot, "state"))?;

        let info = VolumeSnapshotSpec {
            source_id: spec
                .volume_id
                .try_into_id(ResourceKind::VolumeSnapshot, "spec.volume_id")?,
            snap_id: spec
                .snapshot_id
                .try_into_id(ResourceKind::VolumeSnapshot, "spec.snapshot_id")?,
        };
        Ok(Self {
            spec: info.clone(),
            meta: VolumeSnapshotMeta {
                status: common::SpecStatus::from_i32(meta.spec_status)
                    .unwrap_or_default()
                    .into(),
                timestamp: meta.timestamp,
                size: meta.size,
                spec_size: meta.spec_size,
                total_allocated_size: meta.total_allocated_size,
                txn_id: meta.txn_id,
                transactions: meta
                    .transactions
                    .into_iter()
                    .map(|(k, v)| {
                        let snapshots = v
                            .snapshots
                            .into_iter()
                            .map(TryInto::try_into)
                            .collect::<Result<Vec<_>, ReplyError>>();
                        snapshots.map(|s| (k, s))
                    })
                    .collect::<Result<HashMap<_, _>, _>>()?,
            },
            state: VolumeSnapshotState {
                info,
                allocated_size: state.state.as_ref().map(|s| s.allocated_size),
                timestamp: state.state.as_ref().and_then(|s| s.timestamp.clone()),
                repl_snapshots: state
                    .replicas
                    .into_iter()
                    .map(|r| {
                        let state = r.state.unwrap();
                        match state {
                            snapshot::volume_replica_snapshot_state::State::Online(state) => {
                                let n_state = state.clone().try_into()?;
                                let snapshot_id = state
                                    .uuid
                                    .try_into_id(ResourceKind::VolumeSnapshot, "state.uuid")?;
                                let replica_id = state.replica_id.try_into_id(
                                    ResourceKind::VolumeSnapshot,
                                    "state.replica_id",
                                )?;
                                let source = ReplicaSnapshotSource::new(
                                    replica_id,
                                    state.pool_id.into(),
                                    state.pool_uuid.try_into_id(
                                        ResourceKind::VolumeSnapshot,
                                        "state.pool_uuid",
                                    )?,
                                );
                                let spec = ReplicaSnapshotSpec::new(&source, snapshot_id);
                                let status =
                                    crate::snapshot::SnapshotStatus::from_i32(state.status)
                                        .unwrap_or_default();
                                Ok(match status {
                                    crate::snapshot::SnapshotStatus::Unknown => {
                                        VolumeReplicaSnapshotState::new_offline(&spec)
                                    }
                                    crate::snapshot::SnapshotStatus::Online => {
                                        VolumeReplicaSnapshotState::new_online(&spec, n_state)
                                    }
                                })
                            }
                            snapshot::volume_replica_snapshot_state::State::Offline(state) => {
                                let snapshot_id = state
                                    .uuid
                                    .try_into_id(ResourceKind::VolumeSnapshot, "state.uuid")?;
                                let replica_id = state.replica_id.try_into_id(
                                    ResourceKind::VolumeSnapshot,
                                    "state.replica_id",
                                )?;
                                let source = ReplicaSnapshotSource::new(
                                    replica_id,
                                    state.pool_id.into(),
                                    state.pool_uuid.try_into_id(
                                        ResourceKind::VolumeSnapshot,
                                        "state.pool_uuid",
                                    )?,
                                );
                                let spec = ReplicaSnapshotSpec::new(&source, snapshot_id);
                                Ok(VolumeReplicaSnapshotState::new_offline(&spec))
                            }
                        }
                    })
                    .collect::<Result<Vec<_>, ReplyError>>()?,
            },
        })
    }
}

impl TryFrom<&snapshot::ReplicaSnapshotState> for ReplicaSnapshotState {
    type Error = ReplyError;
    fn try_from(val: &snapshot::ReplicaSnapshotState) -> Result<Self, Self::Error> {
        Ok(Self {
            snapshot: transport::ReplicaSnapshot::new(
                val.uuid
                    .clone()
                    .try_into_id(ResourceKind::ReplicaSnapshot, "snapshot_id")?,
                val.name.clone(),
                val.allocated_size,
                val.num_clones,
                val.timestamp
                    .clone()
                    .and_then(|t| std::time::SystemTime::try_from(t).ok())
                    .unwrap_or(UNIX_EPOCH),
                val.replica_id
                    .clone()
                    .try_into_id(ResourceKind::ReplicaSnapshot, "replica_uuid")?,
                val.pool_uuid
                    .clone()
                    .try_into_id(ResourceKind::ReplicaSnapshot, "pool_uuid")?,
                PoolId::from(&val.pool_id),
                val.replica_size,
                val.entity_id.clone(),
                val.txn_id.clone(),
                val.valid,
                val.ready_as_source,
                val.predecessor_alloc_size,
            ),
        })
    }
}

impl TryFrom<volume::ReplicaSnapshot> for ReplicaSnapshot {
    type Error = ReplyError;
    fn try_from(value: volume::ReplicaSnapshot) -> Result<Self, Self::Error> {
        Ok(ReplicaSnapshot {
            status: common::SpecStatus::from_i32(value.spec_status)
                .unwrap_or_default()
                .into(),
            uuid: value
                .uuid
                .try_into_id(ResourceKind::ReplicaSnapshot, "uuid")?,
            timestamp: value.timestamp.into_opt(),
            txn_id: value.txn_id,
            source_id: value
                .source_id
                .try_into_id(ResourceKind::ReplicaSnapshot, "source_id")?,
        })
    }
}
impl TryFrom<VolumeSnapshot> for volume::VolumeSnapshot {
    type Error = ReplyError;
    fn try_from(value: VolumeSnapshot) -> Result<Self, Self::Error> {
        Ok(Self {
            spec: Some(volume::VolumeSnapshotSpec {
                volume_id: value.spec.source_id.to_string(),
                snapshot_id: value.spec.snap_id.to_string(),
            }),
            meta: Some(volume::VolumeSnapshotMeta {
                spec_status: common::SpecStatus::from(&value.meta().status) as i32,
                timestamp: value.meta().timestamp().cloned(),
                txn_id: value.meta().txn_id().clone(),
                transactions: value
                    .meta()
                    .transactions
                    .iter()
                    .map(|(k, v)| {
                        (
                            k.clone(),
                            volume::volume_snapshot_meta::ReplicaSnapshots {
                                snapshots: v
                                    .iter()
                                    .map(|r| volume::ReplicaSnapshot {
                                        uuid: r.uuid.to_string(),
                                        spec_status: common::SpecStatus::from(&r.status) as i32,
                                        timestamp: r.timestamp.clone(),
                                        txn_id: r.txn_id.clone(),
                                        source_id: r.source_id.to_string(),
                                    })
                                    .collect::<Vec<_>>(),
                            },
                        )
                    })
                    .collect::<HashMap<_, _>>(),
                size: value.meta.size,
                spec_size: value.meta.spec_size,
                total_allocated_size: value.meta.total_allocated_size,
            }),
            state: Some(volume::VolumeSnapshotState {
                state: Some(snapshot::SnapshotState {
                    uuid: value.state().uuid().to_string(),
                    status: 0,
                    timestamp: value.state().timestamp().cloned(),
                    allocated_size: value.state().allocated_size().unwrap_or_default(),
                    source_id: value.state().source_id().to_string(),
                }),
                replicas: value
                    .state()
                    .repl_snapshots()
                    .iter()
                    .map(|r| {
                        let state = match r {
                            VolumeReplicaSnapshotState::Online { state, .. } => {
                                crate::snapshot::volume_replica_snapshot_state::State::Online(
                                    crate::snapshot::ReplicaSnapshotState {
                                        uuid: state.snap_uuid().to_string(),
                                        replica_id: state.replica_uuid().to_string(),
                                        pool_uuid: state.pool_uuid().to_string(),
                                        pool_id: state.pool_id().to_string(),
                                        name: state.snap_name().to_string(),
                                        entity_id: state.entity_id().to_string(),
                                        status: crate::snapshot::SnapshotStatus::Online as i32,
                                        timestamp: state.timestamp().try_into().ok(),
                                        replica_size: state.replica_size(),
                                        allocated_size: state.allocated_size(),
                                        num_clones: state.num_clones(),
                                        txn_id: state.txn_id().to_string(),
                                        valid: state.valid(),
                                        ready_as_source: state.ready_as_source(),
                                        predecessor_alloc_size: state.predecessor_alloc_size(),
                                    },
                                )
                            }
                            VolumeReplicaSnapshotState::Offline {
                                replica_id,
                                pool_id,
                                pool_uuid,
                                snapshot_id,
                            } => crate::snapshot::volume_replica_snapshot_state::State::Offline(
                                crate::snapshot::ReplicaSnapshotSourceState {
                                    uuid: snapshot_id.to_string(),
                                    replica_id: replica_id.to_string(),
                                    pool_uuid: pool_uuid.to_string(),
                                    pool_id: pool_id.to_string(),
                                },
                            ),
                        };
                        crate::snapshot::VolumeReplicaSnapshotState { state: Some(state) }
                    })
                    .collect::<Vec<_>>(),
            }),
        })
    }
}

impl TryFrom<VolumeSnapshots> for volume::VolumeSnapshots {
    type Error = ReplyError;
    fn try_from(value: VolumeSnapshots) -> Result<Self, Self::Error> {
        let next_token = value.next_token();
        Ok(Self {
            snapshots: value
                .entries
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?,
            next_token,
        })
    }
}
impl TryFrom<get_snapshots_request::Filter> for Filter {
    type Error = ReplyError;
    fn try_from(filter: get_snapshots_request::Filter) -> Result<Self, Self::Error> {
        Ok(match filter {
            get_snapshots_request::Filter::Volume(filter) => Filter::Volume(
                filter
                    .volume_id
                    .try_into_id(ResourceKind::VolumeSnapshot, "volume_id")?,
            ),
            get_snapshots_request::Filter::VolumeSnapshot(filter) => Filter::VolumeSnapshot(
                filter
                    .volume_id
                    .try_into_id(ResourceKind::VolumeSnapshot, "volume_id")?,
                filter
                    .snapshot_id
                    .try_into_id(ResourceKind::VolumeSnapshot, "snapshot_id")?,
            ),
            get_snapshots_request::Filter::Snapshot(filter) => Filter::Snapshot(
                filter
                    .snapshot_id
                    .try_into_id(ResourceKind::VolumeSnapshot, "snapshot_id")?,
            ),
        })
    }
}

impl From<&SpecStatus<()>> for common::SpecStatus {
    fn from(value: &SpecStatus<()>) -> Self {
        match value {
            SpecStatus::Creating => common::SpecStatus::Creating,
            SpecStatus::Created(_) => common::SpecStatus::Created,
            SpecStatus::Deleting => common::SpecStatus::Deleting,
            SpecStatus::Deleted => common::SpecStatus::Deleted,
        }
    }
}

impl TryFrom<snapshot::ReplicaSnapshotState> for transport::ReplicaSnapshot {
    type Error = ReplyError;
    fn try_from(value: snapshot::ReplicaSnapshotState) -> Result<Self, Self::Error> {
        Ok(transport::ReplicaSnapshot::new(
            value
                .uuid
                .try_into_id(ResourceKind::VolumeSnapshot, "snap_uuid")?,
            value.name,
            value.allocated_size,
            value.num_clones,
            value
                .timestamp
                .and_then(|t| std::time::SystemTime::try_from(t).ok())
                .unwrap_or(UNIX_EPOCH),
            value
                .replica_id
                .try_into_id(ResourceKind::VolumeSnapshot, "replica_id")?,
            value
                .pool_uuid
                .try_into_id(ResourceKind::VolumeSnapshot, "pool_uuid")?,
            value.pool_id.into(),
            value.replica_size,
            value.entity_id,
            value.txn_id,
            value.valid,
            value.ready_as_source,
            value.predecessor_alloc_size,
        ))
    }
}

/// Convert a string to `T`, which can be converted using `TryFrom<String, Error=uuid::Error>`.
trait TryIntoId {
    fn try_into_id<T: TryFrom<String, Error = uuid::Error>>(
        self,
        kind: ResourceKind,
        arg_name: &str,
    ) -> Result<T, ReplyError>
    where
        Self: Sized + Into<String>,
    {
        T::try_from(self.into())
            .map_err(|error| ReplyError::invalid_argument(kind, arg_name, error))
    }
}

impl TryIntoId for String {}
