use crate::{
    common, misc::traits::ValidateRequestTypes, operations::snapshot::SnapshotInfo, snapshot,
    volume, volume::get_snapshots_request,
};

use stor_port::{
    transport_api::{ReplyError, ResourceKind},
    types::v0::{
        self,
        store::{
            snapshots::{replica::ReplicaSnapshotSpec, ReplicaSnapshotState},
            SpecStatus,
        },
        transport,
        transport::{Filter, PoolId, ReplicaId, SnapshotId, SnapshotTxId, VolumeId},
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
pub trait DeleteVolumeSnapshotInfo: Send + Sync + std::fmt::Debug {
    /// Snapshot creation information.
    fn info(&self) -> SnapshotInfo<Option<VolumeId>>;
}

/// A volume snapshot.
#[derive(Debug)]
#[allow(unused)]
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

/// A volume snapshot definition.
#[derive(Debug)]
pub struct VolumeSnapshotDef {
    spec: VolumeSnapshotSpec,
    meta: VolumeSnapshotMeta,
}

impl From<&stor_port::types::v0::store::snapshots::volume::VolumeSnapshot> for VolumeSnapshotDef {
    fn from(value: &stor_port::types::v0::store::snapshots::volume::VolumeSnapshot) -> Self {
        Self {
            spec: SnapshotInfo {
                source_id: value.spec().source_id().clone(),
                snap_id: value.spec().uuid().clone(),
            },
            meta: VolumeSnapshotMeta {
                status: value.status().clone(),
                creation_timestamp: value
                    .metadata()
                    .timestamp()
                    .map(|t| std::time::SystemTime::from(t).into()),
                txn_id: value.metadata().txn_id().clone(),
                // todo: add replica transactions..
                transactions: Default::default(),
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
    creation_timestamp: Option<prost_types::Timestamp>,

    /// Transaction Id that defines this snapshot when it is created.
    txn_id: SnapshotTxId,
    /// Replicas which "reference" to this snapshot as its parent, indexed by the transaction
    /// id when they were attempted.
    /// The "actual" snapshots can be accessed by the key `txn_id`.
    /// Failed transactions are any other key.
    transactions: HashMap<String, Vec<ReplicaSnapshot>>,
}
impl VolumeSnapshotMeta {
    /// Get the volume snapshot timestamp.
    pub fn timestamp(&self) -> Option<&prost_types::Timestamp> {
        self.creation_timestamp.as_ref()
    }
    /// Get the volume snapshot transaction id.
    pub fn txn_id(&self) -> &SnapshotTxId {
        &self.txn_id
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
    creation_timestamp: Option<prost_types::Timestamp>,
    /// A transaction id for this request.
    txn_id: SnapshotTxId,
    source_id: ReplicaId,
}

/// Volume replica snapshot state information.
#[derive(Debug)]
#[allow(unused)]
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
            pool_id: spec.source_id().pool_id().clone(),
            replica_id: spec.source_id().replica_id().clone(),
            snapshot_id: spec.uuid().clone(),
        }
    }
}

/// Volume snapshot state information.
#[derive(Debug)]
pub struct VolumeSnapshotState {
    info: SnapshotInfo<VolumeId>,
    size: Option<u64>,
    timestamp: Option<prost_types::Timestamp>,
    repl_snapshots: Vec<VolumeReplicaSnapshotState>,
}
impl VolumeSnapshotState {
    /// Create a new `Self` with the given parameters.
    pub fn new(
        volume_snapshot: &v0::store::snapshots::volume::VolumeSnapshot,
        size: Option<u64>,
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
            size,
            timestamp,
            repl_snapshots,
        }
    }
    /// Get the volume snapshot state.
    pub fn uuid(&self) -> &SnapshotId {
        self.info.snap_id()
    }
    /// Get the volume snapshot state.
    pub fn source_id(&self) -> &VolumeId {
        self.info.source_id()
    }
    /// Get the volume snapshot state.
    pub fn size(&self) -> Option<u64> {
        self.size
    }
    /// Get the volume snapshot state.
    pub fn timestamp(&self) -> Option<&prost_types::Timestamp> {
        self.timestamp.as_ref()
    }
    /// Get the volume snapshot state.
    pub fn clone_ready(&self) -> bool {
        false
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
}

impl VolumeSnapshots {
    /// Get a refernce to all the snapshot entries.
    pub fn entries(&self) -> &Vec<VolumeSnapshot> {
        &self.entries
    }
}

/// Validated create/delete volume snapshot parameters.
pub type CreateVolumeSnapshot = SnapshotInfo<VolumeId>;

/// Validated delete volume snapshot parameters.
pub type DeleteVolumeSnapshot = SnapshotInfo<Option<VolumeId>>;

impl ValidateRequestTypes for volume::CreateSnapshotRequest {
    type Validated = CreateVolumeSnapshot;
    fn validated(self) -> Result<Self::Validated, ReplyError> {
        Ok(CreateVolumeSnapshot {
            source_id: self.volume_id.try_into().map_err(|error| {
                ReplyError::invalid_argument(ResourceKind::VolumeSnapshot, "volume_id", error)
            })?,
            snap_id: self.snapshot_id.try_into().map_err(|error| {
                ReplyError::invalid_argument(ResourceKind::VolumeSnapshot, "snapshot_id", error)
            })?,
        })
    }
}
impl ValidateRequestTypes for volume::DeleteSnapshotRequest {
    type Validated = DeleteVolumeSnapshot;
    fn validated(self) -> Result<Self::Validated, ReplyError> {
        Ok(DeleteVolumeSnapshot {
            source_id: match self.volume_id {
                None => None,
                Some(id) => Some(id.try_into().map_err(|error| {
                    ReplyError::invalid_argument(ResourceKind::VolumeSnapshot, "volume_id", error)
                })?),
            },
            snap_id: self.snapshot_id.try_into().map_err(|error| {
                ReplyError::invalid_argument(ResourceKind::VolumeSnapshot, "snapshot_id", error)
            })?,
        })
    }
}

impl CreateVolumeSnapshotInfo for CreateVolumeSnapshot {
    fn info(&self) -> CreateVolumeSnapshot {
        self.clone()
    }
}
impl DeleteVolumeSnapshotInfo for DeleteVolumeSnapshot {
    fn info(&self) -> DeleteVolumeSnapshot {
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
impl From<&dyn DeleteVolumeSnapshotInfo> for volume::DeleteSnapshotRequest {
    fn from(value: &dyn DeleteVolumeSnapshotInfo) -> Self {
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
            source_id: VolumeId::try_from(spec.volume_id).map_err(|e| {
                ReplyError::invalid_argument(ResourceKind::VolumeSnapshot, "spec.volume_id", e)
            })?,
            snap_id: SnapshotId::try_from(spec.snapshot_id).map_err(|e| {
                ReplyError::invalid_argument(ResourceKind::VolumeSnapshot, "spec.snapshot_id", e)
            })?,
        };
        Ok(Self {
            spec: info.clone(),
            meta: VolumeSnapshotMeta {
                status: common::SpecStatus::from_i32(meta.spec_status)
                    .unwrap_or_default()
                    .into(),
                creation_timestamp: meta.timestamp,
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
                size: state.state.as_ref().map(|s| s.size),
                timestamp: state.state.as_ref().and_then(|s| s.timestamp.clone()),
                // todo: grpc protobuf
                repl_snapshots: vec![],
            },
        })
    }
}

impl TryFrom<&snapshot::ReplicaSnapshotState> for ReplicaSnapshotState {
    type Error = ReplyError;
    fn try_from(val: &snapshot::ReplicaSnapshotState) -> Result<Self, Self::Error> {
        Ok(Self {
            snapshot: transport::ReplicaSnapshot::new(
                SnapshotId::try_from(val.uuid.as_str()).map_err(|_| {
                    ReplyError::invalid_argument(
                        ResourceKind::ReplicaSnapshot,
                        "snap uuid",
                        val.status.to_string(),
                    )
                })?,
                &val.uuid,
                val.size_referenced,
                val.num_clones,
                val.timestamp
                    .clone()
                    .and_then(|t| std::time::SystemTime::try_from(t).ok())
                    .unwrap_or(UNIX_EPOCH),
                ReplicaId::try_from(val.source_id.as_str()).map_err(|_| {
                    ReplyError::invalid_argument(
                        ResourceKind::ReplicaSnapshot,
                        "source(replica) id",
                        val.status.to_string(),
                    )
                })?,
                val.size,
                &val.source_id,
                &val.txn_id,
                val.valid,
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
            uuid: value.uuid.try_into().map_err(|error| {
                ReplyError::invalid_argument(ResourceKind::VolumeSnapshot, "uuid", error)
            })?,
            creation_timestamp: value.timestamp.into_opt(),
            txn_id: value.txn_id,
            source_id: value.source_id.try_into().map_err(|error| {
                ReplyError::invalid_argument(ResourceKind::VolumeSnapshot, "source_id", error)
            })?,
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
                spec_status: 0,
                timestamp: None,
                txn_id: "".to_string(),
                transactions: Default::default(),
            }),
            state: Some(volume::VolumeSnapshotState {
                state: Some(snapshot::SnapshotState {
                    uuid: "".to_string(),
                    status: 0,
                    timestamp: None,
                    size: 0,
                    source_id: "".to_string(),
                }),
                replicas: vec![],
            }),
        })
    }
}

impl TryFrom<VolumeSnapshots> for volume::VolumeSnapshots {
    type Error = ReplyError;
    fn try_from(value: VolumeSnapshots) -> Result<Self, Self::Error> {
        Ok(Self {
            snapshots: value
                .entries
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}
impl TryFrom<get_snapshots_request::Filter> for Filter {
    type Error = ReplyError;
    fn try_from(filter: get_snapshots_request::Filter) -> Result<Self, Self::Error> {
        Ok(match filter {
            get_snapshots_request::Filter::Volume(filter) => {
                Filter::Volume(VolumeId::try_from(filter.volume_id).map_err(|error| {
                    ReplyError::invalid_argument(ResourceKind::VolumeSnapshot, "volume_id", error)
                })?)
            }
            get_snapshots_request::Filter::VolumeSnapshot(filter) => Filter::VolumeSnapshot(
                VolumeId::try_from(filter.volume_id).map_err(|error| {
                    ReplyError::invalid_argument(ResourceKind::VolumeSnapshot, "volume_id", error)
                })?,
                SnapshotId::try_from(filter.snapshot_id).map_err(|error| {
                    ReplyError::invalid_argument(ResourceKind::VolumeSnapshot, "snapshot_id", error)
                })?,
            ),
            get_snapshots_request::Filter::Snapshot(filter) => {
                Filter::Snapshot(SnapshotId::try_from(filter.snapshot_id).map_err(|error| {
                    ReplyError::invalid_argument(ResourceKind::VolumeSnapshot, "snapshot_id", error)
                })?)
            }
        })
    }
}
