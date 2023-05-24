use crate::{
    common, misc::traits::ValidateRequestTypes, operations::snapshot::SnapshotInfo, snapshot,
    volume, volume::get_snapshots_request,
};

use stor_port::{
    transport_api::{ReplyError, ResourceKind},
    types::v0::transport::{Filter, ReplicaId, VolumeId},
    IntoOption,
};

use std::{collections::HashMap, convert::TryFrom};
use stor_port::types::v0::{
    store::SpecStatus,
    transport::{SnapshotId, SnapshotTxId},
};

/// Volume snapshot information.
pub trait IVolumeSnapshot: Send + Sync + std::fmt::Debug {
    /// Snapshot creation information.
    fn info(&self) -> VolumeSnapshotInfo;
}

/// Volume snapshot create information.
pub type VolumeSnapshotInfo = SnapshotInfo<VolumeId>;

/// A volume snapshot.
#[derive(Debug)]
#[allow(unused)]
pub struct VolumeSnapshot {
    spec: VolumeSnapshotSpec,
    meta: VolumeSnapshotMeta,
    state: VolumeSnapshotState,
}
impl From<&stor_port::types::v0::store::snapshots::volume::VolumeSnapshot> for VolumeSnapshot {
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
                transactions: Default::default(),
            },
            state: VolumeSnapshotState {},
        }
    }
}

/// Volume snapshot specification.
pub type VolumeSnapshotSpec = VolumeSnapshotInfo;

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

/// Volume snapshot state information.
#[derive(Debug)]
pub struct VolumeSnapshotState {}

/// Collection of volume snapshots.
pub struct VolumeSnapshots {
    snapshots: Vec<VolumeSnapshot>,
}

/// Validated create/delete volume snapshot parameters.
pub type CreateVolumeSnapshot = VolumeSnapshotInfo;

/// Validated delete volume snapshot parameters.
pub type DeleteVolumeSnapshot = VolumeSnapshotInfo;

impl ValidateRequestTypes for volume::CreateSnapshotRequest {
    type Validated = CreateVolumeSnapshot;
    fn validated(self) -> Result<Self::Validated, ReplyError> {
        Ok(VolumeSnapshotInfo {
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
        Ok(VolumeSnapshotInfo {
            source_id: self.volume_id.try_into().map_err(|error| {
                ReplyError::invalid_argument(ResourceKind::VolumeSnapshot, "volume_id", error)
            })?,
            snap_id: self.snapshot_id.try_into().map_err(|error| {
                ReplyError::invalid_argument(ResourceKind::VolumeSnapshot, "snapshot_id", error)
            })?,
        })
    }
}

impl IVolumeSnapshot for VolumeSnapshotInfo {
    fn info(&self) -> VolumeSnapshotInfo {
        self.clone()
    }
}

impl From<&dyn IVolumeSnapshot> for volume::CreateSnapshotRequest {
    fn from(value: &dyn IVolumeSnapshot) -> Self {
        let info = value.info();
        Self {
            volume_id: info.source_id.to_string(),
            snapshot_id: info.snap_id.to_string(),
        }
    }
}
impl From<&dyn IVolumeSnapshot> for volume::DeleteSnapshotRequest {
    fn from(value: &dyn IVolumeSnapshot) -> Self {
        let info = value.info();
        Self {
            volume_id: info.source_id.to_string(),
            snapshot_id: info.snap_id.to_string(),
        }
    }
}

impl TryFrom<volume::VolumeSnapshots> for VolumeSnapshots {
    type Error = ReplyError;
    fn try_from(value: volume::VolumeSnapshots) -> Result<Self, Self::Error> {
        Ok(Self {
            snapshots: value
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
        let _state = value
            .state
            .ok_or_else(|| ReplyError::missing_argument(ResourceKind::VolumeSnapshot, "state"))?;

        Ok(Self {
            spec: VolumeSnapshotSpec {
                source_id: VolumeId::try_from(spec.volume_id).map_err(|e| {
                    ReplyError::invalid_argument(ResourceKind::VolumeSnapshot, "spec.volume_id", e)
                })?,
                snap_id: SnapshotId::try_from(spec.snapshot_id).map_err(|e| {
                    ReplyError::invalid_argument(
                        ResourceKind::VolumeSnapshot,
                        "spec.snapshot_id",
                        e,
                    )
                })?,
            },
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
            state: VolumeSnapshotState {},
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
                    size_referenced: 0,
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
                .snapshots
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
            get_snapshots_request::Filter::Snapshot(filter) => Filter::VolumeSnapshot(
                VolumeId::try_from(filter.volume_id).map_err(|error| {
                    ReplyError::invalid_argument(ResourceKind::VolumeSnapshot, "volume_id", error)
                })?,
                SnapshotId::try_from(filter.snapshot_id).map_err(|error| {
                    ReplyError::invalid_argument(ResourceKind::VolumeSnapshot, "snapshot_id", error)
                })?,
            ),
        })
    }
}
