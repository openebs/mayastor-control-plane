use super::{SnapshotId, SnapshotSpec};
use crate::types::v0::{
    store::{AsOperationSequencer, OperationSequence, SpecStatus, SpecTransaction},
    transport::{PoolId, PoolUuid, ReplicaId, SnapshotParameters, SnapshotTxId, VolumeId},
};
use chrono::{DateTime, Utc};
use pstor::{ApiVersion, ObjectKey, StorableObject, StorableObjectType};
use serde::{Deserialize, Serialize};

/// User specification of a replica snapshot source.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct ReplicaSnapshotSource {
    replica_id: ReplicaId,
    pool_id: PoolId,
    pool_uuid: PoolUuid,
}
impl ReplicaSnapshotSource {
    /// Create a new `Self` from the given parameters.
    pub fn new(replica_id: ReplicaId, pool_id: PoolId, pool_uuid: PoolUuid) -> Self {
        Self {
            replica_id,
            pool_id,
            pool_uuid,
        }
    }
    /// Get the snapshot source id.
    pub fn replica_id(&self) -> &ReplicaId {
        &self.replica_id
    }
    /// Get the snapshot pool id.
    pub fn pool_id(&self) -> &PoolId {
        &self.pool_id
    }
    /// Get the snapshot pool uuid.
    pub fn pool_uuid(&self) -> &PoolUuid {
        &self.pool_uuid
    }
}

/// User specification of a replica snapshot.
pub type ReplicaSnapshotSpec = SnapshotSpec<ReplicaSnapshotSource>;
/// State of the ReplicaSnapshotSpec Spec.
pub type ReplicaSnapshotSpecStatus = SpecStatus<()>;

/// Replica snapshot definition for the pstor.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct ReplicaSnapshot {
    /// Status of the replica snapshot.
    status: ReplicaSnapshotSpecStatus,
    /// User specification of the snapshot.
    spec: ReplicaSnapshotSpec,
    /// Control-plane related information of the snapshot (book-keeping).
    metadata: ReplicaSnapshotMeta,
}
impl ReplicaSnapshot {
    /// Return a new `Self` as a volume replica snapshot.
    pub fn new_vol(
        spec: ReplicaSnapshotSpec,
        vol_params: SnapshotParameters<VolumeId>,
        size: u64,
        allocated_size: u64,
        source_spec_size: u64,
    ) -> Self {
        Self {
            status: ReplicaSnapshotSpecStatus::Creating,
            metadata: ReplicaSnapshotMeta::new(
                vol_params.uuid(),
                vol_params.txn_id(),
                size,
                source_spec_size,
                allocated_size,
            ),
            spec,
        }
    }
    /// Get a reference to the replica spec.
    pub fn spec(&self) -> &ReplicaSnapshotSpec {
        &self.spec
    }
    /// Get a reference to the replica status.
    pub fn status(&self) -> &ReplicaSnapshotSpecStatus {
        &self.status
    }
    /// Get a reference to the replica metadata.
    pub fn meta(&self) -> &ReplicaSnapshotMeta {
        &self.metadata
    }
    /// Complete the volume operation on the replica.
    pub fn complete_vol(&mut self, timestamp: DateTime<Utc>, size: u64, total_allocated_size: u64) {
        self.commit_op();
        self.metadata.timestamp = Some(timestamp);
        self.metadata.size = size;
        self.metadata.allocated_size = total_allocated_size;
    }
    /// Mark the status as deleting.
    pub fn set_status_deleting(&mut self) {
        self.status = ReplicaSnapshotSpecStatus::Deleting;
        self.clear_op();
    }
}

/// Replica snapshot metadata, which is control-plane specific data that allows it
/// to manage a replica snapshot.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct ReplicaSnapshotMeta {
    #[serde(skip)]
    sequencer: OperationSequence,
    /// Record of the operation in progress.
    operation: Option<ReplicaSnapshotOperationState>,

    /// Creation timestamp of the snapshot (set after creation time).
    timestamp: Option<DateTime<Utc>>,
    /// User specified size of the source of snapshot.
    source_spec_size: u64,
    /// Actual size of the source of snapshot.
    size: u64,
    /// The amount of bytes allocated to the snapshot.
    allocated_size: u64,
    /// Information about the snapshot which is specific to how the snapshot was created,
    /// either as stand-alone snapshot or part of a volume snapshot transaction.
    meta: SnapshotMeta,
}
impl ReplicaSnapshotMeta {
    /// Return a new `Self` from the given parameters.
    pub fn new(
        parent: &SnapshotId,
        txn_id: &SnapshotTxId,
        size: u64,
        source_spec_size: u64,
        allocated_size: u64,
    ) -> Self {
        Self {
            sequencer: OperationSequence::new(),
            operation: Some(ReplicaSnapshotOperationState {
                operation: ReplicaSnapshotOperation::Create,
                result: None,
            }),
            timestamp: None,
            source_spec_size,
            size,
            allocated_size,
            meta: SnapshotMeta::Volume {
                parent: parent.clone(),
                txn_id: txn_id.to_string(),
            },
        }
    }
    /// Get the snapshot size.
    pub fn size(&self) -> u64 {
        self.size
    }
    /// Get the snapshot allocated size.
    pub fn allocated_size(&self) -> u64 {
        self.allocated_size
    }
    /// Get the snapshot source spec size.
    pub fn source_spec_size(&self) -> u64 {
        self.source_spec_size
    }
    /// Get the snapshot timestamp reference.
    pub fn timestamp(&self) -> Option<&DateTime<Utc>> {
        self.timestamp.as_ref()
    }
    /// Get the transaction identifier reference.
    pub fn txn_id(&self) -> Option<&SnapshotTxId> {
        self.meta.txn_id()
    }
}

/// Snapshot meta information.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
enum SnapshotMeta {
    #[default]
    Default,
    /// If we want to allow taking a replica snapshot directly?
    Replica { txn_id: SnapshotTxId },
    Volume {
        /// Volume Snapshot "parent" which owns this replica snapshot.
        /// Example: when taking a volume snapshot with N replicas, all replica snapshots will
        /// share the same transaction id.
        parent: SnapshotId,
        /// Volume snapshot transaction identifier part of the top-level snapshot.
        /// Example: if we retry snapshot creation, we attempt it with a different transaction
        /// identifier to ensure we don't mix snapshots from different points in time.
        txn_id: String,
    },
}
impl SnapshotMeta {
    fn txn_id(&self) -> Option<&SnapshotTxId> {
        match &self {
            Self::Default => None,
            Self::Replica { txn_id } => Some(txn_id),
            Self::Volume { txn_id, .. } => Some(txn_id),
        }
    }
}

/// Operation State for a ReplicaSnapshot resource.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ReplicaSnapshotOperationState {
    /// Record of the operation.
    operation: ReplicaSnapshotOperation,
    /// Result of the operation.
    result: Option<bool>,
}

/// Available ReplicaSnapshot Operations.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ReplicaSnapshotOperation {
    Create,
    Destroy,
}

impl AsOperationSequencer for ReplicaSnapshot {
    fn as_ref(&self) -> &OperationSequence {
        &self.metadata.sequencer
    }

    fn as_mut(&mut self) -> &mut OperationSequence {
        &mut self.metadata.sequencer
    }
}
impl SpecTransaction<ReplicaSnapshotOperation> for ReplicaSnapshot {
    fn has_pending_op(&self) -> bool {
        self.metadata.operation.is_some()
    }

    fn commit_op(&mut self) {
        if let Some(op) = self.metadata.operation.take() {
            match op.operation {
                ReplicaSnapshotOperation::Destroy => {
                    self.status = SpecStatus::Deleted;
                }
                ReplicaSnapshotOperation::Create => {
                    self.status = SpecStatus::Created(());
                }
            }
        }
    }

    fn clear_op(&mut self) {
        self.metadata.operation = None;
    }

    fn start_op(&mut self, operation: ReplicaSnapshotOperation) {
        self.metadata.operation = Some(ReplicaSnapshotOperationState {
            operation,
            result: None,
        })
    }

    fn set_op_result(&mut self, result: bool) {
        if let Some(op) = &mut self.metadata.operation {
            op.result = Some(result);
        }
    }

    fn pending_op(&self) -> Option<&ReplicaSnapshotOperation> {
        self.metadata.operation.as_ref().map(|o| &o.operation)
    }
}

/// Key used by the store to uniquely identify a VolumeSpec structure.
pub struct ReplicaSnapshotKey(SnapshotId);

impl From<&SnapshotId> for ReplicaSnapshotKey {
    fn from(id: &SnapshotId) -> Self {
        Self(id.clone())
    }
}

impl ObjectKey for ReplicaSnapshotKey {
    type Kind = StorableObjectType;

    fn version(&self) -> ApiVersion {
        ApiVersion::V0
    }

    fn key_type(&self) -> StorableObjectType {
        StorableObjectType::ReplicaSnapshot
    }

    fn key_uuid(&self) -> String {
        self.0.to_string()
    }
}

impl StorableObject for ReplicaSnapshot {
    type Key = ReplicaSnapshotKey;

    fn key(&self) -> Self::Key {
        ReplicaSnapshotKey(self.spec.uuid.clone())
    }
}
