use super::{SnapshotId, SnapshotSpec};
use crate::types::v0::{
    store::{AsOperationSequencer, OperationSequence, SpecStatus, SpecTransaction},
    transport::ReplicaId,
};
use chrono::{DateTime, Utc};
use pstor::{ApiVersion, ObjectKey, StorableObject, StorableObjectType};
use serde::{Deserialize, Serialize};

/// User specification of a replica snapshot.
pub type ReplicaSnapshotSpec = SnapshotSpec<ReplicaId>;
/// State of the ReplicaSnapshotSpec Spec.
pub type ReplicaSnapshotSpecStatus = SpecStatus<()>;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct ReplicaSnapshot {
    /// Status of the replica snapshot.
    status: ReplicaSnapshotSpecStatus,
    /// User specification of the snapshot.
    spec: ReplicaSnapshotSpec,
    /// Control-plane related information of the snapshot (book-keeping).
    metadata: ReplicaSnapshotMeta,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct ReplicaSnapshotMeta {
    #[serde(skip)]
    sequencer: OperationSequence,
    /// Record of the operation in progress.
    operation: Option<ReplicaSnapshotOperationState>,

    /// Creation timestamp of the snapshot (set after creation time).
    creation_timestamp: Option<DateTime<Utc>>,
    /// Size of the snapshot (typically follows source size).
    size: u64,
    /// Information about the snapshot which is specific to how the snapshot was created,
    /// either as stand-alone snapshot or part of a volume snapshot transaction.
    meta: SnapshotMeta,
}

/// Snapshot meta information.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
enum SnapshotMeta {
    #[default]
    Default,
    /// If we want to allow taking a replica snapshot directly?
    Replica { txn_id: String },
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
    fn pending_op(&self) -> bool {
        self.metadata.operation.is_some()
    }

    fn commit_op(&mut self) {
        if let Some(op) = self.metadata.operation.clone() {
            match op.operation {
                ReplicaSnapshotOperation::Destroy => {
                    self.status = SpecStatus::Deleted;
                }
                ReplicaSnapshotOperation::Create => {
                    self.status = SpecStatus::Created(());
                }
            }
        }
        self.clear_op();
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
