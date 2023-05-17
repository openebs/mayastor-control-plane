use super::{replica::ReplicaSnapshot, SnapshotId, SnapshotSpec};
use crate::types::v0::{
    store::{AsOperationSequencer, OperationSequence, SpecStatus, SpecTransaction},
    transport::VolumeId,
};
use chrono::{DateTime, Utc};
use pstor::{ApiVersion, ObjectKey, StorableObject, StorableObjectType};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// User specification of a volume snapshot.
/// todo: is e-derivations a better way of doing this?
pub type VolumeSnapshotUserSpec = SnapshotSpec<VolumeId>;

/// State of the VolumeSnapshotSpec Spec.
pub type VolumeSnapshotSpecStatus = SpecStatus<()>;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct VolumeSnapshot {
    /// Status of the volume snapshot.
    status: VolumeSnapshotSpecStatus,
    /// User specification of the snapshot.
    spec: VolumeSnapshotUserSpec,
    /// Control-plane related information of the snapshot (book-keeping).
    metadata: VolumeSnapshotMeta,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct VolumeSnapshotMeta {
    #[serde(skip)]
    sequencer: OperationSequence,
    /// Record of the operation in progress.
    operation: Option<VolumeSnapshotOperationState>,

    /// Creation timestamp of the snapshot (set after creation time).
    creation_timestamp: Option<DateTime<Utc>>,
    /// Size of the snapshot (typically follows source size).
    size: u64,
    /// Transaction Id that defines this snapshot when it is created.
    txn_id: String,
    /// Replicas which "reference" to this snapshot as its parent, indexed by the transaction
    /// id when they were attempted.
    /// The "actual" snapshots can be accessed by the key `txn_id`.
    /// Failed transactions are any other key.
    transactions: HashMap<String, Vec<ReplicaSnapshot>>,
}

/// Operation State for a VolumeSnapshot resource.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct VolumeSnapshotOperationState {
    /// Record of the operation.
    operation: VolumeSnapshotOperation,
    /// Result of the operation.
    result: Option<bool>,
}

/// Available VolumeSnapshot Operations.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum VolumeSnapshotOperation {
    Create(VolumeSnapshotCreateInfo),
    Destroy,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VolumeSnapshotCreateInfo {
    txn_id: String,
    replicas: Vec<ReplicaSnapshot>,
    #[serde(skip, default)]
    complete: Option<std::sync::Arc<std::sync::Mutex<VolumeSnapshotCreateResult>>>,
}
impl VolumeSnapshotCreateInfo {
    pub fn new(
        txn_id: String,
        replicas: Vec<ReplicaSnapshot>,
        complete: std::sync::Arc<std::sync::Mutex<VolumeSnapshotCreateResult>>,
    ) -> Self {
        Self {
            txn_id,
            replicas,
            complete: Some(complete),
        }
    }
}
impl PartialEq for VolumeSnapshotCreateInfo {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VolumeSnapshotCreateResult {
    replicas: Vec<ReplicaSnapshot>,
}

impl AsOperationSequencer for VolumeSnapshot {
    fn as_ref(&self) -> &OperationSequence {
        &self.metadata.sequencer
    }

    fn as_mut(&mut self) -> &mut OperationSequence {
        &mut self.metadata.sequencer
    }
}
impl SpecTransaction<VolumeSnapshotOperation> for VolumeSnapshot {
    fn pending_op(&self) -> bool {
        self.metadata.operation.is_some()
    }

    fn commit_op(&mut self) {
        let Some(op) = self.metadata.operation.take() else {
            return;
        };
        match op.operation {
            VolumeSnapshotOperation::Destroy => {
                self.status = SpecStatus::Deleted;
            }
            VolumeSnapshotOperation::Create(info) => {
                if let Some(result) = info.complete {
                    let result = result.lock().unwrap();
                    // replace-in-place the logged replica specs.
                    self.metadata
                        .transactions
                        .insert(info.txn_id, result.replicas.clone());
                    self.status = SpecStatus::Created(());
                }
                // else means we've restarted with the op in progress... and the snapshot was not
                // successful!
            }
        }
        self.clear_op();
    }

    fn clear_op(&mut self) {
        self.metadata.operation = None;
    }

    fn start_op(&mut self, operation: VolumeSnapshotOperation) {
        self.metadata.operation = Some(VolumeSnapshotOperationState {
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

/// Key used by the store to uniquely identify a VolumeSnapshot.
pub struct VolumeSnapshotKey(SnapshotId);

impl From<&SnapshotId> for VolumeSnapshotKey {
    fn from(id: &SnapshotId) -> Self {
        Self(id.clone())
    }
}

impl ObjectKey for VolumeSnapshotKey {
    type Kind = StorableObjectType;

    fn version(&self) -> ApiVersion {
        ApiVersion::V0
    }

    fn key_type(&self) -> StorableObjectType {
        StorableObjectType::VolumeSnapshot
    }

    fn key_uuid(&self) -> String {
        self.0.to_string()
    }
}

impl StorableObject for VolumeSnapshot {
    type Key = VolumeSnapshotKey;

    fn key(&self) -> Self::Key {
        VolumeSnapshotKey(self.spec.uuid.clone())
    }
}

/// List of all volume snapshots and related information.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct VolumeSnapshotList {
    snapshots: HashMap<SnapshotId, VolumeSnapshot>,
}
