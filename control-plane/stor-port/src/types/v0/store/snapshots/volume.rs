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

/// The volume snapshot definition which is stored in the persistent store.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct VolumeSnapshot {
    /// Status of the volume snapshot.
    status: VolumeSnapshotSpecStatus,
    /// User specification of the snapshot.
    spec: VolumeSnapshotUserSpec,
    /// Control-plane related information of the snapshot (book-keeping).
    metadata: VolumeSnapshotMeta,
}
impl VolumeSnapshot {
    pub fn new(spec: VolumeSnapshotUserSpec) -> Self {
        Self {
            status: VolumeSnapshotSpecStatus::Creating,
            spec,
            metadata: Default::default(),
        }
    }
    /// Get the snapshot status.
    pub fn status(&self) -> &VolumeSnapshotSpecStatus {
        &self.status
    }
    /// Set the snapshot status.
    pub fn set_status(&mut self, status: VolumeSnapshotSpecStatus) {
        self.status = status;
    }
    /// Get the snapshot spec.
    pub fn spec(&self) -> &VolumeSnapshotUserSpec {
        &self.spec
    }
    /// Get the snapshot metadata.
    pub fn metadata(&self) -> &VolumeSnapshotMeta {
        &self.metadata
    }
}

/// Control-plane snapshot metadata, used for book-keeping.
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
impl VolumeSnapshotMeta {
    /// Get the snapshot operation state.
    pub fn operation(&self) -> &Option<VolumeSnapshotOperationState> {
        &self.operation
    }
}

/// Operation State for a VolumeSnapshot resource.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct VolumeSnapshotOperationState {
    /// Record of the operation.
    pub operation: VolumeSnapshotOperation,
    /// Result of the operation.
    pub result: Option<bool>,
}

/// Available VolumeSnapshot Operations.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum VolumeSnapshotOperation {
    Create(VolumeSnapshotCreateInfo),
    Destroy,
}

/// Snapshot create information, used to set the initial data as part of the write log and also
/// the completion channel that is used to get the resulting data.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VolumeSnapshotCreateInfo {
    txn_id: String,
    replicas: Vec<ReplicaSnapshot>,
    #[serde(skip, default)]
    complete: Option<std::sync::Arc<std::sync::Mutex<VolumeSnapshotCreateResult>>>,
}
impl VolumeSnapshotCreateInfo {
    /// Get a new `Self` from the given parameters.
    pub fn new(
        txn_id: impl Into<String>,
        replicas: Vec<ReplicaSnapshot>,
        complete: std::sync::Arc<std::sync::Mutex<VolumeSnapshotCreateResult>>,
    ) -> Self {
        Self {
            txn_id: txn_id.into(),
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

/// The replica snapshot results from the creation operation.
#[derive(Debug, Clone, PartialEq)]
pub struct VolumeSnapshotCreateResult {
    /// The resulting replicas including their success status.
    pub replicas: Vec<ReplicaSnapshot>,
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

impl PartialEq<()> for VolumeSnapshot {
    fn eq(&self, _other: &()) -> bool {
        false
    }
}
