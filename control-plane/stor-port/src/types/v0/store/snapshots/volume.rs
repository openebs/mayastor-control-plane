use super::{replica::ReplicaSnapshot, SnapshotId, SnapshotSpec};
use crate::types::v0::{
    store::{AsOperationSequencer, OperationSequence, SpecStatus, SpecTransaction},
    transport::{GenericSnapshotParameters, SnapshotParameters, SnapshotTxId, VolumeId},
};
use chrono::{DateTime, Utc};
use pstor::{ApiVersion, ObjectKey, StorableObject, StorableObjectType};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

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
    pub fn prepare(&self) -> Option<SnapshotParameters<VolumeId>> {
        if !self.status.creating() {
            // we're done..
            return None;
        }

        let params = SnapshotParameters::new(
            self.spec().source_id(),
            GenericSnapshotParameters::new(
                self.spec().uuid(),
                self.spec().source_id().to_string(),
                self.metadata().prepare(),
            ),
        );
        Some(params)
    }
    /// Set the transactions to the params.
    pub fn set_transactions(&mut self, transactions: HashMap<SnapshotTxId, Vec<ReplicaSnapshot>>) {
        self.metadata.transactions = transactions
    }
    /// Set the stale transactions, that is, all which don't match the current txn.
    pub fn set_stale_transactions(
        &mut self,
        transactions: HashMap<SnapshotTxId, Vec<ReplicaSnapshot>>,
    ) {
        self.metadata
            .transactions
            .retain(|key, _| key == &self.metadata.txn_id);
        self.metadata.transactions.extend(transactions)
    }
}
impl From<&VolumeSnapshotUserSpec> for VolumeSnapshot {
    fn from(value: &VolumeSnapshotUserSpec) -> Self {
        Self::new(VolumeSnapshotUserSpec::new(
            value.source_id(),
            value.uuid().clone(),
        ))
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
    timestamp: Option<DateTime<Utc>>,
    /// User specified size of the source of snapshot.
    spec_size: u64,
    /// Actual size of the source of snapshot.
    size: u64,
    /// The amount of bytes allocated by the snapshot and its successors.
    total_allocated_size: u64,
    /// Transaction Id that defines this snapshot when it is created.
    txn_id: SnapshotTxId,
    /// Replicas which "reference" to this snapshot as its parent, indexed by the transaction
    /// id when they were attempted.
    /// The "actual" snapshots can be accessed by the key `txn_id`.
    /// Failed transactions are any other key.
    transactions: HashMap<SnapshotTxId, Vec<ReplicaSnapshot>>,
}
impl VolumeSnapshotMeta {
    /// Get the snapshot operation state.
    pub fn operation(&self) -> &Option<VolumeSnapshotOperationState> {
        &self.operation
    }
    /// Get the snapshot timestamp.
    pub fn timestamp(&self) -> &Option<DateTime<Utc>> {
        &self.timestamp
    }
    /// Get the snapshot transaction id.
    pub fn txn_id(&self) -> &SnapshotTxId {
        &self.txn_id
    }
    /// Get the snapshot size.
    pub fn size(&self) -> u64 {
        self.size
    }
    /// Get the snapshot spec size.
    pub fn spec_size(&self) -> u64 {
        self.spec_size
    }
    /// The amount of bytes allocated to the snapshot and its successors.
    pub fn total_allocated_size(&self) -> u64 {
        self.total_allocated_size
    }
    pub fn prepare(&self) -> SnapshotTxId {
        // If this is a create retry, then we must allocate a new transaction id, and prepare
        // replicas
        let txn_id: u64 = self.txn_id.parse().unwrap_or_default();
        (txn_id + 1).to_string()
    }

    /// Get the transactions.
    pub fn transactions(&self) -> &HashMap<SnapshotTxId, Vec<ReplicaSnapshot>> {
        &self.transactions
    }
    /// Get the stale transactions.
    pub fn stale_transactions(&self) -> HashMap<SnapshotTxId, Vec<ReplicaSnapshot>> {
        self.stale_transactions_ref()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
    /// Get the stale transactions as a reference.
    pub fn stale_transactions_ref(&self) -> impl Iterator<Item = (&String, &Vec<ReplicaSnapshot>)> {
        self.transactions
            .iter()
            .filter(|(txn, _)| txn != &&self.txn_id)
    }
    /// Get the current replica snapshots.
    pub fn replica_snapshots(&self) -> Option<&Vec<ReplicaSnapshot>> {
        self.transactions.get(&self.txn_id)
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
#[allow(clippy::large_enum_variant)]
pub enum VolumeSnapshotOperation {
    Create(VolumeSnapshotCreateInfo),
    Destroy,
    CleanupStaleTransactions,
}

/// Completion info for volume snapshot create operation.
pub type VolumeSnapshotCompleter = Arc<std::sync::Mutex<Option<VolumeSnapshotCreateResult>>>;

/// Snapshot create information, used to set the initial data as part of the write log and also
/// the completion channel that is used to get the resulting data.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VolumeSnapshotCreateInfo {
    txn_id: SnapshotTxId,
    // todo: support multi-replica snapshot
    replica: ReplicaSnapshot,
    #[serde(skip, default)]
    complete: VolumeSnapshotCompleter,
}
impl VolumeSnapshotCreateInfo {
    /// Get a new `Self` from the given parameters.
    pub fn new(
        txn_id: impl Into<SnapshotTxId>,
        replica: ReplicaSnapshot,
        complete: &VolumeSnapshotCompleter,
    ) -> Self {
        Self {
            txn_id: txn_id.into(),
            replica,
            complete: complete.clone(),
        }
    }
}

impl PartialEq for VolumeSnapshotCreateInfo {
    fn eq(&self, other: &Self) -> bool {
        self.txn_id
            .eq(&other.txn_id)
            .then(|| self.replica.eq(&other.replica))
            .unwrap_or_default()
    }
}

impl PartialEq<VolumeSnapshotCreateInfo> for VolumeSnapshot {
    fn eq(&self, other: &VolumeSnapshotCreateInfo) -> bool {
        // This is a bit nuanced, actually we simply expect that the txn_id is not the
        // same as we don't allow reusing the txn_id. Instead, if we have the same txn_id then
        // we should check if all is created and ready!
        self.metadata.txn_id.ne(&other.txn_id)
    }
}

/// The replica snapshot created from the creation operation.
#[derive(Debug, Clone, PartialEq)]
pub struct VolumeSnapshotCreateResult {
    /// The resulting replicas including their success status.
    /// todo: add support for multiple replica snapshots.
    replicas: Vec<ReplicaSnapshot>,
    /// The actual timestamp returned by the dataplane.
    timestamp: DateTime<Utc>,
}
impl VolumeSnapshotCreateResult {
    /// Create a new `Self` based on the given parameters.
    pub fn new_ok(replica: ReplicaSnapshot, timestamp: DateTime<Utc>) -> Self {
        Self {
            replicas: vec![replica],
            timestamp,
        }
    }
    /// Create a new `Self` based on the given parameters.
    pub fn new_err(replicas: Vec<ReplicaSnapshot>) -> Self {
        Self {
            replicas,
            timestamp: Default::default(),
        }
    }
    /// The the size of the replica snapshots.
    pub fn size(&self) -> u64 {
        let first = self.replicas.first();
        first.map(|r| r.meta().size()).unwrap_or_default()
    }

    /// The amount of bytes allocated to the snapshot.
    pub fn allocated_size(&self) -> u64 {
        let first = self.replicas.first();
        first.map(|r| r.meta().allocated_size()).unwrap_or_default()
    }
    /// The snapshot source spec size.
    pub fn source_spec_size(&self) -> u64 {
        let first = self.replicas.first();
        first
            .map(|r| r.meta().source_spec_size())
            .unwrap_or_default()
    }
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
    fn has_pending_op(&self) -> bool {
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
                if let Some(result) = info.complete.lock().unwrap().as_ref() {
                    self.metadata.size = result.size();
                    self.metadata.total_allocated_size = result.allocated_size();
                    self.metadata.spec_size = result.source_spec_size();
                    self.metadata.timestamp = Some(result.timestamp);
                    // replace-in-place the logged replica specs.
                    self.metadata
                        .transactions
                        .insert(info.txn_id, result.replicas.clone());
                    self.status = SpecStatus::Created(());
                } else {
                    // means we've restarted with the op in progress... and the snapshot was not
                    // successful!
                    tracing::error!(?self, "Snapshot Create completion without the result");
                }
            }
            VolumeSnapshotOperation::CleanupStaleTransactions => {}
        }
    }

    fn clear_op(&mut self) {
        let Some(op) = self.metadata.operation.take() else {
            return;
        };
        match op.operation {
            VolumeSnapshotOperation::Create(mut info) => {
                if let Some(result) = info.complete.lock().unwrap().as_ref() {
                    if result.replicas.is_empty() {
                        self.metadata.transactions.remove(&info.txn_id);
                    } else {
                        self.metadata
                            .transactions
                            .insert(info.txn_id, result.replicas.clone());
                    }
                } else {
                    info.replica.set_status_deleting();
                    self.metadata
                        .transactions
                        .insert(info.txn_id, vec![info.replica]);
                }
            }
            VolumeSnapshotOperation::Destroy => {}
            VolumeSnapshotOperation::CleanupStaleTransactions => {}
        }
    }

    fn start_op(&mut self, operation: VolumeSnapshotOperation) {
        if let VolumeSnapshotOperation::Create(info) = &operation {
            self.metadata.txn_id = info.txn_id.clone();
            self.metadata
                .transactions
                .insert(info.txn_id.clone(), vec![info.replica.clone()]);
        }
        self.metadata.operation = Some(VolumeSnapshotOperationState {
            operation,
            result: None,
        });
    }

    fn set_op_result(&mut self, result: bool) {
        if let Some(op) = &mut self.metadata.operation {
            op.result = Some(result);
        }
    }

    fn pending_op(&self) -> Option<&VolumeSnapshotOperation> {
        self.metadata.operation.as_ref().map(|o| &o.operation)
    }

    fn allow_op_creating(&mut self, operation: &VolumeSnapshotOperation) -> bool {
        matches!(operation, VolumeSnapshotOperation::CleanupStaleTransactions)
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
    snapshots: HashSet<SnapshotId>,
}
impl VolumeSnapshotList {
    /// Insert snapshot into the list.
    pub fn insert(&mut self, snapshot: SnapshotId) {
        self.snapshots.insert(snapshot);
    }
    /// Remove snapshot from the list.
    pub fn remove(&mut self, snapshot: &SnapshotId) {
        self.snapshots.remove(snapshot);
    }
    /// Check if there's any snapshot.
    pub fn is_empty(&self) -> bool {
        self.snapshots.is_empty()
    }
}

impl PartialEq<()> for VolumeSnapshot {
    fn eq(&self, _other: &()) -> bool {
        false
    }
}
