use crate::types::v0::{
    store::{AsOperationSequencer, OperationSequence, SpecStatus, SpecTransaction},
    transport::{SnapshotGroupId, SnapshotId, VolumeId},
};
use chrono::{DateTime, Utc};
use pstor::{ApiVersion, ObjectKey, StorableObject, StorableObjectType};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

/// State of the VolumeSnapshotGroup Spec.
pub type VolumeSnapshotGroupSpecStatus = SpecStatus<()>;

/// How the members of a snapshot group are quiesced while the group is taken.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VolumeSnapshotGroupQuiesce {
    /// Freeze the filesystem of every published member.
    #[default]
    Freeze,
    /// No quiescing, members are snapshotted as-is.
    None,
}

/// User specification of a volume snapshot group.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct VolumeSnapshotGroupUserSpec {
    /// Unique identification of the group.
    uuid: SnapshotGroupId,
    /// The member volumes and their pre-derived snapshot ids, so a retried
    /// create request maps onto the same member snapshots.
    members: HashMap<VolumeId, SnapshotId>,
    /// How the members are quiesced during the group operation.
    quiesce: VolumeSnapshotGroupQuiesce,
}
impl VolumeSnapshotGroupUserSpec {
    /// Create a new `Self` from the given parameters.
    pub fn new(
        uuid: SnapshotGroupId,
        members: HashMap<VolumeId, SnapshotId>,
        quiesce: VolumeSnapshotGroupQuiesce,
    ) -> Self {
        Self {
            uuid,
            members,
            quiesce,
        }
    }
    /// Get the group id.
    pub fn uuid(&self) -> &SnapshotGroupId {
        &self.uuid
    }
    /// Get the group members as a map of volume to snapshot id.
    pub fn members(&self) -> &HashMap<VolumeId, SnapshotId> {
        &self.members
    }
    /// Get the group member volumes, sorted for deterministic lock ordering.
    pub fn sorted_volumes(&self) -> Vec<VolumeId> {
        let mut volumes = self.members.keys().cloned().collect::<Vec<_>>();
        volumes.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        volumes
    }
    /// Get the quiesce mode of the group.
    pub fn quiesce(&self) -> VolumeSnapshotGroupQuiesce {
        self.quiesce
    }
}

/// The volume snapshot group definition which is stored in the persistent store.
/// The member snapshots themselves are ordinary `VolumeSnapshot` resources; this
/// records their membership so get/delete behave correctly across restarts.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct VolumeSnapshotGroup {
    /// Status of the volume snapshot group.
    status: VolumeSnapshotGroupSpecStatus,
    /// User specification of the group.
    spec: VolumeSnapshotGroupUserSpec,
    /// Control-plane related information of the group (book-keeping).
    metadata: VolumeSnapshotGroupMeta,
}
impl VolumeSnapshotGroup {
    /// Create a new `Self` from the user specification.
    pub fn new(spec: VolumeSnapshotGroupUserSpec) -> Self {
        Self {
            status: VolumeSnapshotGroupSpecStatus::Creating,
            spec,
            metadata: Default::default(),
        }
    }
    /// Get the group status.
    pub fn status(&self) -> &VolumeSnapshotGroupSpecStatus {
        &self.status
    }
    /// Set the group status.
    pub fn set_status(&mut self, status: VolumeSnapshotGroupSpecStatus) {
        self.status = status;
    }
    /// Get the group spec.
    pub fn spec(&self) -> &VolumeSnapshotGroupUserSpec {
        &self.spec
    }
    /// Get the group metadata.
    pub fn metadata(&self) -> &VolumeSnapshotGroupMeta {
        &self.metadata
    }
    /// Get the group id.
    pub fn uuid(&self) -> &SnapshotGroupId {
        self.spec.uuid()
    }
}
impl From<&VolumeSnapshotGroupUserSpec> for VolumeSnapshotGroup {
    fn from(value: &VolumeSnapshotGroupUserSpec) -> Self {
        Self::new(value.clone())
    }
}

/// Control-plane snapshot group metadata, used for book-keeping.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct VolumeSnapshotGroupMeta {
    #[serde(skip)]
    sequencer: OperationSequence,
    /// Record of the operation in progress.
    operation: Option<VolumeSnapshotGroupOperationState>,
    /// Creation timestamp of the group (set after creation time).
    timestamp: Option<DateTime<Utc>>,
}
impl VolumeSnapshotGroupMeta {
    /// Get the group operation state.
    pub fn operation(&self) -> &Option<VolumeSnapshotGroupOperationState> {
        &self.operation
    }
    /// Get the group creation timestamp.
    pub fn timestamp(&self) -> &Option<DateTime<Utc>> {
        &self.timestamp
    }
}

/// Operation State for a VolumeSnapshotGroup resource.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct VolumeSnapshotGroupOperationState {
    /// Record of the operation.
    pub operation: VolumeSnapshotGroupOperation,
    /// Result of the operation.
    pub result: Option<bool>,
}

/// Available VolumeSnapshotGroup Operations.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum VolumeSnapshotGroupOperation {
    Create(VolumeSnapshotGroupCreateInfo),
    Destroy,
}

/// Completion info for the volume snapshot group create operation.
pub type VolumeSnapshotGroupCompleter = Arc<Mutex<Option<VolumeSnapshotGroupCreateResult>>>;

/// The result of a successful group create operation.
#[derive(Debug, Clone, PartialEq)]
pub struct VolumeSnapshotGroupCreateResult {
    /// The timestamp reported when all member snapshots completed.
    timestamp: DateTime<Utc>,
}
impl VolumeSnapshotGroupCreateResult {
    /// Create a new `Self` with the given completion timestamp.
    pub fn new(timestamp: DateTime<Utc>) -> Self {
        Self { timestamp }
    }
}

/// Group create information logged as part of the operation write log, along with
/// the completion channel that is used to get the resulting data.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct VolumeSnapshotGroupCreateInfo {
    #[serde(skip, default)]
    complete: VolumeSnapshotGroupCompleter,
}
impl VolumeSnapshotGroupCreateInfo {
    /// Create a new `Self` with the given completion channel.
    pub fn new(complete: &VolumeSnapshotGroupCompleter) -> Self {
        Self {
            complete: complete.clone(),
        }
    }
}
impl PartialEq for VolumeSnapshotGroupCreateInfo {
    fn eq(&self, _other: &Self) -> bool {
        // The create info carries no request parameters, only the completion channel.
        true
    }
}

impl AsOperationSequencer for VolumeSnapshotGroup {
    fn as_ref(&self) -> &OperationSequence {
        &self.metadata.sequencer
    }

    fn as_mut(&mut self) -> &mut OperationSequence {
        &mut self.metadata.sequencer
    }
}

impl SpecTransaction<VolumeSnapshotGroupOperation> for VolumeSnapshotGroup {
    fn has_pending_op(&self) -> bool {
        self.metadata.operation.is_some()
    }

    fn commit_op(&mut self) {
        let Some(op) = self.metadata.operation.take() else {
            return;
        };
        match op.operation {
            VolumeSnapshotGroupOperation::Create(info) => {
                if let Some(result) = info.complete.lock().unwrap().as_ref() {
                    self.metadata.timestamp = Some(result.timestamp);
                    self.status = SpecStatus::Created(());
                } else {
                    // means we've restarted with the op in progress... and the group was not
                    // successful!
                    tracing::error!(?self, "Snapshot group create completion without the result");
                }
            }
            VolumeSnapshotGroupOperation::Destroy => {
                self.status = SpecStatus::Deleted;
            }
        }
    }

    fn clear_op(&mut self) {
        self.metadata.operation = None;
    }

    fn start_op(&mut self, operation: VolumeSnapshotGroupOperation) {
        self.metadata.operation = Some(VolumeSnapshotGroupOperationState {
            operation,
            result: None,
        });
    }

    fn set_op_result(&mut self, result: bool) {
        if let Some(op) = &mut self.metadata.operation {
            op.result = Some(result);
        }
    }

    fn pending_op(&self) -> Option<&VolumeSnapshotGroupOperation> {
        self.metadata.operation.as_ref().map(|o| &o.operation)
    }
}

/// Key used by the store to uniquely identify a VolumeSnapshotGroup.
pub struct VolumeSnapshotGroupKey(SnapshotGroupId);

impl From<&SnapshotGroupId> for VolumeSnapshotGroupKey {
    fn from(id: &SnapshotGroupId) -> Self {
        Self(id.clone())
    }
}

impl ObjectKey for VolumeSnapshotGroupKey {
    type Kind = StorableObjectType;

    fn version(&self) -> ApiVersion {
        ApiVersion::V0
    }

    fn key_type(&self) -> StorableObjectType {
        StorableObjectType::VolumeSnapshotGroup
    }

    fn key_uuid(&self) -> String {
        self.0.to_string()
    }
}

impl StorableObject for VolumeSnapshotGroup {
    type Key = VolumeSnapshotGroupKey;

    fn key(&self) -> Self::Key {
        VolumeSnapshotGroupKey(self.spec.uuid().clone())
    }
}

impl PartialEq<VolumeSnapshotGroupCreateInfo> for VolumeSnapshotGroup {
    fn eq(&self, _other: &VolumeSnapshotGroupCreateInfo) -> bool {
        // A creating group may always retry its create operation; membership equality
        // is enforced by the caller against the persisted user spec.
        true
    }
}

impl PartialEq<()> for VolumeSnapshotGroup {
    fn eq(&self, _other: &()) -> bool {
        false
    }
}
