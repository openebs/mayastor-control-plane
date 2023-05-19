use super::*;
use serde::Serialize;

rpc_impl_string_uuid!(SnapshotId, "UUID of a snapshot");

/// The entity if of a snapshot.
pub type SnapshotEntId = String;
/// The transaction id of a snapshot.
pub type SnapshotTxId = String;
/// The name of a snapshot.
pub type SnapshotName = String;

/// Common set of snapshot parameters used for snapshot creation.
pub struct SnapshotParameters<TargetId> {
    /// Name of the target which we'll aim create snapshot at, which can either be
    /// a nexus or a replica at the moment.
    target: TargetId,
    /// Unique identification of the snapshot.
    uuid: SnapshotId,
    /// Entity id of the entity involved.
    entity_id: SnapshotEntId,
    /// A transaction id for this request.
    txn_id: SnapshotTxId,
    /// Name of the snapshot to be created.
    name: SnapshotName,
}

impl<TargetId> SnapshotParameters<TargetId> {
    /// Create a new set of snapshot parameters.
    pub fn new(
        target: impl Into<TargetId>,
        uuid: impl Into<SnapshotId>,
        entity_id: impl Into<SnapshotEntId>,
        txn_id: impl Into<SnapshotTxId>,
        name: impl Into<SnapshotName>,
    ) -> Self {
        Self {
            target: target.into(),
            uuid: uuid.into(),
            entity_id: entity_id.into(),
            txn_id: txn_id.into(),
            name: name.into(),
        }
    }

    /// Get a reference to the target uuid.
    pub fn target(&self) -> &TargetId {
        &self.target
    }
    /// Get a reference to the snapshot uuid.
    pub fn uuid(&self) -> &SnapshotId {
        &self.uuid
    }
    /// Get a reference to the entity id.
    pub fn entity(&self) -> &SnapshotEntId {
        &self.entity_id
    }
    /// Get a reference to the transaction id.
    pub fn txn_id(&self) -> &SnapshotTxId {
        &self.txn_id
    }
    /// Get a reference to the snapshot name.
    pub fn name(&self) -> &SnapshotName {
        &self.name
    }
}
