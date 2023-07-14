use super::*;
use crate::types::v0::store::replica::PoolRef;
use serde::Serialize;

rpc_impl_string_uuid!(SnapshotId, "UUID of a snapshot");
/// UUID of a snapshot clone.
pub type VolSnapshotCloneId = VolumeId;
/// UUID of a replica clone.
pub type SnapshotCloneId = ReplicaId;

/// The entity if of a snapshot.
pub type SnapshotEntId = String;
/// The transaction id of a snapshot.
pub type SnapshotTxId = String;
/// The name of a snapshot.
pub type SnapshotName = String;
/// The name of a snapshot clone.
pub type SnapshotCloneName = String;

/// Common set of snapshot parameters used for snapshot creation against `TargetId`.
#[derive(Debug)]
pub struct SnapshotParameters<TargetId: Debug> {
    /// Name of the target which we'll aim create snapshot at, which can either be
    /// a nexus or a replica at the moment.
    target: TargetId,
    params: GenericSnapshotParameters,
}

/// Common set of snapshot parameters used for snapshot creation.
#[derive(Debug, Clone)]
pub struct GenericSnapshotParameters {
    /// Unique identification of the snapshot.
    uuid: SnapshotId,
    /// Entity id of the entity involved.
    entity_id: SnapshotEntId,
    /// A transaction id for this request.
    txn_id: SnapshotTxId,
    /// Name of the snapshot to be created.
    name: SnapshotName,
}

impl<TargetId: Debug> SnapshotParameters<TargetId> {
    /// Create a new set of snapshot parameters.
    pub fn new(target: impl Into<TargetId>, params: GenericSnapshotParameters) -> Self {
        Self {
            target: target.into(),
            params,
        }
    }

    /// Get a reference to the target uuid.
    pub fn target(&self) -> &TargetId {
        &self.target
    }
    /// Get a reference to the generic snapshot parameters.
    pub fn params(&self) -> &GenericSnapshotParameters {
        &self.params
    }
    /// Get a reference to the snapshot uuid.
    pub fn uuid(&self) -> &SnapshotId {
        &self.params.uuid
    }
    /// Get a reference to the entity id.
    pub fn entity(&self) -> &SnapshotEntId {
        &self.params.entity_id
    }
    /// Get a reference to the transaction id.
    pub fn txn_id(&self) -> &SnapshotTxId {
        &self.params.txn_id
    }
    /// Get a reference to the snapshot name.
    pub fn name(&self) -> &SnapshotName {
        &self.params.name
    }
}

impl GenericSnapshotParameters {
    /// Create a new set of snapshot parameters.
    pub fn new(
        uuid: impl Into<SnapshotId>,
        entity_id: impl Into<SnapshotEntId>,
        txn_id: impl Into<SnapshotTxId>,
    ) -> Self {
        let uuid = uuid.into();
        let txn_id = txn_id.into();
        let name = format!("{uuid}/{txn_id}");
        Self {
            uuid,
            entity_id: entity_id.into(),
            txn_id,
            name,
        }
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

    /// Modify the uuid.
    pub fn with_uuid(mut self, uuid: &SnapshotId) -> Self {
        self.uuid = uuid.clone();
        self
    }
}

/// The request type to list replica's snapshots.
#[derive(Default)]
pub enum ListReplicaSnapshots {
    /// All snapshots.
    #[default]
    All,
    /// All snapshots from the given source.
    ReplicaSnapshots(ReplicaId),
    /// The specific snapshot.
    Snapshot(SnapshotId),
}

/// The request type to create a snapshot's clone.
pub type IoEngCreateSnapshotClone = SnapshotCloneParameters;

/// Common set of parameters used for snapshot clone creation.
#[derive(Debug, Clone)]
pub struct SnapshotCloneParameters {
    /// Unique identification of the source snapshot.
    snapshot_uuid: SnapshotId,
    /// Name of the snapshot clone.
    name: SnapshotCloneName,
    /// Unique identification of the snapshot clone.
    uuid: SnapshotCloneId,
}
impl SnapshotCloneParameters {
    pub fn new(snapshot_uuid: SnapshotId, name: SnapshotCloneName, uuid: SnapshotCloneId) -> Self {
        Self {
            snapshot_uuid,
            name,
            uuid,
        }
    }
    /// Get a reference to the snapshot uuid.
    pub fn snapshot_uuid(&self) -> &SnapshotId {
        &self.snapshot_uuid
    }
    /// Get a reference to the clone name.
    pub fn name(&self) -> &SnapshotCloneName {
        &self.name
    }
    /// Get a reference to the clone uuid.
    pub fn uuid(&self) -> &SnapshotCloneId {
        &self.uuid
    }
}

/// List all clones from the given snapshot.
#[derive(Debug, Clone)]
pub enum ListSnapshotClones {
    All,
    Snapshot(SnapshotId),
}
impl ListSnapshotClones {
    /// Get a reference to the snapshot uuid.
    pub fn uuid(&self) -> Option<&SnapshotId> {
        match self {
            ListSnapshotClones::All => None,
            ListSnapshotClones::Snapshot(uuid) => Some(uuid),
        }
    }
}

/// Common set of parameters used for snapshot clone creation.
#[derive(Debug, Clone)]
pub struct SnapshotCloneSpecParams {
    /// Parameters for creation of the snapshot clone from.
    repl_params: SnapshotCloneParameters,
    /// Size of the volume clone.
    size: u64,
    /// Reference of a pool that the clone should live on.
    pool: PoolRef,
    /// Node where the snapshot pool lives on.
    node: NodeId,
    /// Uuid of the volume clone.
    uuid: VolSnapshotCloneId,
}
impl SnapshotCloneSpecParams {
    pub fn new(
        repl_params: SnapshotCloneParameters,
        size: u64,
        pool: PoolRef,
        node: NodeId,
        uuid: VolSnapshotCloneId,
    ) -> Self {
        Self {
            repl_params,
            size,
            pool,
            node,
            uuid,
        }
    }
    /// Get a reference to the snapshot uuid.
    pub fn params(&self) -> &SnapshotCloneParameters {
        &self.repl_params
    }
    /// Get a reference to the clone name.
    pub fn size(&self) -> u64 {
        self.size
    }
    /// Get a reference to the clone name.
    pub fn pool(&self) -> &PoolRef {
        &self.pool
    }
    /// Get a reference to the node id.
    pub fn node(&self) -> &NodeId {
        &self.node
    }
    /// Get a reference to the clone name.
    pub fn uuid(&self) -> &VolSnapshotCloneId {
        &self.uuid
    }
}
