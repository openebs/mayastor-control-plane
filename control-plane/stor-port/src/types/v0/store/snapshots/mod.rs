pub mod replica;
pub mod volume;

use crate::types::v0::transport::SnapshotId;
use serde::{Deserialize, Serialize};

/// User specification of a snapshot.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct SnapshotSpec<SourceId> {
    source_id: SourceId,
    uuid: SnapshotId,
}

impl<SourceId> SnapshotSpec<SourceId> {
    /// Get the snapshot source id.
    pub fn source_id(&self) -> &SourceId {
        &self.source_id
    }
    /// Get the snapshot id.
    pub fn uuid(&self) -> &SnapshotId {
        &self.uuid
    }
}
