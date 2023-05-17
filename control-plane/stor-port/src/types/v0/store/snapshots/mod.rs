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
