use stor_port::types::v0::transport::SnapshotId;

/// General Specification of a snapshot information.
#[derive(Debug, Clone)]
pub struct SnapshotInfo<SourceId: Clone> {
    /// The source id from which this snapshot is taken.
    pub source_id: SourceId,
    /// The snapshot id.
    pub snap_id: SnapshotId,
}

impl<SourceId: Clone> SnapshotInfo<SourceId> {
    /// Create a new `Self` from the given parameters.
    pub fn new(source_id: &SourceId, snap_id: SnapshotId) -> Self {
        Self {
            source_id: source_id.clone(),
            snap_id,
        }
    }
    /// Get the snapshot source id.
    pub fn source_id(&self) -> &SourceId {
        &self.source_id
    }
    /// Get the snapshot id.
    pub fn snap_id(&self) -> &SnapshotId {
        &self.snap_id
    }
}
