use super::{ResourceMutex, ResourceUid};
use stor_port::types::v0::{
    store::snapshots::{group::VolumeSnapshotGroup, volume::VolumeSnapshot, ReplicaSnapshotState},
    transport::{SnapshotGroupId, SnapshotId},
};

impl ResourceMutex<VolumeSnapshot> {
    /// Get the resource uuid.
    #[allow(dead_code)]
    pub fn uuid(&self) -> &SnapshotId {
        self.immutable_ref().uid()
    }
}
impl ResourceUid for VolumeSnapshot {
    type Uid = SnapshotId;
    fn uid(&self) -> &Self::Uid {
        self.spec().uuid()
    }
}
impl ResourceUid for ReplicaSnapshotState {
    type Uid = SnapshotId;
    fn uid(&self) -> &Self::Uid {
        self.snapshot.snap_uuid()
    }
}
impl ResourceMutex<VolumeSnapshotGroup> {
    /// Get the resource uuid.
    #[allow(dead_code)]
    pub fn uuid(&self) -> &SnapshotGroupId {
        self.immutable_ref().uid()
    }
}
impl ResourceUid for VolumeSnapshotGroup {
    type Uid = SnapshotGroupId;
    fn uid(&self) -> &Self::Uid {
        self.uuid()
    }
}
