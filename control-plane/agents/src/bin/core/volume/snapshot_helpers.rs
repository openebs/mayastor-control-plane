use crate::controller::{
    registry::Registry,
    resources::{
        operations_helper::{GuardedOperationsHelper, SpecOperationsHelper},
        OperationGuardArc, ResourceUid,
    },
};
use agents::errors::SvcError;
use stor_port::{
    transport_api::ResourceKind,
    types::v0::store::{
        snapshots::volume::{VolumeSnapshot, VolumeSnapshotCreateInfo, VolumeSnapshotOperation},
        SpecStatus, SpecTransaction,
    },
};

#[async_trait::async_trait]
impl GuardedOperationsHelper for OperationGuardArc<VolumeSnapshot> {
    type Create = CreateVolumeSnapshot;
    type Owners = ();
    type Status = ();
    type State = ();
    type UpdateOp = VolumeSnapshotOperation;
    type Inner = VolumeSnapshot;

    fn remove_spec(&self, registry: &Registry) {
        let uuid = self.uuid().clone();
        registry.specs().remove_volume_snapshot(&uuid);
    }
}

#[derive(Debug)]
pub(crate) struct CreateVolumeSnapshot {
    /// The user spec request
    #[allow(dead_code)]
    pub(crate) request: (),
    /// The info used by the control-plane agent.
    pub(crate) info: VolumeSnapshotCreateInfo,
}
impl PartialEq for CreateVolumeSnapshot {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}
impl PartialEq<CreateVolumeSnapshot> for VolumeSnapshot {
    fn eq(&self, _other: &CreateVolumeSnapshot) -> bool {
        false
    }
}

#[async_trait::async_trait]
impl SpecOperationsHelper for VolumeSnapshot {
    type Create = CreateVolumeSnapshot;
    type Owners = ();
    type Status = ();
    type State = ();
    type UpdateOp = VolumeSnapshotOperation;

    async fn start_update_op(
        &mut self,
        _registry: &Registry,
        _state: &Self::State,
        _operation: Self::UpdateOp,
    ) -> Result<(), SvcError> {
        unreachable!("No updates allowed")
    }
    fn start_create_op(&mut self, request: &Self::Create) {
        self.start_op(VolumeSnapshotOperation::Create(request.info.clone()));
    }
    fn start_destroy_op(&mut self) {
        self.start_op(VolumeSnapshotOperation::Destroy);
    }
    fn dirty(&self) -> bool {
        self.pending_op()
    }
    fn kind(&self) -> ResourceKind {
        ResourceKind::VolumeSnapshot
    }
    fn uuid_str(&self) -> String {
        self.uid().to_string()
    }
    fn status(&self) -> SpecStatus<Self::Status> {
        self.status().clone()
    }
    fn set_status(&mut self, status: SpecStatus<Self::Status>) {
        self.set_status(status);
    }
    fn operation_result(&self) -> Option<Option<bool>> {
        self.metadata().operation().as_ref().map(|r| r.result)
    }
}
