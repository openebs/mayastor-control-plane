use crate::controller::{
    registry::Registry,
    resources::{
        operations::ResourceSnapshotting,
        operations_helper::{GuardedOperationsHelper, OperationSequenceGuard},
        OperationGuardArc,
    },
};
use agents::errors::SvcError;

use stor_port::types::v0::store::volume::VolumeSpec;

use crate::{
    controller::resources::{
        operations::ResourceLifecycleWithLifetime, operations_helper::OnCreateFail,
    },
    volume::snapshot_helpers::CreateVolumeSnapshot,
};
use stor_port::types::v0::store::snapshots::volume::{
    VolumeSnapshot, VolumeSnapshotCreateInfo, VolumeSnapshotCreateResult,
};

#[async_trait::async_trait]
impl ResourceSnapshotting for OperationGuardArc<VolumeSpec> {
    type Create = ();
    type CreateOutput = ();
    type Destroy = ();
    type List = ();
    type ListOutput = ();

    async fn create_snap(
        &mut self,
        registry: &Registry,
        _request: &Self::Create,
    ) -> Result<Self::CreateOutput, SvcError> {
        OperationGuardArc::<VolumeSnapshot>::create(
            registry,
            &CreateVolumeSnapshotRequest {
                volume: self,
                request: (),
            },
        )
        .await?;

        Ok(())
    }

    async fn list_snaps(
        &self,
        _registry: &Registry,
        _request: &Self::List,
    ) -> Result<Self::ListOutput, SvcError> {
        todo!()
    }

    async fn destroy_snap(
        &mut self,
        _registry: &Registry,
        _request: &Self::Destroy,
    ) -> Result<(), SvcError> {
        todo!()
    }
}

/// Local create a volume snapshot request.
pub(crate) struct CreateVolumeSnapshotRequest<'a> {
    /// A mutable reference to the volume which will own this snapshot.
    /// This helps us mutate it if necessary but most of all ensure nothing else is modifying
    /// the volume.
    volume: &'a mut OperationGuardArc<VolumeSpec>,
    /// Any request specific info - TBD.
    request: (),
}

#[async_trait::async_trait]
impl ResourceLifecycleWithLifetime for OperationGuardArc<VolumeSnapshot> {
    type Create<'a> = CreateVolumeSnapshotRequest<'a>;
    type CreateOutput = ();
    type Destroy<'a> = ();

    async fn create(
        registry: &Registry,
        request: &Self::Create<'_>,
    ) -> Result<Self::CreateOutput, SvcError> {
        // we have the guard, so we may create the snapshot..
        if request.volume.published() {
            tracing::trace!("snapshot through the nexus");
        } else {
            tracing::trace!("snapshot through the replicas directly");
        }
        let request = &request.request;

        let specs = registry.specs();
        let snapshot = specs
            .get_or_create_snapshot(request)
            .operation_guard_wait()
            .await?;
        let complete = std::sync::Arc::new(std::sync::Mutex::new(VolumeSnapshotCreateResult {
            replicas: vec![],
        }));
        let _snapshot_cln = snapshot
            .start_create(
                registry,
                &CreateVolumeSnapshot {
                    request: *request,
                    info: VolumeSnapshotCreateInfo::new("", vec![], complete.clone()),
                },
            )
            .await?;

        // create snapshots..
        let result = Ok(());
        if let Ok(ref _result) = result {
            // update replica snapshot result
            complete.lock().unwrap().replicas = vec![];
        }

        snapshot
            .complete_create(result, registry, OnCreateFail::LeaveAsIs)
            .await?;

        Ok(())
    }

    async fn destroy(
        &mut self,
        registry: &Registry,
        _request: &Self::Destroy<'_>,
    ) -> Result<(), SvcError> {
        self.start_destroy_by(registry, &()).await?;
        let result = Ok(());
        self.complete_destroy(result, registry).await
    }
}
