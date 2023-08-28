use crate::{
    controller::{
        registry::Registry,
        resources::{
            operations::{ResourceCloning, ResourceLifecycle, ResourceLifecycleExt},
            operations_helper::{GuardedOperationsHelper, SpecOperationsHelper},
            OperationGuardArc, TraceStrLog,
        },
        scheduling::{volume::CloneVolumeSnapshot, ResourceFilter},
    },
    volume::operations::{Context, CreateVolumeExe, CreateVolumeExeVal, CreateVolumeSource},
};
use agents::errors::{self, SvcError};
use stor_port::{
    transport_api::ErrorChain,
    types::v0::{
        store::{
            replica::ReplicaSpec,
            snapshots::volume::{
                CreateRestoreInfo, DestroyRestoreInfo, VolumeSnapshot, VolumeSnapshotOperation,
            },
            volume::{VolumeContentSource, VolumeSpec},
        },
        transport::{
            CreateSnapshotVolume, DestroyVolume, Replica, SnapshotCloneId, SnapshotCloneParameters,
            SnapshotCloneSpecParams,
        },
    },
};

pub(crate) struct SnapshotCloneOp<'a>(
    pub(crate) &'a CreateSnapshotVolume,
    pub(crate) &'a mut OperationGuardArc<VolumeSnapshot>,
);

impl SnapshotCloneOp<'_> {
    /// Get a Snapshot Source for VolumeContentSource from the same.
    pub(crate) fn to_snapshot_source(&self) -> VolumeContentSource {
        VolumeContentSource::new_snapshot_source(
            self.1.uuid().clone(),
            self.1.as_ref().spec().source_id().clone(),
        )
    }
}

#[async_trait::async_trait]
impl ResourceCloning for OperationGuardArc<VolumeSnapshot> {
    type Create = CreateSnapshotVolume;
    type CreateOutput = OperationGuardArc<VolumeSpec>;
    type Destroy = DestroyVolume;

    async fn create_clone(
        &mut self,
        registry: &Registry,
        request: &Self::Create,
    ) -> Result<Self::CreateOutput, SvcError> {
        let spec_clone = self
            .start_update(
                registry,
                self.as_ref(),
                VolumeSnapshotOperation::CreateRestore(CreateRestoreInfo::new(
                    request.params().uuid.clone(),
                )),
            )
            .await?;
        let request = CreateVolumeSource::Snapshot(SnapshotCloneOp(request, self));

        // Create the restore using the volume op guard.
        let create_result = OperationGuardArc::<VolumeSpec>::create_ext(registry, &request).await;

        self.complete_update(registry, create_result, spec_clone)
            .await
    }

    async fn destroy_clone(
        &mut self,
        registry: &Registry,
        request: &Self::Destroy,
        mut volume: OperationGuardArc<VolumeSpec>,
    ) -> Result<(), SvcError> {
        let spec_clone = self
            .start_update(
                registry,
                self.as_ref(),
                VolumeSnapshotOperation::DestroyRestore(DestroyRestoreInfo::new(
                    request.uuid.clone(),
                )),
            )
            .await?;

        // Destroy the restore using the volume op guard.
        let destroy_result = volume.destroy(registry, request).await;

        self.complete_update(registry, destroy_result, spec_clone)
            .await
    }
}

impl CreateVolumeExeVal for SnapshotCloneOp<'_> {
    fn pre_flight_check(&self) -> Result<(), SvcError> {
        let new_volume = self.0.params();
        let snapshot = self.1.as_ref();

        snafu::ensure!(
            new_volume.replicas == 1,
            errors::NReplSnapshotCloneCreationNotAllowed {}
        );
        snafu::ensure!(
            new_volume.allowed_nodes().is_empty()
                || new_volume.allowed_nodes().len() >= new_volume.replicas as usize,
            errors::InvalidArguments {}
        );
        snafu::ensure!(new_volume.thin, errors::ClonedSnapshotVolumeThin {});
        snafu::ensure!(snapshot.status().created(), errors::SnapshotNotCreated {});
        snafu::ensure!(
            snapshot.metadata().replica_snapshots().map(|s| s.len()) == Some(1),
            errors::ClonedSnapshotVolumeRepl {}
        );
        snafu::ensure!(
            new_volume.size == snapshot.metadata().spec_size(),
            errors::ClonedSnapshotVolumeSize {}
        );

        Ok(())
    }
}

#[async_trait::async_trait]
impl CreateVolumeExe for SnapshotCloneOp<'_> {
    type Candidates = SnapshotCloneSpecParams;

    async fn setup<'a>(&'a self, context: &mut Context<'a>) -> Result<Self::Candidates, SvcError> {
        self.cloneable_snapshot(context).await
    }

    async fn create<'a>(
        &'a self,
        context: &mut Context<'a>,
        clone_replica: Self::Candidates,
    ) -> Vec<Replica> {
        match OperationGuardArc::<ReplicaSpec>::create_ext(context.registry, &clone_replica).await {
            Ok(replica) => vec![replica],
            Err(error) => {
                context.volume.error(&format!(
                    "Failed to create replica {:?} for volume, error: {}",
                    clone_replica,
                    error.full_string()
                ));
                vec![]
            }
        }
    }

    async fn undo<'a>(&'a self, _context: &mut Context<'a>, _replicas: Vec<Replica>) {
        // nothing to undo since we only support 1-replica snapshot
    }
}

impl SnapshotCloneOp<'_> {
    /// Return healthy replica snapshots for volume snapshot cloning.
    async fn cloneable_snapshot(
        &self,
        context: &Context<'_>,
    ) -> Result<SnapshotCloneSpecParams, SvcError> {
        let new_volume = context.volume.as_ref();
        let registry = context.registry;
        let snapshot = self.1.as_ref();

        // Already validated by CreateVolumeExeVal

        let snapshots = snapshot.metadata().replica_snapshots();
        let snapshot = match snapshots.map(|s| s.as_slice()) {
            Some([replica]) => Ok(replica),
            None | Some([]) => Err(SvcError::NoHealthyReplicas {
                id: new_volume.uuid_str(),
            }),
            _ => Err(SvcError::NReplSnapshotCloneCreationNotAllowed {}),
        }?;

        let mut pools = CloneVolumeSnapshot::builder_with_defaults(registry, new_volume, snapshot)
            .await
            .collect();

        let pool = match pools.pop() {
            Some(pool) if pools.is_empty() => Ok(pool),
            // todo: support more than 1 replica snapshots
            Some(_) => Err(SvcError::NReplSnapshotCloneCreationNotAllowed {}),
            // todo: filtering should keep invalid pools/resources and tag them with a reason
            //   why they cannot be used!
            None => Err(SvcError::NoSnapshotPools {
                id: snapshot.spec().uuid().to_string(),
            }),
        }?;

        let clone_id = SnapshotCloneId::new();
        let clone_name = clone_id.to_string();
        let repl_params =
            SnapshotCloneParameters::new(snapshot.spec().uuid().clone(), clone_name, clone_id);
        Ok(SnapshotCloneSpecParams::new(
            repl_params,
            snapshot.meta().source_spec_size(),
            pool.pool().pool_ref(),
            pool.pool.node.clone(),
            new_volume.uuid.clone(),
        ))
    }
}
