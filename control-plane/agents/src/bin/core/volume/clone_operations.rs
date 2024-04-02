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
            new_volume.allowed_nodes().is_empty()
                || new_volume.allowed_nodes().len() >= new_volume.replicas as usize,
            errors::InvalidArguments {}
        );
        snafu::ensure!(new_volume.thin, errors::ClonedSnapshotVolumeThin {});
        snafu::ensure!(snapshot.status().created(), errors::SnapshotNotCreated {});
        snafu::ensure!(
            new_volume.size == snapshot.metadata().spec_size(),
            errors::ClonedSnapshotVolumeSize {}
        );

        Ok(())
    }
}

#[async_trait::async_trait]
impl CreateVolumeExe for SnapshotCloneOp<'_> {
    type Candidates = Vec<SnapshotCloneSpecParams>;

    async fn setup<'a>(&'a self, context: &mut Context<'a>) -> Result<Self::Candidates, SvcError> {
        let clonable_snapshots = self.cloneable_snapshot(context).await?;
        let volume = context.volume.as_ref();
        if volume.num_replicas > clonable_snapshots.len() as u8 {
            return Err(SvcError::InsufficientSnapshotsForClone {
                snapshots: clonable_snapshots.len() as u8,
                replicas: volume.num_replicas,
                id: volume.uuid_str(),
            });
        }
        Ok(clonable_snapshots)
    }

    async fn create<'a>(
        &'a self,
        context: &mut Context<'a>,
        clone_replicas: Self::Candidates,
    ) -> Vec<Replica> {
        let mut replicas = Vec::new();
        let volume_replicas = context.volume.as_ref().num_replicas as usize;
        // todo: need to add new replica and do full rebuild, if clonable snapshots
        // count is less than volume replicas count.
        for (idx, clone_replica) in clone_replicas.iter().enumerate() {
            match OperationGuardArc::<ReplicaSpec>::create_ext(context.registry, clone_replica)
                .await
            {
                Ok(replica) => {
                    replicas.push(replica);
                    if idx + 1 == volume_replicas {
                        break;
                    }
                }
                Err(error) => {
                    context.volume.error(&format!(
                        "Failed to create replica {:?} for volume, error: {}",
                        clone_replica,
                        error.full_string()
                    ));
                }
            }
        }
        replicas
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
    ) -> Result<Vec<SnapshotCloneSpecParams>, SvcError> {
        let new_volume = context.volume.as_ref();
        let registry = context.registry;
        let snapshot = self.1.as_ref();

        let snapshots = match snapshot.metadata().replica_snapshots() {
            Some(snapshots) if !snapshots.is_empty() => Ok(snapshots),
            _ => Err(SvcError::NoHealthyReplicas {
                id: new_volume.uuid_str(),
            }),
        }?;

        let mut pools = CloneVolumeSnapshot::builder_with_defaults(registry, new_volume, snapshots)
            .await
            .collect();
        if pools.len() != snapshots.len() || pools.is_empty() {
            return Err(SvcError::NoSnapshotPools {
                id: snapshot.spec().uuid().to_string(),
            });
        }

        let mut clone_spec_params = vec![];
        for snapshot in snapshots {
            let clone_id = SnapshotCloneId::new();
            let clone_name = clone_id.to_string();
            let repl_params =
                SnapshotCloneParameters::new(snapshot.spec().uuid().clone(), clone_name, clone_id);
            let pool = pools.remove(0);
            let clone_spec_param = SnapshotCloneSpecParams::new(
                repl_params,
                snapshot.meta().source_spec_size(),
                pool.pool().pool_ref(),
                pool.pool.node.clone(),
                new_volume.uuid.clone(),
            );
            clone_spec_params.push(clone_spec_param);
        }
        Ok(clone_spec_params)
    }
}
