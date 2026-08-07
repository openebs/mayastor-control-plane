use crate::{
    controller::{
        registry::Registry,
        resources::{
            operations::{ResourceLifecycleWithLifetime, ResourceSnapshotting},
            operations_helper::{GuardedOperationsHelper, OnCreateFail, OperationSequenceGuard},
            OperationGuardArc,
        },
    },
    volume::snapshot_operations::DestroyVolumeSnapshotRequest,
};
use agents::errors::SvcError;
use stor_port::{
    transport_api::ResourceKind,
    types::v0::{
        store::{
            snapshots::{
                group::{
                    VolumeSnapshotGroup, VolumeSnapshotGroupCompleter,
                    VolumeSnapshotGroupCreateInfo, VolumeSnapshotGroupCreateResult,
                    VolumeSnapshotGroupUserSpec,
                },
                volume::VolumeSnapshotUserSpec,
            },
            volume::VolumeSpec,
        },
        transport::{SnapshotId, VolumeId},
    },
};

use chrono::Utc;

/// Maximum number of member volumes allowed in a snapshot group.
/// todo: make this configurable on the core agent (A9/T1.6).
pub(super) const MAX_SNAPSHOT_GROUP_MEMBERS: usize = 16;

/// Local create volume snapshot group request.
pub(crate) struct CreateVolumeSnapshotGroupRequest {
    /// The user specification of the group to create.
    pub(crate) spec: VolumeSnapshotGroupUserSpec,
}

/// Local destroy volume snapshot group request.
#[derive(Default)]
pub(crate) struct DestroyVolumeSnapshotGroupRequest {}

#[async_trait::async_trait]
impl ResourceLifecycleWithLifetime for OperationGuardArc<VolumeSnapshotGroup> {
    type Create<'a> = CreateVolumeSnapshotGroupRequest;
    type CreateOutput = Self;
    type Destroy = DestroyVolumeSnapshotGroupRequest;
    type DestroyOutput = ();

    async fn create(
        registry: &Registry,
        request: &Self::Create<'_>,
    ) -> Result<Self::CreateOutput, SvcError> {
        let spec = &request.spec;

        if spec.members().is_empty() {
            return Err(SvcError::InvalidArguments {});
        }
        if spec.members().len() > MAX_SNAPSHOT_GROUP_MEMBERS {
            return Err(SvcError::SnapshotGroupMaxMembers {
                group_id: spec.uuid().to_string(),
                members: spec.members().len(),
                max_members: MAX_SNAPSHOT_GROUP_MEMBERS,
            });
        }

        let mut group = registry
            .specs()
            .get_or_create_snapshot_group(spec)
            .operation_guard_wait()
            .await?;

        // A retried create must carry the exact same membership and quiesce mode.
        if group.as_ref().spec() != spec {
            return Err(SvcError::ReCreateMismatch {
                id: spec.uuid().to_string(),
                kind: ResourceKind::VolumeSnapshotGroup,
                resource: format!("{:?}", group.as_ref().spec()),
                request: format!("{spec:?}"),
            });
        }

        // Take all member volume guards upfront, in sorted order (deterministic
        // lock ordering), failing fast if any member volume does not exist or
        // has reached its snapshot limit.
        let mut volumes = member_volumes(registry, spec).await?;

        let completer = VolumeSnapshotGroupCompleter::default();
        group
            .start_create_update(registry, &VolumeSnapshotGroupCreateInfo::new(&completer))
            .await?;

        // A previous incomplete attempt may have left member snapshots behind; they
        // are not consistent with the snapshots this attempt takes, so clear them out.
        let result = match destroy_member_snapshots(registry, &mut volumes, spec).await {
            Ok(()) => create_member_snapshots(registry, &mut volumes, spec).await,
            Err(error) => Err(error),
        };
        let result = match result {
            Ok(()) => {
                let result = VolumeSnapshotGroupCreateResult::new(Utc::now());
                *completer.lock().unwrap() = Some(result);
                Ok(())
            }
            Err(error) => {
                // All-or-nothing: roll back every member snapshot of the group.
                destroy_member_snapshots(registry, &mut volumes, spec)
                    .await
                    .ok();
                Err(error)
            }
        };

        // The rollback above is best-effort so let the group be garbage collectable,
        // allowing any left-over member snapshots to be cleaned up.
        group
            .complete_create(result, registry, OnCreateFail::SetDeleting)
            .await?;

        Ok(group)
    }

    async fn destroy(
        &mut self,
        registry: &Registry,
        _request: &Self::Destroy,
    ) -> Result<(), SvcError> {
        self.start_destroy(registry).await?;

        let spec = self.as_ref().spec().clone();
        let mut result = Ok(());
        // Attempt to destroy every member snapshot, keeping the first error.
        for (volume_id, snapshot_id) in sorted_members(&spec) {
            let Some(snapshot) = registry.specs().volume_snapshot_rsc(&snapshot_id) else {
                continue;
            };
            let destroy_result = match registry.specs().volume(&volume_id).await {
                Ok(mut volume) => {
                    volume
                        .destroy_snap(
                            registry,
                            &DestroyVolumeSnapshotRequest::new(
                                snapshot,
                                Some(volume_id.clone()),
                                snapshot_id.clone(),
                            ),
                        )
                        .await
                }
                Err(SvcError::VolumeNotFound { .. }) => match snapshot.operation_guard_wait().await
                {
                    Ok(mut snapshot_guard) => {
                        snapshot_guard
                            .destroy(
                                registry,
                                &DestroyVolumeSnapshotRequest::new(
                                    snapshot.clone(),
                                    None,
                                    snapshot_id.clone(),
                                ),
                            )
                            .await
                    }
                    Err(error) => Err(error),
                },
                Err(error) => Err(error),
            };
            if let Err(error) = destroy_result {
                tracing::error!(
                    %error,
                    snapshot.uuid = %snapshot_id,
                    volume.uuid = %volume_id,
                    snapshot_group.uuid = %spec.uuid(),
                    "Failed to destroy snapshot group member snapshot"
                );
                if result.is_ok() {
                    result = Err(error);
                }
            }
        }

        self.complete_destroy(result, registry).await
    }
}

/// The group members as a list of volume/snapshot id pairs, sorted by volume id
/// for deterministic lock ordering.
fn sorted_members(spec: &VolumeSnapshotGroupUserSpec) -> Vec<(VolumeId, SnapshotId)> {
    spec.sorted_volumes()
        .into_iter()
        .filter_map(|volume_id| {
            let snapshot_id = spec.members().get(&volume_id)?.clone();
            Some((volume_id, snapshot_id))
        })
        .collect()
}

/// Take the operation guards of all member volumes in sorted order, validating
/// that every member volume can take another snapshot.
async fn member_volumes(
    registry: &Registry,
    spec: &VolumeSnapshotGroupUserSpec,
) -> Result<Vec<OperationGuardArc<VolumeSpec>>, SvcError> {
    let mut volumes = Vec::with_capacity(spec.members().len());
    for volume_id in spec.sorted_volumes() {
        let volume = registry.specs().volume(&volume_id).await?;
        if let Some(max_snapshots) = volume.as_ref().max_snapshots {
            if volume.as_ref().metadata.num_snapshots() as u32 >= max_snapshots {
                return Err(SvcError::SnapshotMaxLimit {
                    max_snapshots,
                    volume_id: volume.as_ref().uuid.to_string(),
                });
            }
        }
        volumes.push(volume);
    }
    Ok(volumes)
}

/// Create a snapshot of every member volume, stopping at the first failure.
async fn create_member_snapshots(
    registry: &Registry,
    volumes: &mut [OperationGuardArc<VolumeSpec>],
    spec: &VolumeSnapshotGroupUserSpec,
) -> Result<(), SvcError> {
    // todo: quiesce fan-out hook (freeze-all before, thaw-all after) lands here (T3.1).
    for volume in volumes.iter_mut() {
        let Some(snapshot_id) = spec.members().get(volume.uuid()).cloned() else {
            continue;
        };
        if let Err(error) = volume
            .create_snap(
                registry,
                &VolumeSnapshotUserSpec::new(volume.uuid(), snapshot_id.clone()),
            )
            .await
        {
            tracing::error!(
                %error,
                snapshot.uuid = %snapshot_id,
                volume.uuid = %volume.uuid(),
                snapshot_group.uuid = %spec.uuid(),
                "Failed to create snapshot group member snapshot"
            );
            return Err(error);
        }
    }
    Ok(())
}

/// Destroy the existing snapshots of every member volume, attempting all members
/// and keeping the first error.
async fn destroy_member_snapshots(
    registry: &Registry,
    volumes: &mut [OperationGuardArc<VolumeSpec>],
    spec: &VolumeSnapshotGroupUserSpec,
) -> Result<(), SvcError> {
    let mut result = Ok(());
    for volume in volumes.iter_mut() {
        let Some(snapshot_id) = spec.members().get(volume.uuid()).cloned() else {
            continue;
        };
        let Some(snapshot) = registry.specs().volume_snapshot_rsc(&snapshot_id) else {
            continue;
        };
        if let Err(error) = volume
            .destroy_snap(
                registry,
                &DestroyVolumeSnapshotRequest::new(
                    snapshot,
                    Some(volume.uuid().clone()),
                    snapshot_id.clone(),
                ),
            )
            .await
        {
            tracing::error!(
                %error,
                snapshot.uuid = %snapshot_id,
                volume.uuid = %volume.uuid(),
                snapshot_group.uuid = %spec.uuid(),
                "Failed to destroy snapshot group member snapshot"
            );
            if result.is_ok() {
                result = Err(error);
            }
        }
    }
    result
}
