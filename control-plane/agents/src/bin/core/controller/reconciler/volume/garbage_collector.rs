use crate::controller::{
    reconciler::{GarbageCollect, PollContext, TaskPoller},
    resources::{
        operations::ResourceLifecycle,
        operations_helper::{OperationSequenceGuard, SpecOperationsHelper},
        OperationGuardArc, TraceSpan, TraceStrLog,
    },
    task_poller::{PollEvent, PollResult, PollTimer, PollTriggerEvent, PollerState},
};

use crate::controller::{
    reconciler::poller::ReconcilerWorker,
    resources::{operations::ResourceOwnerUpdate, ResourceMutex, ResourceUid},
};
use agents::errors::SvcError;
use stor_port::types::v0::{
    store::{
        nexus::NexusSpec, nexus_persistence::NexusInfo, replica::ReplicaSpec, volume::VolumeSpec,
    },
    transport::{DestroyVolume, ReplicaOwners, VolumeStatus},
};
use tracing::Instrument;

/// Volume Garbage Collector reconciler.
#[derive(Debug)]
pub(super) struct GarbageCollector {
    counter: PollTimer,
}
impl GarbageCollector {
    /// Return a new `Self`.
    pub(super) fn new() -> Self {
        Self {
            counter: ReconcilerWorker::garbage_collection_period(),
        }
    }
}

#[async_trait::async_trait]
impl TaskPoller for GarbageCollector {
    async fn poll(&mut self, context: &PollContext) -> PollResult {
        let mut results = vec![];
        for volume in context.specs().volumes_rsc() {
            let mut volume = match volume.operation_guard() {
                Ok(guard) => guard,
                Err(_) => continue,
            };
            results.push(volume.garbage_collect(context).await);
        }
        Self::squash_results(results)
    }

    async fn poll_timer(&mut self, _context: &PollContext) -> bool {
        self.counter.poll()
    }

    async fn poll_event(&mut self, context: &PollContext) -> bool {
        match context.event() {
            PollEvent::TimedRun
            | PollEvent::Triggered(PollTriggerEvent::VolumeDegraded)
            | PollEvent::Triggered(PollTriggerEvent::ResourceCreatingToDeleting)
            | PollEvent::Triggered(PollTriggerEvent::Start) => true,
            PollEvent::Shutdown | PollEvent::Triggered(_) => false,
        }
    }
}

#[async_trait::async_trait]
impl GarbageCollect for OperationGuardArc<VolumeSpec> {
    async fn garbage_collect(&mut self, context: &PollContext) -> PollResult {
        GarbageCollector::squash_results(vec![
            self.destroy_deleting(context).await,
            self.disown_unused(context).await,
            self.disown_invalid(context).await,
        ])
    }

    async fn destroy_deleting(&mut self, context: &PollContext) -> PollResult {
        destroy_deleting_volume(self, context).await
    }

    async fn destroy_orphaned(&mut self, _context: &PollContext) -> PollResult {
        unimplemented!()
    }

    async fn disown_unused(&mut self, context: &PollContext) -> PollResult {
        GarbageCollector::squash_results(vec![
            disown_unused_nexuses(self, context).await,
            disown_unused_replicas(self, context).await,
        ])
    }

    async fn disown_orphaned(&mut self, _context: &PollContext) -> PollResult {
        unimplemented!()
    }

    // Replicas which are local to nexuses which did not undergo a graceful
    // shutdown and since we cannot take reservation on local replicas we should rather
    // disown them.
    async fn disown_invalid(&mut self, context: &PollContext) -> PollResult {
        disown_non_reservable_replicas(self, context).await
    }
}

#[tracing::instrument(level = "trace", skip(volume, context), fields(volume.uuid = %volume.uuid(), request.reconcile = true))]
async fn destroy_deleting_volume(
    volume: &mut OperationGuardArc<VolumeSpec>,
    context: &PollContext,
) -> PollResult {
    let deleting = volume.as_ref().status().deleting();
    if deleting {
        destroy_volume(volume, context)
            .instrument(tracing::info_span!(
                "destroy_deleting_volume",
                request.reconcile = true
            ))
            .await
    } else {
        PollResult::Ok(PollerState::Idle)
    }
}

async fn destroy_volume(
    volume: &mut OperationGuardArc<VolumeSpec>,
    context: &PollContext,
) -> PollResult {
    let volume_uuid = &volume.immutable_arc().uuid;
    match volume
        .destroy(context.registry(), &DestroyVolume::new(volume_uuid))
        .await
    {
        Ok(_) => {
            volume.info_span(|| tracing::info!("Successfully destroyed volume"));
            Ok(PollerState::Idle)
        }
        Err(error) => {
            volume.error_span(|| tracing::error!(error = %error, "Failed to destroy volume"));
            Err(error)
        }
    }
}

/// Given a volume
/// When any of its nexuses are no longer used
/// Then they should be disowned
/// And they should eventually be destroyed
#[tracing::instrument(level = "debug", skip(context, volume), fields(volume.uuid = %volume.uuid(), request.reconcile = true))]
async fn disown_unused_nexuses(
    volume: &mut OperationGuardArc<VolumeSpec>,
    context: &PollContext,
) -> PollResult {
    let mut results = vec![];

    for nexus in context.specs().volume_nexuses(volume.uuid()) {
        if let Ok(mut nexus) = nexus.operation_guard() {
            if let Some(target) = volume.as_ref().target() {
                if target.nexus() == nexus.uid() || nexus.as_ref().is_shutdown() {
                    continue;
                }
            }

            nexus.warn_span(|| tracing::warn!("Attempting to disown unused nexus"));
            // the nexus garbage collector will destroy the disowned nexuses
            match nexus.disown(context.registry()).await {
                Ok(_) => {
                    nexus.info_span(|| tracing::info!("Successfully disowned unused nexus"));
                }
                Err(error) => {
                    nexus.error_span(|| tracing::error!("Failed to disown unused nexus"));
                    results.push(Err(error));
                }
            }
        }
    }

    GarbageCollector::squash_results(results)
}

/// Given a published volume
/// When some of its replicas are not healthy, not online and not used by a nexus
/// Then they should be disowned
#[tracing::instrument(level = "debug", skip(context, volume), fields(volume.uuid = %volume.uuid(), request.reconcile = true))]
async fn disown_unused_replicas(
    volume: &mut OperationGuardArc<VolumeSpec>,
    context: &PollContext,
) -> PollResult {
    let target = match context.specs().volume_target_nexus_rsc(volume.as_ref()) {
        Some(target) => target.operation_guard()?,
        None => {
            // if the volume is not published, then leave the replicas around as they might
            // still reappear as online by the time we publish
            return PollResult::Ok(PollerState::Busy);
        }
    };
    if !target.as_ref().status().created() || target.as_ref().dirty() {
        // don't attempt to disown the replicas if the nexus that should own them is not stable
        return PollResult::Ok(PollerState::Busy);
    }

    let volume_state = context.registry().volume_state(volume.uuid()).await?;
    if matches!(
        volume_state.status,
        VolumeStatus::Faulted | VolumeStatus::Unknown
    ) {
        // don't attempt to disown the replicas if the volume state is faulted or unknown
        return PollResult::Ok(PollerState::Busy);
    }

    let mut nexus_info = None; // defer reading from the persistent store unless we find a candidate
    let mut results = vec![];

    for replica in context.specs().volume_replicas(volume.uuid()) {
        let mut replica = match replica.operation_guard() {
            Ok(guard) => guard,
            Err(_) => continue,
        };

        let replica_in_target = target.as_ref().contains_replica(&replica.as_ref().uuid);
        let replica_online = matches!(context.registry().replica(&replica.as_ref().uuid).await, Ok(state) if state.online());
        if !replica_online
            && !replica.as_ref().dirty()
            && replica.as_ref().owners.owned_by(volume.uuid())
            && !replica.as_ref().owners.owned_by_a_nexus()
            && !replica_in_target
            && !is_replica_healthy(context, &mut nexus_info, replica.as_ref(), volume.as_ref())
                .await?
        {
            volume.warn_span(|| tracing::warn!(replica.uuid = %replica.as_ref().uuid, "Attempting to disown unused replica"));

            // the replica garbage collector will destroy the disowned replica
            match replica
                .remove_owners(
                    context.registry(),
                    &ReplicaOwners::from_volume(volume.uuid()),
                    true,
                )
                .await
            {
                Ok(_) => {
                    volume.info_span(|| tracing::info!(replica.uuid = %replica.as_ref().uuid, "Successfully disowned unused replica"));
                }
                Err(error) => {
                    volume.error_span(|| tracing::error!(replica.uuid = %replica.as_ref().uuid, "Failed to disown unused replica"));
                    results.push(Err(error));
                }
            }
        }
    }

    GarbageCollector::squash_results(results)
}

/// After republishing if the nexus was not shutdown gracefully, we should disown its local
/// child as that is non reservable.
#[tracing::instrument(level = "debug", skip(context, volume), fields(volume.uuid = %volume.uuid(), request.reconcile = true))]
async fn disown_non_reservable_replicas(
    volume: &mut OperationGuardArc<VolumeSpec>,
    context: &PollContext,
) -> PollResult {
    let target = match context.specs().volume_target_nexus_rsc(volume.as_ref()) {
        Some(target) => target.operation_guard()?,
        None => {
            // if the volume is not published, then leave the replicas around as they might
            // still reappear as online by the time we publish
            return PollResult::Ok(PollerState::Idle);
        }
    };
    if !target.as_ref().status().created()
        || target.as_ref().is_shutdown()
        || target.as_ref().dirty()
    {
        // don't attempt to disown the replicas if the nexus that should own them is not stable
        return PollResult::Ok(PollerState::Idle);
    }

    let volume_state = context.registry().volume_state(volume.uuid()).await?;
    if matches!(
        volume_state.status,
        VolumeStatus::Faulted | VolumeStatus::Unknown
    ) {
        // don't attempt to disown the replicas if the volume state is faulted or unknown
        return PollResult::Ok(PollerState::Idle);
    }

    let mut nexus_info = None;
    let mut results = vec![];

    // Fetch all nexuses that are not properly shutdown
    let shutdown_failed_nexuses: Vec<ResourceMutex<NexusSpec>> = context
        .registry()
        .specs()
        .volume_failed_shutdown_nexuses(volume.uuid())
        .await;

    // Remove the local child from the shutdown pending nexuses as they are
    // non reservable.
    for nexus in shutdown_failed_nexuses {
        let children = nexus.lock().clone().children;
        for child in children {
            if let Some(replica_uri) = child.as_replica() {
                if replica_uri.uri().is_local() {
                    if let Ok(mut replica) = context.specs().replica(replica_uri.uuid()).await {
                        if !replica.as_ref().dirty()
                            && replica.as_ref().owners.owned_by(volume.uuid())
                            && !replica
                                .as_ref()
                                .owners
                                .nexuses()
                                .iter()
                                .any(|p| p != nexus.uuid())
                            && !is_replica_healthy(
                                context,
                                &mut nexus_info,
                                replica.as_ref(),
                                volume.as_ref(),
                            )
                            .await?
                        {
                            volume.warn_span(|| tracing::warn!(replica.uuid = %replica.as_ref().uuid, "Attempting to disown non reservable replica"));

                            // Since its a local replica of a shutting down nexus i.e. a failed
                            // shutdown nexus we don't want it
                            // to be a part of anything. Once this replica has no owner, the
                            // garbage collector will remove the nexus. Even if the clean up is
                            // delayed we since this replica is
                            // anyhow not a part of the volume, we should be safe.
                            match replica
                                .remove_owners(
                                    context.registry(),
                                    &ReplicaOwners::new_disown_all(),
                                    true,
                                )
                                .await
                            {
                                Ok(_) => {
                                    volume.info_span(|| tracing::info!(replica.uuid = %replica.as_ref().uuid, "Successfully disowned non reservable replica"));
                                }
                                Err(error) => {
                                    volume.error_span(|| tracing::error!(replica.uuid = %replica.as_ref().uuid, "Failed to disown non reservable replica"));
                                    results.push(Err(error));
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    GarbageCollector::squash_results(results)
}

async fn is_replica_healthy(
    context: &PollContext,
    nexus_info: &mut Option<NexusInfo>,
    replica_spec: &ReplicaSpec,
    volume_spec: &VolumeSpec,
) -> Result<bool, SvcError> {
    let info = match &nexus_info {
        None => {
            *nexus_info = context
                .registry()
                .nexus_info(Some(&volume_spec.uuid), volume_spec.health_info_id(), true)
                .await?;
            match nexus_info {
                Some(info) => info,
                None => {
                    // this should not happen unless the persistent store is corrupted somehow
                    volume_spec.error("Persistent NexusInformation is not available");
                    return Err(SvcError::Internal {
                        details: "Persistent NexusInformation is not available".to_string(),
                    });
                }
            }
        }
        Some(info) => info,
    };
    if info.no_healthy_replicas() {
        Err(SvcError::NoHealthyReplicas {
            id: volume_spec.uuid_str(),
        })
    } else {
        Ok(info.is_replica_healthy(&replica_spec.uuid))
    }
}
