use crate::controller::{
    reconciler::{PollContext, TaskPoller},
    specs::OperationSequenceGuard,
    task_poller::{PollEvent, PollResult, PollTimer, PollTriggerEvent, PollerState},
};

use common_lib::types::v0::store::{volume::VolumeSpec, OperationGuardArc, TraceSpan, TraceStrLog};

use crate::controller::{
    operations::ResourceLifecycle, reconciler::GarbageCollect, specs::SpecOperationsHelper,
};
use common::errors::SvcError;
use common_lib::types::v0::{
    store::{nexus_persistence::NexusInfo, replica::ReplicaSpec},
    transport::{DestroyVolume, VolumeStatus},
};
use tracing::Instrument;

/// Volume Garbage Collector reconciler
#[derive(Debug)]
pub(super) struct GarbageCollector {
    counter: PollTimer,
}
impl GarbageCollector {
    /// Return a new `Self`
    pub(super) fn new() -> Self {
        Self {
            counter: PollTimer::from(5),
        }
    }
}

#[async_trait::async_trait]
impl TaskPoller for GarbageCollector {
    async fn poll(&mut self, context: &PollContext) -> PollResult {
        let mut results = vec![];
        for volume in context.specs().get_locked_volumes() {
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
}

#[tracing::instrument(level = "trace", skip(volume, context), fields(volume.uuid = %volume.uuid(), request.reconcile = true))]
async fn destroy_deleting_volume(
    volume: &mut OperationGuardArc<VolumeSpec>,
    context: &PollContext,
) -> PollResult {
    let deleting = volume.lock().status().deleting();
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
    let uuid = volume.lock().uuid.clone();
    match volume
        .destroy(context.registry(), &DestroyVolume::new(&uuid))
        .await
    {
        Ok(_) => {
            volume
                .lock()
                .info_span(|| tracing::info!("Successfully destroyed volume"));
            Ok(PollerState::Idle)
        }
        Err(error) => {
            volume
                .lock()
                .error_span(|| tracing::error!(error = %error, "Failed to destroy volume"));
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
    let volume_clone = volume.lock().clone();

    for nexus in context.specs().get_volume_nexuses(&volume_clone.uuid) {
        match &volume_clone.target {
            Some(target) if target.nexus() == &nexus.lock().uuid => continue,
            _ => {}
        };
        let nexus_clone = nexus.lock().clone();

        nexus_clone.warn_span(|| tracing::warn!("Attempting to disown unused nexus"));
        // the nexus garbage collector will destroy the disowned nexuses
        match context
            .specs()
            .disown_nexus(context.registry(), &nexus)
            .await
        {
            Ok(_) => {
                nexus_clone.info_span(|| tracing::info!("Successfully disowned unused nexus"));
            }
            Err(error) => {
                nexus_clone.error_span(|| tracing::error!("Failed to disown unused nexus"));
                results.push(Err(error));
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
    let volume_clone = volume.lock().clone();
    let target = match context.specs().get_volume_target_nexus(&volume_clone) {
        Some(target) => target,
        None => {
            // if the volume is not published, then leave the replicas around as they might
            // still reappear as online by the time we publish
            return PollResult::Ok(PollerState::Busy);
        }
    };
    if !target.lock().status().created() {
        // don't attempt to disown the replicas if the nexus that should own them is not stable
        return PollResult::Ok(PollerState::Busy);
    }

    let volume_state = context
        .registry()
        .get_volume_state(&volume_clone.uuid)
        .await?;
    if matches!(
        volume_state.status,
        VolumeStatus::Faulted | VolumeStatus::Unknown
    ) {
        // don't attempt to disown the replicas if the volume state is faulted or unknown
        return PollResult::Ok(PollerState::Busy);
    }

    let mut nexus_info = None; // defer reading from the persistent store unless we find a candidate
    let mut results = vec![];

    for replica in context.specs().get_volume_replicas(&volume_clone.uuid) {
        let replica = match replica.operation_guard() {
            Ok(guard) => guard,
            Err(_) => continue,
        };
        let replica_clone = replica.lock().clone();

        let replica_in_target = target.lock().contains_replica(&replica_clone.uuid);
        let replica_online = matches!(context.registry().get_replica(&replica_clone.uuid).await, Ok(state) if state.online());
        if !replica_online
            && replica_clone.owners.owned_by(&volume_clone.uuid)
            && !replica_clone.owners.owned_by_a_nexus()
            && !replica_in_target
            && !is_replica_healthy(context, &mut nexus_info, &replica_clone, &volume_clone).await?
        {
            volume_clone.warn_span(|| tracing::warn!(replica.uuid = %replica_clone.uuid, "Attempting to disown unused replica"));
            // the replica garbage collector will destroy the disowned replica
            match context
                .specs()
                .disown_volume_replica(context.registry(), &replica)
                .await
            {
                Ok(_) => {
                    volume_clone.info_span(|| tracing::info!(replica.uuid = %replica_clone.uuid, "Successfully disowned unused replica"));
                }
                Err(error) => {
                    volume_clone.error_span(|| tracing::error!(replica.uuid = %replica_clone.uuid, "Failed to disown unused replica"));
                    results.push(Err(error));
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
                .get_nexus_info(
                    Some(&volume_spec.uuid),
                    volume_spec.last_nexus_id.as_ref(),
                    true,
                )
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
