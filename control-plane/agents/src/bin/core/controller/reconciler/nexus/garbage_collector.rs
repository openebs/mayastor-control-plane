use crate::controller::{
    reconciler::{poller::ReconcilerWorker, GarbageCollect, PollContext, TaskPoller},
    resources::{
        operations::{ResourceLifecycle, ResourceOwnerUpdate},
        operations_helper::{OperationSequenceGuard, SpecOperationsHelper},
        OperationGuardArc, TraceSpan,
    },
    task_poller::{PollEvent, PollResult, PollTimer, PollTriggerEvent, PollerState},
};
use stor_port::types::v0::{
    store::nexus::NexusSpec,
    transport::{DestroyNexus, NexusOwners},
};
use tracing::Instrument;

/// Nexus Garbage Collector reconciler
#[derive(Debug)]
pub(super) struct GarbageCollector {
    counter: PollTimer,
}
impl GarbageCollector {
    /// Return a new `Self`
    pub(super) fn new() -> Self {
        Self {
            counter: ReconcilerWorker::garbage_collection_period(),
        }
    }
}

#[async_trait::async_trait]
impl TaskPoller for GarbageCollector {
    async fn poll(&mut self, context: &PollContext) -> PollResult {
        let nexuses = context.specs().nexuses();
        for nexus in nexuses {
            let mut nexus = match nexus.operation_guard() {
                Ok(guard) => guard,
                Err(_) => continue,
            };
            let _ = nexus.garbage_collect(context).await;
        }
        PollResult::Ok(PollerState::Idle)
    }

    async fn poll_timer(&mut self, _context: &PollContext) -> bool {
        self.counter.poll()
    }

    async fn poll_event(&mut self, context: &PollContext) -> bool {
        match context.event() {
            PollEvent::TimedRun
            | PollEvent::Triggered(PollTriggerEvent::Start)
            | PollEvent::Triggered(PollTriggerEvent::ResourceCreatingToDeleting) => true,
            PollEvent::Shutdown | PollEvent::Triggered(_) => false,
        }
    }
}

#[async_trait::async_trait]
impl GarbageCollect for OperationGuardArc<NexusSpec> {
    async fn garbage_collect(&mut self, context: &PollContext) -> PollResult {
        GarbageCollector::squash_results(vec![
            self.disown_orphaned(context).await,
            self.destroy_orphaned(context).await,
            self.destroy_deleting(context).await,
        ])
    }

    async fn destroy_deleting(&mut self, context: &PollContext) -> PollResult {
        destroy_deleting_nexus(self, context).await
    }

    async fn destroy_orphaned(&mut self, context: &PollContext) -> PollResult {
        destroy_disowned_nexus(self, context).await
    }

    async fn disown_unused(&mut self, _context: &PollContext) -> PollResult {
        unimplemented!()
    }

    async fn disown_orphaned(&mut self, context: &PollContext) -> PollResult {
        destroy_orphaned_nexus(self, context).await
    }

    async fn disown_invalid(&mut self, _context: &PollContext) -> PollResult {
        unimplemented!()
    }
}

/// Given a control plane managed nexus
/// When a nexus is owned by a volume which no longer exists
/// Then the nexus should be disowned
/// And it should eventually be destroyed
#[tracing::instrument(level = "debug", skip(nexus, context), fields(nexus.uuid = %nexus.uuid(), request.reconcile = true))]
async fn destroy_orphaned_nexus(
    nexus: &mut OperationGuardArc<NexusSpec>,
    context: &PollContext,
) -> PollResult {
    if !nexus.as_ref().managed {
        return PollResult::Ok(PollerState::Idle);
    }

    if let Some(owner) = &nexus.as_ref().owner {
        if context.specs().volume_rsc(owner).is_none() {
            let owner = NexusOwners::All;
            nexus.warn_span(|| tracing::warn!("Attempting to disown orphaned nexus"));
            nexus
                .remove_owners(context.registry(), &owner, true)
                .await?;
            nexus.info_span(|| tracing::info!("Successfully disowned orphaned nexus"));
        }
    }

    PollResult::Ok(PollerState::Idle)
}

/// Given a control plane managed nexus
/// When a nexus is not owned by a volume
/// Then it should eventually be destroyed
#[tracing::instrument(level = "debug", skip(nexus, context), fields(nexus.uuid = %nexus.uuid(), request.reconcile = true))]
async fn destroy_disowned_nexus(
    nexus: &mut OperationGuardArc<NexusSpec>,
    context: &PollContext,
) -> PollResult {
    let not_owned = {
        let nexus_spec = nexus.as_ref();
        nexus_spec.managed && !nexus_spec.owned()
    };
    if not_owned {
        let span = tracing::info_span!("destroy_disowned_nexus", nexus.uuid = %nexus.uuid(), request.reconcile = true);
        destroy_nexus(nexus, context).instrument(span).await?;
    }

    PollResult::Ok(PollerState::Idle)
}

/// Given a control plane nexus
/// When a nexus destruction fails
/// Then it should eventually be destroyed
#[tracing::instrument(level = "debug", skip(nexus, context), fields(nexus.uuid = %nexus.uuid(), request.reconcile = true))]
async fn destroy_deleting_nexus(
    nexus: &mut OperationGuardArc<NexusSpec>,
    context: &PollContext,
) -> PollResult {
    let deleting = nexus.as_ref().status().deleting();
    if deleting {
        let span = tracing::info_span!("destroy_deleting_nexus", nexus.uuid = %nexus.uuid(), request.reconcile = true);
        destroy_nexus(nexus, context).instrument(span).await?;
    }

    PollResult::Ok(PollerState::Idle)
}

#[tracing::instrument(level = "trace", skip(nexus, context), fields(nexus.uuid = %nexus.uuid(), request.reconcile = true))]
async fn destroy_nexus(
    nexus: &mut OperationGuardArc<NexusSpec>,
    context: &PollContext,
) -> PollResult {
    let node = nexus.as_ref().node.clone();
    let node_online = matches!(context.registry().node_wrapper(&node).await, Ok(node) if node.read().await.is_online());
    if node_online {
        nexus.warn_span(|| tracing::warn!("Attempting to destroy nexus"));
        let request = DestroyNexus::from(nexus.as_ref());
        match nexus
            .destroy(context.registry(), &request.with_disown_all())
            .await
        {
            Ok(_) => {
                nexus.info_span(|| tracing::info!("Successfully destroyed nexus"));
                Ok(PollerState::Idle)
            }
            Err(error) => {
                nexus.error_span(|| tracing::error!(%error, "Failed to destroy nexus"));
                Err(error)
            }
        }
    } else {
        Ok(PollerState::Idle)
    }
}
