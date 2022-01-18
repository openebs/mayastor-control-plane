use crate::core::{
    reconciler::{PollContext, TaskPoller},
    specs::{OperationSequenceGuard, SpecOperations},
    task_poller::{PollEvent, PollResult, PollTimer, PollerState},
};
use common_lib::types::v0::{
    message_bus::DestroyNexus,
    store::{nexus::NexusSpec, OperationMode, TraceSpan},
};

use crate::core::task_poller::PollTriggerEvent;
use parking_lot::Mutex;
use std::sync::Arc;

/// Nexus Garbage Collector reconciler
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
        let nexuses = context.specs().get_nexuses();
        for nexus in nexuses {
            let _ = nexus_garbage_collector(&nexus, context).await;
        }
        PollResult::Ok(PollerState::Idle)
    }

    async fn poll_timer(&mut self, _context: &PollContext) -> bool {
        self.counter.poll()
    }

    async fn poll_event(&mut self, context: &PollContext) -> bool {
        match context.event() {
            PollEvent::TimedRun | PollEvent::Triggered(PollTriggerEvent::Start) => true,
            PollEvent::Shutdown | PollEvent::Triggered(_) => false,
        }
    }
}

async fn nexus_garbage_collector(
    nexus_spec: &Arc<Mutex<NexusSpec>>,
    context: &PollContext,
) -> PollResult {
    let mode = OperationMode::ReconcileStep;
    let results = vec![
        destroy_orphaned_nexus(nexus_spec, context).await,
        destroy_disowned_nexus(nexus_spec, context, mode).await,
    ];
    GarbageCollector::squash_results(results)
}

/// Given a control plane managed nexus
/// When a nexus is owned by a volume which no longer exists
/// Then the nexus should be disowned
/// And it should eventually be destroyed
#[tracing::instrument(level = "debug", skip(nexus_spec, context), fields(nexus.uuid = %nexus_spec.lock().uuid, request.reconcile = true))]
async fn destroy_orphaned_nexus(
    nexus_spec: &Arc<Mutex<NexusSpec>>,
    context: &PollContext,
) -> PollResult {
    let _guard = match nexus_spec.operation_guard(OperationMode::ReconcileStart) {
        Ok(guard) => guard,
        Err(_) => return PollResult::Ok(PollerState::Busy),
    };
    let nexus_clone = nexus_spec.lock().clone();

    if !nexus_clone.managed {
        return PollResult::Ok(PollerState::Idle);
    }
    if let Some(owner) = &nexus_clone.owner {
        if context.specs().get_volume(owner).is_err() {
            nexus_clone.warn_span(|| tracing::warn!("Attempting to disown orphaned nexus"));
            context
                .specs()
                .disown_nexus(context.registry(), nexus_spec)
                .await?;
            nexus_clone.info_span(|| tracing::info!("Successfully disowned orphaned nexus"));
        }
    }

    PollResult::Ok(PollerState::Idle)
}

/// Given a control plane managed nexus
/// When a nexus is not owned by a volume
/// Then it should eventually be destroyed
#[tracing::instrument(level = "debug", skip(nexus_spec, context, mode), fields(nexus.uuid = %nexus_spec.lock().uuid, request.reconcile = true))]
async fn destroy_disowned_nexus(
    nexus_spec: &Arc<Mutex<NexusSpec>>,
    context: &PollContext,
    mode: OperationMode,
) -> PollResult {
    let _guard = match nexus_spec.operation_guard(OperationMode::ReconcileStart) {
        Ok(guard) => guard,
        Err(_) => return PollResult::Ok(PollerState::Busy),
    };

    let nexus_clone = nexus_spec.lock().clone();
    if nexus_clone.managed && !nexus_clone.owned() {
        let node_online = matches!(context.registry().get_node_wrapper(&nexus_clone.node).await, Ok(node) if node.read().await.is_online());
        if node_online {
            nexus_clone.warn_span(|| tracing::warn!("Attempting to destroy disowned nexus"));
            let request = DestroyNexus::from(nexus_clone.clone());
            context
                .specs()
                .destroy_nexus(context.registry(), &request, true, mode)
                .await?;
            nexus_clone.info_span(|| tracing::info!("Successfully destroyed disowned nexus"));
        }
    }

    PollResult::Ok(PollerState::Idle)
}
