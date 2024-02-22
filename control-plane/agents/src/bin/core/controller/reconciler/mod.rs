pub(crate) mod nexus;
mod node;
mod persistent_store;
pub(crate) mod poller;
mod pool;
mod replica;
mod snapshot;
mod volume;

pub(crate) use crate::controller::task_poller::PollTriggerEvent;
use crate::controller::task_poller::{
    squash_results, PollContext, PollEvent, PollResult, PollerState, TaskPoller,
};
use poller::ReconcilerWorker;
use std::fmt::Debug;

use crate::controller::registry::Registry;

use parking_lot::Mutex;

/// Used to start and stop the reconcile pollers
#[derive(Debug)]
pub(crate) struct ReconcilerControl {
    worker: Mutex<Option<ReconcilerWorker>>,
    event_channel: tokio::sync::mpsc::Sender<PollEvent>,
    shutdown_channel: tokio::sync::mpsc::Sender<()>,
}

impl ReconcilerControl {
    /// Return a new `Self`
    pub(crate) fn new() -> Self {
        let mut worker = ReconcilerWorker::new();
        Self {
            event_channel: worker.take_event_channel(),
            shutdown_channel: worker.take_shutdown_channel(),
            worker: Mutex::new(Some(worker)),
        }
    }

    /// Starts the polling of the registered reconciliation loops
    pub(crate) async fn start(&self, registry: Registry) {
        let worker = self.worker.lock().take().expect("Can only start once");
        tokio::spawn(async move {
            tracing::info!("Starting the reconciler control loop");
            worker.poller(registry).await;
        });
    }

    /// Send the shutdown signal to the poller's main loop
    /// (does not wait for the pollers to stop)
    #[allow(dead_code)]
    pub(crate) async fn shutdown(&self) {
        self.shutdown_channel.send(()).await.ok();
    }

    /// Send an event signal to the poller's main loop
    /// todo: don't requeque duplicate events
    pub(crate) async fn notify(&self, event: PollEvent) {
        if let Err(error) = self.event_channel.try_send(event) {
            tracing::warn!(error=?error, "Failed to send event to reconcile worker");
        }
    }
}

#[async_trait::async_trait]
trait Reconciler {
    /// Run the reconcile logic for this resource.
    async fn reconcile(&mut self, context: &PollContext) -> PollResult;
}

#[async_trait::async_trait]
trait GarbageCollect {
    /// Run the `GarbageCollect` reconciler.
    /// The default implementation calls all garbage collection methods.
    async fn garbage_collect(&mut self, context: &PollContext) -> PollResult {
        squash_results(vec![
            self.disown_orphaned(context).await,
            self.disown_unused(context).await,
            self.destroy_deleting(context).await,
            self.destroy_orphaned(context).await,
            self.disown_invalid(context).await,
        ])
    }

    /// Destroy resources which are in the deleting phase.
    /// A resource goes into the deleting phase when we start to delete it and stay in this
    /// state until we successfully delete it.
    async fn destroy_deleting(&mut self, context: &PollContext) -> PollResult;

    /// Destroy resources which have been orphaned.
    /// A resource becomes orphaned when all its owners have disowned it and at that point
    /// it is no longer needed and may be destroyed.
    async fn destroy_orphaned(&mut self, context: &PollContext) -> PollResult;

    /// Disown resources which are no longer needed by their owners.
    async fn disown_unused(&mut self, context: &PollContext) -> PollResult;
    /// Disown resources whose owners are no longer in existence.
    /// This may happen as a result of a bug or manual edit of the persistent store (etcd).
    async fn disown_orphaned(&mut self, context: &PollContext) -> PollResult;
    /// Disown resources which have questionable existence, for example non reservable replicas.
    async fn disown_invalid(&mut self, context: &PollContext) -> PollResult;
    /// Reclaim unused capacity - for example an expanded but unused replica, which may
    /// happen as part of a failed volume expand operation.
    async fn reclaim_space(&mut self, _context: &PollContext) -> PollResult {
        PollResult::Ok(PollerState::Idle)
    }
}

#[async_trait::async_trait]
trait ReCreate {
    /// Recreate the state according to the specification.
    /// This is required when an io-engine instance crashes/restarts as it always starts with no
    /// state.
    /// This is because it's the control-plane's job to recreate the state since it has the
    /// overview of the whole system.
    async fn recreate_state(&mut self, context: &PollContext) -> PollResult;
}
