mod nexus;
mod persistent_store;
pub mod poller;
mod pool;
mod volume;

pub(crate) use crate::core::task_poller::PollTriggerEvent;
use crate::core::task_poller::{PollContext, PollEvent, TaskPoller};
use poller::ReconcilerWorker;

use crate::core::registry::Registry;
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
    pub(crate) async fn notify(&self, event: PollEvent) {
        self.event_channel.send(event).await.ok();
    }
}
