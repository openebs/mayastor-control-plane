use crate::core::{
    reconciler::{nexus, persistent_store::PersistentStoreReconciler, pool, volume},
    registry::Registry,
    task_poller::{squash_results, PollContext, PollEvent, PollResult, PollerState, TaskPoller},
};

/// Reconciliation worker that polls all reconciliation loops
/// The loops are polled one at a time to avoid any potential contention
/// and also hopefully making the logging clearer
pub(super) struct ReconcilerWorker {
    poll_targets: Vec<Box<dyn TaskPoller>>,
    event_channel: tokio::sync::mpsc::Receiver<PollEvent>,
    shutdown_channel: tokio::sync::mpsc::Receiver<()>,
    event_channel_sender: Option<tokio::sync::mpsc::Sender<PollEvent>>,
    shutdown_channel_sender: Option<tokio::sync::mpsc::Sender<()>>,
}

impl std::fmt::Debug for ReconcilerWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reconciler").finish()
    }
}
impl ReconcilerWorker {
    /// Create a new `Self` with the provided communication channels
    pub(super) fn new() -> Self {
        let poll_targets: Vec<Box<dyn TaskPoller>> = vec![
            Box::new(pool::PoolReconciler::new()),
            Box::new(nexus::NexusReconciler::new()),
            Box::new(volume::VolumeReconciler::new()),
            Box::new(PersistentStoreReconciler::new()),
        ];

        let event_channel = tokio::sync::mpsc::channel(poll_targets.len());
        let shutdown_channel = tokio::sync::mpsc::channel(1);
        Self {
            poll_targets,
            event_channel: event_channel.1,
            shutdown_channel: shutdown_channel.1,
            event_channel_sender: Some(event_channel.0),
            shutdown_channel_sender: Some(shutdown_channel.0),
        }
    }
    /// Take the shutdown channel sender (can only be called once)
    pub(super) fn take_shutdown_channel(&mut self) -> tokio::sync::mpsc::Sender<()> {
        self.shutdown_channel_sender
            .take()
            .expect("initialised shutdown sender")
    }
    /// Take the event channel sender (can only be called once)
    pub(super) fn take_event_channel(&mut self) -> tokio::sync::mpsc::Sender<PollEvent> {
        self.event_channel_sender
            .take()
            .expect("initialised event sender")
    }
}

impl ReconcilerWorker {
    /// Start polling the registered reconciliation loops
    /// The polling will continue until we receive the shutdown signal
    pub(super) async fn poller(mut self, registry: Registry) {
        // kick-off the first run
        let mut event = PollEvent::TimedRun;
        loop {
            let result = match &event {
                PollEvent::Shutdown => {
                    tracing::warn!("Shutting down... (reconcilers will NOT be polled again)");
                    return;
                }
                PollEvent::TimedRun | PollEvent::Triggered(_) => {
                    self.poller_work(PollContext::from(&event, &registry)).await
                }
            };

            event = tokio::select! {
                _shutdown = self.shutdown_channel.recv() => {
                    PollEvent::Shutdown
                },
                event = self.event_channel.recv() => {
                    event.unwrap_or(PollEvent::Shutdown)
                },
                _timed = tokio::time::sleep(if result.unwrap_or(PollerState::Busy) == PollerState::Busy {
                    registry.reconcile_period()
                } else {
                    registry.reconcile_idle_period()
                }) => {
                    PollEvent::TimedRun
                }
            };
        }
    }

    #[tracing::instrument(skip(context), level = "trace")]
    async fn poller_work(&mut self, context: PollContext) -> PollResult {
        tracing::trace!("Entering the reconcile loop...");
        let mut results = vec![];
        for target in &mut self.poll_targets {
            results.push(target.try_poll(&context).await);
        }
        tracing::trace!("Leaving the reconcile loop...");
        squash_results(results)
    }
}
