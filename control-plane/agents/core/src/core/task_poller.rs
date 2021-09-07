use crate::core::registry::Registry;
use common::errors::SvcError;

/// Poll Event that identifies why a poll is running
#[derive(Debug, Clone)]
pub(crate) enum PollEvent {
    /// Poller period elapsed
    TimedRun,
    /// Request Triggered by another component
    /// example: A node has come back online so it could be a good idea to run the
    /// reconciliation loop's as soon as possible
    Triggered(PollTriggerEvent),
    /// Shutdown the pollers
    Shutdown,
}

/// Poll Trigger source
#[derive(Debug, Clone)]
pub(crate) enum PollTriggerEvent {
    /// A node state has changed to Online
    NodeStateChangeOnline,
}

/// State of a poller
#[derive(Eq, PartialEq)]
pub(crate) enum PollerState {
    /// No immediate work remains to be done
    Idle,
    /// There is still work outstanding
    Busy,
}
/// Result of a poll with the poller state
pub(crate) type PollResult = Result<PollerState, SvcError>;

/// The period at which a reconciliation loop is polled
/// It's defined as a number of ticks
/// Each tick has the period of the base poll period
pub(crate) type PollPeriods = u32;

/// PollTimer polls a timer and returns true to indicate that the reconciler should be polled
/// If true, the timer is reloaded back to the setup period
#[derive(Debug)]
pub(crate) struct PollTimer {
    period: PollPeriods,
    timer: PollPeriods,
}

impl PollTimer {
    /// Create a new `Self` for the given period
    pub(crate) fn from(period: PollPeriods) -> Self {
        Self {
            period,
            timer: period,
        }
    }
    /// True when the timer reaches the bottom indicating the reconciler should be polled
    /// (the counter is auto-reloaded)
    pub(crate) fn poll(&mut self) -> bool {
        if self.ready() {
            self.reload();
            true
        } else {
            self.decrement();
            false
        }
    }
    fn ready(&self) -> bool {
        self.timer <= 1
    }
    fn decrement(&mut self) {
        self.timer -= 1;
    }
    fn reload(&mut self) {
        self.timer = self.period;
    }
}

/// Poll Context passed around the poll handlers
pub(crate) struct PollContext {
    /// Event that triggered this poll
    event: PollEvent,
    /// Core Registry
    registry: Registry,
}
impl PollContext {
    /// Create a context for a `PollEvent` with the global `Registry`
    pub(crate) fn from(event: &PollEvent, registry: &Registry) -> Self {
        Self {
            event: event.clone(),
            registry: registry.clone(),
        }
    }
    /// Get a reference to the core registry
    pub(crate) fn registry(&self) -> &Registry {
        &self.registry
    }

    #[allow(dead_code)]
    /// Get a reference to the event that triggered this poll
    pub(crate) fn event(&self) -> &PollEvent {
        &self.event
    }
}

/// Trait used by all reconciliation loops
#[async_trait::async_trait]
pub(crate) trait TaskPoller: Send + Sync + std::fmt::Debug {
    /// Attempts to poll this poller, which will poll itself depending on the `PollEvent`
    #[tracing::instrument(skip(context), level = "trace", err)]
    async fn try_poll(&mut self, context: &PollContext) -> PollResult {
        tracing::trace!("Entering trace call");
        let result = if self.poll_ready(context).await {
            self.poll(context).await
        } else {
            PollResult::Ok(PollerState::Idle)
        };
        tracing::trace!("Leaving trace call");
        result
    }

    /// Force poll the poller
    async fn poll(&mut self, context: &PollContext) -> PollResult;

    /// Polls the ready state and returns true if ready
    async fn poll_ready(&mut self, context: &PollContext) -> bool {
        match context.event() {
            PollEvent::TimedRun => self.poll_timer(context).await,
            _ => self.poll_event(context).await,
        }
    }

    /// Polls the `PollTimer` and returns true if ready
    async fn poll_timer(&mut self, _context: &PollContext) -> bool {
        true
    }

    /// Determine if self should be polled for this `PollEvent`
    async fn poll_event(&mut self, context: &PollContext) -> bool {
        match context.event() {
            PollEvent::TimedRun => true,
            PollEvent::Triggered(_event) => true,
            PollEvent::Shutdown => true,
        }
    }

    /// Convert from a vector of results to a single result
    fn squash_results(results: Vec<PollResult>) -> PollResult
    where
        Self: Sized,
    {
        squash_results(results)
    }
}

/// Convert from a vector of results to a single result
pub(crate) fn squash_results(results: Vec<PollResult>) -> PollResult {
    let mut results = results.into_iter();
    match results.find(|r| r.is_err()) {
        Some(error) => error,
        None => {
            match results.find(|r| r.as_ref().unwrap_or(&PollerState::Busy) == &PollerState::Busy) {
                Some(busy) => busy,
                None => PollResult::Ok(PollerState::Idle),
            }
        }
    }
}
