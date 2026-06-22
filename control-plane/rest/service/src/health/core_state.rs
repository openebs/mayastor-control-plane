use crate::v0::core_grpc;
use grpc::operations::node::traits::NodeOperations;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// This is a type to cache the liveness of the agent-core service.
/// This is meant to be wrapped inside an Arc and used across threads.
pub struct CachedCoreState {
    state: Mutex<ServerState>,
    cache_duration: Duration,
}

/// This type remembers a liveness state, and when this data was refreshed.
struct ServerState {
    is_live: bool,
    /// Initially `None`, causing every readiness check to make the actual gRPC
    /// call. Set to `Some` on the first successful probe, and updated on every
    /// probe thereafter so that cache expiry works correctly for failures too.
    last_updated: Option<Instant>,
}

impl ServerState {
    /// Update the state of the agent-core service, or assume it's unavailable if something
    /// went wrong.
    async fn update_or_assume_unavailable(&mut self) {
        let new_value = core_grpc().node().probe(None).await.unwrap_or(false);
        self.is_live = new_value;
        if new_value || self.last_updated.is_some() {
            self.last_updated = Some(Instant::now());
        }
    }
}

impl CachedCoreState {
    /// Create a new cache for serving readiness health checks based on agent-core health.
    pub fn new(cache_duration: Duration) -> Self {
        CachedCoreState {
            state: Mutex::new(ServerState {
                is_live: false,
                last_updated: None,
            }),
            cache_duration,
        }
    }

    /// Get the cached state of the agent-core service, or assume it's unavailable if something
    /// went wrong.
    pub async fn get_or_assume_unavailable(&self) -> bool {
        let mut state = self.state.lock().await;

        let cache_expired = state
            .last_updated
            .is_none_or(|t| t.elapsed() >= self.cache_duration);

        if cache_expired {
            state.update_or_assume_unavailable().await;
        }

        state.is_live
    }
}
