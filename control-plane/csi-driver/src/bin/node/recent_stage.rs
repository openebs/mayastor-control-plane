//! Tracks volumes that were successfully staged (`NodeStageVolume`, or the
//! `NodePublishVolume` re-stage fallback) very recently on this node.
//!
//! `force_unstage_volume` (see `nodeplugin_grpc.rs`) uses this to avoid
//! tearing down a subsystem/mount that was legitimately (re)established only
//! moments ago -- eg: a routine `ControllerPublishVolume` resync from the CSI
//! external-attacher racing a just-completed stage on the same node right
//! after the node comes back from a reboot. See GLCP-379583: the previous
//! unconditional `force_unstage` would disconnect a fresh, correct connection
//! seconds after it was established, with no way for kubelet to recover
//! short of deleting the pod.
//!
//! This is a coarse, node-local heuristic (a grace window), not a
//! correctness guarantee -- it does not know whether the current subsystem
//! actually matches what the control-plane now considers authoritative, only
//! that *something* staged this exact volume very recently. It deliberately
//! does not replace `force_unstage`'s real job of cleaning up genuinely
//! stale, long-lived connections (eg: left over from before a reboot, or
//! from a since-abandoned node), which will have no recent stage record and
//! so fall straight through to the existing cleanup path.
use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};

use once_cell::sync::OnceCell;
use uuid::Uuid;

/// How long a successful stage is remembered for, regardless of what grace
/// period callers ask for -- bounds the map's memory and avoids a stage from
/// hours ago ever being mistaken for "recent".
const MAX_RETENTION: Duration = Duration::from_secs(3600);

fn recent_stages() -> &'static Mutex<HashMap<Uuid, Instant>> {
    static RECENT_STAGES: OnceCell<Mutex<HashMap<Uuid, Instant>>> = OnceCell::new();
    RECENT_STAGES.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Record that `volume_id` was just successfully staged on this node.
pub(crate) fn mark_staged(volume_id: Uuid) {
    let mut map = recent_stages().lock().unwrap();
    let now = Instant::now();
    map.insert(volume_id, now);
    // Opportunistic prune, piggy-backing on the insert, so this doesn't grow
    // unbounded across the lifetime of the process.
    map.retain(|_, staged_at| now.duration_since(*staged_at) < MAX_RETENTION);
}

/// True if `volume_id` was marked staged within the last `within` duration.
pub(crate) fn recently_staged(volume_id: &Uuid, within: Duration) -> bool {
    let map = recent_stages().lock().unwrap();
    map.get(volume_id)
        .is_some_and(|staged_at| staged_at.elapsed() < within)
}
