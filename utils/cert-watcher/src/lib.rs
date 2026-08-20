//! Filesystem watching for file-backed TLS certificate reload.
//!
//! Consolidates the watch machinery shared by every TLS hot-reload site: watch-target
//! selection, modification-time fingerprinting, and a background inotify loop with a
//! polling fallback. Consumers provide a reload callback which owns the actual reload
//! logic (fingerprint guard, rebuilding connectors/acceptors, logging).

use std::{io, path::PathBuf, time::SystemTime};

/// Filesystem paths to watch for certificate changes.
///
/// If all paths share a parent directory and the process runs inside Kubernetes, watch
/// that directory instead. This catches atomic symlink-swap rotations (as used by
/// Kubernetes secret mounts) where individual file symlinks may not emit events.
pub fn watch_targets(paths: Vec<PathBuf>) -> Vec<PathBuf> {
    if paths.is_empty() || std::env::var("KUBERNETES_SERVICE_HOST").is_err() {
        return paths;
    }
    let mut parents = paths.iter().map(|path| path.parent().map(PathBuf::from));
    if let Some(Some(first)) = parents.next() {
        if parents.all(|parent| parent.as_ref() == Some(&first)) {
            return vec![first];
        }
    }
    paths
}

/// Last-modified fingerprint of the given files, used to skip redundant reloads.
pub fn fingerprint(paths: &[PathBuf]) -> io::Result<Vec<SystemTime>> {
    paths
        .iter()
        .map(|path| std::fs::metadata(path)?.modified())
        .collect()
}

/// Spawn a background OS thread that watches `watch_paths` and invokes `reload` on changes.
///
/// The callback is invoked whenever the watched paths may have changed: right after the
/// watches are (re-)armed (to catch up on changes that predate the watch), and after every
/// batch of filesystem events. Spurious invocations are expected; callers are responsible
/// for guarding actual reload work with a [`fingerprint`] comparison.
///
/// A plain OS thread is used rather than an async task because the watch blocks in
/// `inotify` reads and the reload path is fully synchronous/blocking.
///
/// On non-Linux platforms this logs a warning and does nothing.
#[cfg(target_os = "linux")]
pub fn spawn(thread_name: &str, watch_paths: Vec<PathBuf>, reload: impl FnMut() + Send + 'static) {
    if watch_paths.is_empty() {
        return;
    }
    std::thread::Builder::new()
        .name(thread_name.into())
        .spawn(move || watch(watch_paths, reload))
        .ok();
}

/// Spawn a background OS thread that watches `watch_paths` and invokes `reload` on changes.
///
/// On non-Linux platforms this logs a warning and does nothing.
#[cfg(not(target_os = "linux"))]
pub fn spawn(
    _thread_name: &str,
    watch_paths: Vec<PathBuf>,
    _reload: impl FnMut() + Send + 'static,
) {
    if watch_paths.is_empty() {
        return;
    }
    tracing::warn!(
        "TLS certificate watch is only supported on Linux; automatic reload is disabled"
    );
}

/// Watch certificate paths and invoke the reload callback after changes.
///
/// Watch targets normally come from [`watch_targets`]: either the shared parent directory
/// of the certificate files, or the individual files as a fallback. A single inotify
/// instance reacts to `MODIFY` (in-place writes) and `MOVED_TO` (an entry moved into the
/// watched directory, i.e. an atomic-rename rotation).
///
/// A renewal usually touches several files at once; each blocking read drains the whole
/// batch of queued events, which collapses into a single reload, and the caller's
/// fingerprint guard ensures reload work happens only once per actual change. Watches are
/// re-armed only if the watched inode is removed (`IGNORED`); the common Kubernetes
/// symlink swap keeps the directory inode.
///
/// inotify can fail at runtime: `init` when `fs.inotify.max_user_instances` is exhausted,
/// and `add` when a path is briefly missing mid-rotation or `fs.inotify.max_user_watches`
/// is exhausted. Whenever a watch cannot be (re)armed, this falls back to invoking the
/// reload callback on a timer until it recovers.
#[cfg(target_os = "linux")]
fn watch(watch_paths: Vec<PathBuf>, mut reload: impl FnMut()) {
    use inotify::{EventMask, Inotify, WatchMask};
    use std::time::Duration;

    /// The polling interval used whenever the inotify watch is unavailable.
    const POLL_INTERVAL: Duration = Duration::from_secs(60);

    tracing::debug!(?watch_paths, "TLS watch requested");

    // Keep one inotify instance for the watcher's lifetime: the kernel only drops the
    // watch descriptors when their inodes are deleted, the instance stays usable.
    let mut inotify = loop {
        match Inotify::init() {
            Ok(inotify) => break inotify,
            Err(error) => {
                tracing::warn!(
                    %error,
                    "Failed to initialise inotify for TLS watch; polling for changes"
                );
                std::thread::sleep(POLL_INTERVAL);
                reload();
            }
        }
    };

    let mask = WatchMask::MODIFY | WatchMask::MOVED_TO;
    let mut buffer = [0u8; 4096];
    'outer: loop {
        for path in &watch_paths {
            if let Err(error) = inotify.watches().add(path, mask) {
                tracing::warn!(
                    %error,
                    path = %path.display(),
                    "Failed to watch TLS path; will retry"
                );
                // Fall back to polling until the full set can be re-armed.
                std::thread::sleep(POLL_INTERVAL);
                reload();
                continue 'outer;
            }
        }

        // Catch up on any change that happened before the watches were (re-)armed, since
        // such changes emit no events; the caller's fingerprint guard makes this a no-op
        // otherwise.
        reload();

        loop {
            // Blocks until an event is available, then returns the whole batch of
            // currently-queued events in one read.
            let events = match inotify.read_events_blocking(&mut buffer) {
                Ok(events) => events,
                Err(error) => {
                    tracing::warn!(%error, "TLS watch read error; re-arming");
                    break;
                }
            };

            tracing::debug!(?watch_paths, "TLS watch events received");

            // The watch descriptor is only dropped when the watched inode itself is
            // removed (delivered as `IGNORED`); in-directory rotations keep the inode
            // and the watch keeps reporting `MOVED_TO`/`MODIFY`.
            let ignored = events
                .into_iter()
                .any(|event| event.mask.contains(EventMask::IGNORED));

            reload();

            if ignored {
                break;
            }
        }
        // Brief settle time before re-arming.
        std::thread::sleep(Duration::from_millis(100));
    }
}
