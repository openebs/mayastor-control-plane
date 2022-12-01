use crossbeam_queue::SegQueue;
use futures_util::{future::ready, stream::StreamExt};
use nvmeadm::nvmf_subsystem::{NvmeSubsystems, Subsystem};
use std::{
    collections::HashMap,
    convert::TryInto,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio_udev::{AsyncMonitorSocket, EventType, MonitorBuilder};
use utils::NVME_TARGET_NQN_PREFIX;

const SYSFS_PREFIX: &str = "/sys/devices/virtual/nvme-fabrics/ctl/";
/// Maximum number of events to process upon cache update (to prevent event flooding).
const CACHE_EVENTS_BATCH_SIZE: usize = 128;

/// Object that represents a path to sysfs NVMe object.
#[derive(Debug, Clone)]
pub struct NvmePath {
    path_buffer: PathBuf,
    nqn: String,
}

impl NvmePath {
    fn new(path_buffer: PathBuf, nqn: String) -> Self {
        Self { path_buffer, nqn }
    }

    /// Get the NVMe path reference.
    #[inline]
    pub fn path(&self) -> &Path {
        self.path_buffer.as_path()
    }

    /// Get the NVMe nqn reference.
    #[inline]
    pub fn nqn(&self) -> &String {
        &self.nqn
    }
}

/// Check that NVMe path represents a target created by our product and
/// obtain a Path object that represents a system path for this NVMe device.
pub fn get_nvme_path_entry(path: &String) -> Option<NvmePath> {
    Path::new(path).canonicalize().ok().and_then(|pb| {
        Subsystem::new(pb.as_path()).ok().and_then(|s| {
            // Check NQN of the subsystem to make sure it belongs to the product.
            if s.nqn.starts_with(NVME_TARGET_NQN_PREFIX) {
                Some(NvmePath::new(pb, s.nqn))
            } else {
                None
            }
        })
    })
}

/// UDEV-driven cache synchronization messages.
#[derive(Debug, Clone)]
enum CacheOp {
    Add(String),
    Remove(String),
    InvalidateCache,
}

/// UDEV-based NVMe path provider that uses cache to keep all known NVMe targets
/// in memory and avoid wildcard Sysfs enumeration when there is a need to get
/// a list of all existing NVMe path in the system.
/// This name provider consists of 2 parts: backend and frontend.
/// Backend: (CachedNvmePathProvider itself) is responsible for monitoring UDEV events
/// and signalling cache modification in case there are suitable UDEV events related
/// to NVMe subsystem.
/// Frontend: (NvmePathNameCollection), which represents a cache for all known NVMe
/// paths in the system. Note: we only cache paths that represent NVMe targets with product's
/// NQN, so cache acts as a filter for 'unknown' NQNs.
/// In order to eliminate cache inconsistency, there must be only one front-end path collection.
/// To eliminate locking/contention, backend communicates with frontend via a lockless queue.
#[derive(Debug)]
pub struct CachedNvmePathProvider {
    udev_queue: Arc<SegQueue<CacheOp>>,
    frontend: Option<NvmePathNameCollection>,
}

impl CachedNvmePathProvider {
    /// Start UDEV monitoring loop.
    pub fn start(&mut self) -> anyhow::Result<impl std::future::Future<Output = ()> + '_> {
        let builder = MonitorBuilder::new()?.match_subsystem("nvme")?;

        let monitor: AsyncMonitorSocket = builder.listen()?.try_into()?;

        tracing::info!("Starting UDEV event monitor for NVMe subsystem");

        // Once UDEV event monitor is successfully created, we will be notified
        // about all changes in device tree, hence from this point the cache will
        // always be in sync, so it's time to trigger the cache invalidation
        // for the front-end.
        self.udev_queue.push(CacheOp::InvalidateCache);

        let f = monitor.for_each(move |event| {
            if let Ok(event) = event {
                tracing::debug!(
                    "Received UDEV hotplug event: {}: {} = {}",
                    event.event_type(),
                    event.device().syspath().display(),
                    event
                        .subsystem()
                        .map_or("", |s| { s.to_str().unwrap_or("") })
                );

                // Make sure we have a valid UNICODE path.
                match event.device().syspath().to_str() {
                    Some(path) => {
                        // Handle UDEV path addition/removal events.
                        let e = match event.event_type() {
                            EventType::Add => {
                                tracing::debug!(path, "New NVMe path added");
                                Some(CacheOp::Add(path.to_owned()))
                            }
                            EventType::Remove => {
                                tracing::debug!(path, "NVMe path removed");
                                Some(CacheOp::Remove(path.to_owned()))
                            }
                            _ => None,
                        };

                        if let Some(m) = e {
                            self.udev_queue.push(m);
                        }
                    }
                    None => {
                        tracing::error!(
                            path = %event.device().syspath().display(),
                            "Device path is not a valid UNICODE",
                        );
                    }
                }
            }
            ready(())
        });
        Ok(f)
    }

    /// Get a front-end path collection for this name provider.
    pub fn get_path_collection(&mut self) -> Option<NvmePathNameCollection> {
        self.frontend.take()
    }

    /// Create a new cached name provider.
    pub fn new() -> Self {
        let q = Arc::new(SegQueue::new());

        Self {
            frontend: Some(NvmePathNameCollection::new(Arc::clone(&q))),
            udev_queue: q,
        }
    }
}

/// Front-end part of the cached name provider.
#[derive(Debug)]
pub struct NvmePathNameCollection {
    udev_queue: Arc<SegQueue<CacheOp>>,
    entries: HashMap<String, NvmePath>,
}

impl NvmePathNameCollection {
    fn new(udev_queue: Arc<SegQueue<CacheOp>>) -> Self {
        Self {
            udev_queue,
            entries: HashMap::new(),
        }
    }

    fn add_cache_entry(&mut self, path: String) {
        if let Some(pb) = get_nvme_path_entry(&path) {
            tracing::info!(path, nqn = pb.nqn, "Adding new NVMe path entry to cache");
            self.entries.insert(path, pb);
        }
    }

    /// Fully rebuild the NVMe path cache.
    fn invalidate_cache(&mut self) {
        self.entries.clear();

        let subsystems = NvmeSubsystems::new().expect("Failed to initialise NVMe subsystems");

        for e in subsystems {
            match e {
                Ok(s) => self.add_cache_entry(format!("{SYSFS_PREFIX}{}", s.name)),
                Err(error) => tracing::error!(?error, "Failed to read NVMe subsystem"),
            };
        }

        tracing::info!(
            "NVMe path cache invalidated, records added: {}",
            self.entries.len()
        );
    }

    /// Update cache based on UDEV events detected by the path provider.
    fn update_cache(&mut self) {
        let mut to_process = CACHE_EVENTS_BATCH_SIZE;

        while to_process > 0 && !self.udev_queue.is_empty() {
            if let Some(e) = self.udev_queue.pop() {
                match e {
                    CacheOp::Add(p) => {
                        self.add_cache_entry(p);
                    }
                    CacheOp::Remove(path) => match self.entries.remove(&path) {
                        Some(_) => tracing::info!(path, "NVMe path removed from the cache"),
                        None => tracing::warn!(path, "NVMe path not in cache, skipping removal"),
                    },
                    CacheOp::InvalidateCache => {
                        self.invalidate_cache();
                    }
                }
            } else {
                break;
            }
            to_process -= 1;
        }
    }

    /// Get all cached path entries.
    pub fn get_entries(&mut self) -> impl Iterator<Item = &NvmePath> {
        // Check if we have any pending UDEV events and synchronize the cache.
        self.update_cache();
        self.entries.values()
    }
}
