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

#[derive(Debug, Clone)]
pub struct NvmePath {
    path_buffer: PathBuf,
}

/// Object that represents a path to sysfs NVMe object.
impl NvmePath {
    fn new(path_buffer: PathBuf) -> Self {
        Self { path_buffer }
    }

    #[inline]
    pub fn path(&self) -> &Path {
        self.path_buffer.as_path()
    }
}

/// Check that NVMe path represents a target created by our product and
/// obtain a Path object that represents a system path for this NVMe device.
fn get_nvme_path_buf(path: &String) -> Option<PathBuf> {
    Path::new(path).canonicalize().ok().and_then(|pb| {
        Subsystem::new(pb.as_path()).ok().and_then(|s| {
            // Check NQN of the subsystem to make sure it belongs to the product.
            if s.nqn.starts_with(NVME_TARGET_NQN_PREFIX) {
                Some(pb)
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
/// This name provider sonsists of 2 parts: backend and frontend.
/// Backend: (CachedNvmePathProvider itself) is responsible for monitoring UDEV events
/// and signalling cache modification in case there are suitable UDEV events related
/// to NVMe subsystem.
/// Frontend: (NvmePathNameCollection), which represents a cache for all known NVMe
/// paths in the system. Note: we only cache paths that represent NVMe targets with product's
/// In order to eliminate cache inconsistency, there must be only one front-end path collection.
/// NQN, so cache acts as a filter for 'unknown' NQNs.
/// To eliminate locking/contention, backend communicates with frontend via a lockless queue.
#[derive(Debug)]
pub struct CachedNvmePathProvider {
    udev_queue: Arc<SegQueue<CacheOp>>,
    frontend: Option<NvmePathNameCollection>,
}

impl CachedNvmePathProvider {
    /// Start UDEV monitoring loop.
    pub async fn start(&mut self) {
        let builder = MonitorBuilder::new()
            .expect("Couldn't create builder")
            .match_subsystem("nvme")
            .expect("Failed to add filter for USB devices");

        let monitor: AsyncMonitorSocket = builder
            .listen()
            .expect("Couldn't create MonitorSocket")
            .try_into()
            .expect("Couldn't create AsyncMonitorSocket");

        info!("Starting UDEV event monitor for NVMe subsystem");

        // Once UDEV event monitor is successfully created, we will be notified
        // about all changes in device tree, hence from this point the cache will
        // always be in sync, so it's time to trigger the cache invalidation
        // for the front-end.
        self.udev_queue.push(CacheOp::InvalidateCache);

        monitor
            .for_each(|event| {
                if let Ok(event) = event {
                    debug!(
                        "Received UDEV hotplug event: {}: {} = {}",
                        event.event_type(),
                        event.device().syspath().display(),
                        event
                            .subsystem()
                            .map_or("", |s| { s.to_str().unwrap_or("") })
                    );

                    // Make sure we have a valid UNICODE path.
                    match event.device().syspath().to_str() {
                        Some(p) => {
                            // Handle UDEV path addition/removal events.
                            let e = match event.event_type() {
                                EventType::Add => {
                                    info!("New NVMe path added: {}", p);
                                    Some(CacheOp::Add(p.to_owned()))
                                }
                                EventType::Remove => {
                                    info!("NVMe path removed: {}", p);
                                    Some(CacheOp::Remove(p.to_owned()))
                                }
                                _ => None,
                            };

                            if let Some(m) = e {
                                self.udev_queue.push(m);
                            }
                        }
                        None => {
                            error!(
                                "Device path is not a valid UNICODE path: {}",
                                event.device().syspath().display()
                            );
                        }
                    }
                }
                ready(())
            })
            .await;
    }

    /// Get a front-end path collection for this name provider.
    pub fn get_path_collection(&mut self) -> Option<NvmePathNameCollection> {
        self.frontend.take()
    }

    /// Create a new cached name provider.
    pub async fn new() -> anyhow::Result<Self> {
        let q = Arc::new(SegQueue::new());

        Ok(Self {
            frontend: Some(NvmePathNameCollection::new(Arc::clone(&q))),
            udev_queue: q,
        })
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
        if let Some(pb) = get_nvme_path_buf(&path) {
            debug!("Adding new NVMe path to cache: {}", path);
            self.entries.insert(path, NvmePath::new(pb));
        }
    }

    /// Fully rebuild the NVMe path cache.
    fn invalidate_cache(&mut self) {
        self.entries.clear();

        let subsystems = NvmeSubsystems::new().expect("Failed to intialize NVMe subsystems");

        for e in subsystems {
            match e {
                Ok(s) => self.add_cache_entry(format!("{SYSFS_PREFIX}{}", s.name)),
                Err(e) => error!("Failed to read NVMe subsystem: {:?}", e),
            };
        }

        info!(
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
                    CacheOp::Remove(p) => match self.entries.remove(&p) {
                        Some(_) => info!("NVMe path removed from the cache: {}", p),
                        None => warn!("NVMe path not in cache, skipping removal: {}", p),
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
