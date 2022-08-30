use crate::{
    path_provider::{CachedNvmePathProvider, NvmePathNameCollection},
    reporter::PathReporter,
    Cli,
};
use nvmeadm::nvmf_subsystem::Subsystem;
use std::{collections::HashMap, convert::From, rc::Rc, sync::Arc};
use tokio::{
    sync::mpsc::{channel, Receiver, Sender},
    time::{sleep, Duration},
};

/// Possible states of every path record.
#[derive(Debug, Clone)]
enum PathState {
    Good,
    Suspected,
    Failed,
}

impl std::fmt::Display for PathState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{:?}", *self)
    }
}

/// Object that represents a broken/suspected NVMe path.
#[derive(Debug, Clone)]
struct PathRecord {
    /// Used to allow detection of outdated records.
    epoch: u64,
    nqn: String,
    state: PathState,
    path: String,
    reporter: Rc<PathReporter>,
}

impl PathRecord {
    fn new(nqn: String, epoch: u64, path: String, reporter: Rc<PathReporter>) -> Self {
        Self {
            nqn,
            epoch,
            path,
            state: PathState::Good,
            reporter,
        }
    }

    fn get_nqn(&self) -> &str {
        &self.nqn
    }

    #[inline]
    fn get_epoch(&self) -> u64 {
        self.epoch
    }

    #[inline]
    fn set_epoch(&mut self, epoch: u64) {
        self.epoch = epoch;
    }

    // Trigger state transition based on 'connecting' state of the underlying NVMe controller.
    fn report_connecting(&mut self) {
        match self.state {
            PathState::Good => {
                self.state = PathState::Suspected;
                tracing::info!(
                    state=%PathState::Suspected,
                    target=self.nqn,
                    "Target state transition"
                );
            }
            PathState::Suspected => {
                self.state = PathState::Failed;
                tracing::error!(
                    state=%PathState::Failed,
                    target=self.nqn,
                    "Target state transition",
                );
                self.reporter.report_failed_path(self.nqn.clone());
            }
            PathState::Failed => {} // Multiple failures don't cause any state transitions.
        }
    }

    fn report_live(&mut self) {
        self.state = PathState::Good;
    }
}

/// Front-end API client for NVMe cache.
/// Clients use this API client to query cached NVMe controllers.
#[derive(Clone)]
pub struct NvmePathCache {
    channel: Arc<Sender<NvmeCacheMessage>>,
}

impl NvmePathCache {
    fn new(channel: Arc<Sender<NvmeCacheMessage>>) -> Self {
        Self { channel }
    }
}

/// Cached NVMe controller object.
#[derive(Debug)]
pub(crate) struct NvmeController {
    pub _nqn: String,
    pub path: String,
}

impl NvmeController {
    /// Instantiate Nvme controller for given NQN and system path.
    fn new(nqn: String, path: String) -> Self {
        Self { _nqn: nqn, path }
    }
}

impl From<&PathRecord> for NvmeController {
    fn from(v: &PathRecord) -> Self {
        Self::new(v.nqn.clone(), v.path.clone())
    }
}

/// Command for querying NVMe path cache.
#[derive(Debug)]
enum NvmePathCacheCommand {
    LookupNvmeController(String),
}

/// Response objects for NVMe cache commands.
enum NvmePathCacheCommandResponse {
    /// Negative response, contains extended information about failure.
    Nack(String),
    /// Cached instance of an NVMe controller available in the system.
    CachedNvmeController(NvmeController),
}

/// Messsage sent to NVMe cache from a cache client: client passes a channel
/// to receive a response from the cache.
type NvmeCacheMessage = (NvmePathCacheCommand, Sender<NvmePathCacheCommandResponse>);

/// Path failure detector for NVMe paths.
/// All known NVMe paths on system are periodically checked for liveness and get classified
/// as:
///  Good  - path is fully functional.
///  Suspected - path experiences connectivity problems for the first time.
///  Failed - path has experienced connectivity problems two times in a row.
/// Once a path is classified as Failed, it's reported to PathReporter and gets sent to
/// HA Cluster agent.
#[derive(Debug)]
pub struct PathFailureDetector {
    epoch: u64,
    detection_period: Duration,
    suspected_paths: HashMap<String, PathRecord>,
    reporter: Rc<PathReporter>,
    cache_channel_rx: Receiver<NvmeCacheMessage>,
    cache_channel_tx: Arc<Sender<NvmeCacheMessage>>,
}

impl PathFailureDetector {
    pub(crate) fn new(args: &Cli) -> anyhow::Result<Self> {
        let reporter = PathReporter::new(
            args.node_name.clone(),
            *args.retransmission_period,
            *args.aggregation_period,
        );

        let (tx, rx) = channel(64);

        Ok(Self {
            epoch: 0,
            detection_period: *args.detection_period,
            suspected_paths: HashMap::new(),
            reporter: Rc::new(reporter),
            cache_channel_tx: Arc::new(tx),
            cache_channel_rx: rx,
        })
    }

    fn rescan_paths(&mut self, path_collection: &mut NvmePathNameCollection) {
        // Update epoch before scanning controllers.
        self.epoch += 1;

        // Scan all reported NVMe paths on system and check for connectivity.
        for ctrlr in path_collection.get_entries() {
            // Silently ignore errors when we can't get a subsystem for target path,
            // as we might see lots of false-positive errors when removing a failed path
            // from a multi-pathed NVMe subsystem.
            if let Ok(subsystem) = Subsystem::new(ctrlr.path()) {
                let existing_record = match subsystem.state.as_str() {
                    "connecting" => {
                        // Add a new record in case no record exists for target NQN.
                        if !self.suspected_paths.contains_key(&subsystem.nqn) {
                            let path = ctrlr.path().to_string_lossy().to_string();

                            self.suspected_paths.insert(
                                subsystem.nqn.clone(),
                                PathRecord::new(
                                    subsystem.nqn.clone(),
                                    self.epoch,
                                    path,
                                    self.reporter.clone(),
                                ),
                            );
                        }

                        let rec = self.suspected_paths.get_mut(&subsystem.nqn).unwrap();
                        rec.report_connecting();
                        Some(rec)
                    }
                    "live" => self.suspected_paths.get_mut(&subsystem.nqn).map(|rec| {
                        rec.report_live();
                        rec
                    }),
                    _ => None,
                };

                // Update epoch for the existing record.
                if let Some(rec) = existing_record {
                    rec.set_epoch(self.epoch);
                }
            }
        }

        // Remove all existing records that don't have underlying NVMe controllers:
        // can happen in case controller was removed after it had been identified as
        // broken/suspected. Stalled/outdated records have a different (old) epoch number
        // since they have not been touched during the current iteration.
        let mut to_remove = vec![];

        for v in self.suspected_paths.values() {
            if v.get_epoch() != self.epoch {
                to_remove.push(v.get_nqn().to_owned());
            }
        }

        if !to_remove.is_empty() {
            for nqn in to_remove {
                self.suspected_paths.remove(&nqn);
                tracing::debug!(nqn, "Removing stalled NQN path");
            }
        }
    }

    /// Handle command for NVMe cache.
    fn handle_cache_command(&self, cmd: NvmePathCacheCommand) -> NvmePathCacheCommandResponse {
        match cmd {
            NvmePathCacheCommand::LookupNvmeController(nqn) => {
                match self.suspected_paths.get(&nqn) {
                    Some(c) => {
                        NvmePathCacheCommandResponse::CachedNvmeController(NvmeController::from(c))
                    }
                    None => NvmePathCacheCommandResponse::Nack(format!(
                        "No NVMe cache record for NQN {}",
                        nqn
                    )),
                }
            }
        }
    }

    /// Start NVMe path error detection loop.
    pub async fn start(mut self) -> anyhow::Result<()> {
        let mut path_provider = CachedNvmePathProvider::new();
        let mut path_collection = path_provider.get_path_collection().unwrap();
        let start = path_provider.start()?;
        tokio::pin!(start);

        tracing::info!(
            "Starting NVMe path error detection loop, path detection interval: {:?}",
            self.detection_period,
        );

        loop {
            tokio::select! {
                // Path-provider loop, should not complete.
                _ = &mut start => {
                    tracing::warn!("NVMe path provider completed, stopping error detection");
                    break;
                },

                // Path rescan loop.
                _ = sleep(self.detection_period) => self.rescan_paths(&mut path_collection),

                // Cache command loop.
                msg = self.cache_channel_rx.recv() => {
                    match msg {
                        Some((cmd, sender)) => {
                            if let Err(error) = sender
                                .send(self.handle_cache_command(cmd))
                                .await {
                                    tracing::error!(%error,"Failed to send response to cache client");
                                }
                        },
                        None => {
                            tracing::error!("Empty command for NVMe path cache, ignoring");
                        }
                    }
                }
            }
        }

        tracing::info!("Stopping NVMe path error detection loop");
        Ok(())
    }

    /// Instantiate Nvme cache front-end.
    pub fn get_cache(&self) -> NvmePathCache {
        NvmePathCache::new(self.cache_channel_tx.clone())
    }
}

impl NvmePathCache {
    pub(crate) async fn lookup_controller(&self, nqn: String) -> anyhow::Result<NvmeController> {
        let (tx, mut rx) = channel(1);

        self.channel
            .send((NvmePathCacheCommand::LookupNvmeController(nqn.clone()), tx))
            .await
            .map_err(|_| anyhow::Error::msg("No Nvme cache available"))?;

        match rx.recv().await {
            Some(NvmePathCacheCommandResponse::CachedNvmeController(ctrlr)) => Ok(ctrlr),
            _ => Err(anyhow::Error::msg(format!(
                "Can't find Nvme controller for NQN: {}",
                nqn
            ))),
        }
    }
}
