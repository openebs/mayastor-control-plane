use crate::{
    path_provider::{CachedNvmePathProvider, NvmePath, NvmePathNameCollection},
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
#[derive(Debug, Clone, Eq, PartialEq)]
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

/// An abstraction over a number of paths (at least 1).
/// We can have more than 1 path when a path is broken and we try to connect a new one.
/// At this point we'll one new good path and the old broken path.
/// We *should* remove the broken paths, but temporarily we might have both.
#[derive(Debug, Clone)]
struct PathRecords {
    _nqn: String,
    paths: HashMap<String, PathRecord>,
}
impl PathRecords {
    fn new(record: PathRecord) -> Self {
        let path = record.path.clone();
        Self {
            _nqn: record.nqn.clone(),
            paths: HashMap::from([(path, record)]),
        }
    }
    fn report_connecting(&mut self, ctrlr: &NvmePath, epoch: u64, reporter: &Rc<PathReporter>) {
        let path = ctrlr.path().to_string_lossy().to_string();
        let record = self.paths.entry(path.clone()).or_insert_with(|| {
            PathRecord::new(ctrlr.nqn().to_owned(), epoch, path, reporter.clone())
        });
        record.report_connecting();
        record.set_epoch(epoch);
    }
    fn report_live(&mut self, ctrlr: &NvmePath, epoch: u64, reporter: &Rc<PathReporter>) {
        let path = ctrlr.path().to_string_lossy().to_string();
        let record = self.paths.entry(path.clone()).or_insert_with(|| {
            PathRecord::new(ctrlr.nqn().to_owned(), epoch, path, reporter.clone())
        });
        record.report_live();
        record.set_epoch(epoch);
    }
    fn has_broken_paths(&self) -> bool {
        !self.paths().is_empty() && self.paths().iter().any(|(_, r)| r.state != PathState::Good)
    }
    fn paths(&self) -> &HashMap<String, PathRecord> {
        &self.paths
    }
    fn paths_mut(&mut self) -> &mut HashMap<String, PathRecord> {
        &mut self.paths
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
        tracing::trace!(%path, %nqn, "New PathRecord");
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

    /// Trigger state transition based on 'connecting' state of the underlying NVMe controller.
    fn report_connecting(&mut self) {
        match self.state {
            PathState::Good => {
                self.state = PathState::Suspected;
                tracing::info!(
                    state=%PathState::Suspected,
                    target=self.nqn,
                    path=self.path,
                    "Target state transition"
                );
            }
            PathState::Suspected => {
                self.state = PathState::Failed;
                tracing::error!(
                    state=%PathState::Failed,
                    target=self.nqn,
                    path=self.path,
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
impl From<&PathRecords> for Vec<NvmeController> {
    fn from(v: &PathRecords) -> Self {
        v.paths().values().map(NvmeController::from).collect()
    }
}

/// Command for querying NVMe path cache.
#[derive(Debug)]
enum NvmePathCacheCommand {
    LookupNvmeControllers(String),
}

/// Response objects for NVMe cache commands.
enum NvmePathCacheCommandResponse {
    /// Negative response, contains extended information about failure.
    Nack(String),
    /// Cached instance of an NVMe controller available in the system.
    CachedNvmeControllers(Vec<NvmeController>),
}

/// Message sent to NVMe cache from a cache client: client passes a channel
/// to receive a response from the cache.
type NvmeCacheMessage = (NvmePathCacheCommand, Sender<NvmePathCacheCommandResponse>);

/// Path failure detector for NVMe paths.
/// All known NVMe paths on system are periodically checked for liveness and get classified
/// as:
///  Good - path is fully functional.
///  Suspected - path experiences connectivity problems for the first time.
///  Failed - path has experienced connectivity problems two times in a row.
/// Once a path is classified as Failed, it's reported to PathReporter and gets sent to
/// HA Cluster agent.
#[derive(Debug)]
pub struct PathFailureDetector {
    epoch: u64,
    detection_period: Duration,
    suspected_paths: HashMap<String, PathRecords>,
    reporter: Rc<PathReporter>,
    cache_channel_rx: Receiver<NvmeCacheMessage>,
    cache_channel_tx: Arc<Sender<NvmeCacheMessage>>,
}

impl PathFailureDetector {
    /// Return a new `Self`.
    pub(crate) fn new(args: &Cli) -> Self {
        let reporter = PathReporter::new(
            args.node_name.clone(),
            *args.retransmission_period,
            *args.aggregation_period,
        );

        let (tx, rx) = channel(64);

        Self {
            epoch: 0,
            detection_period: *args.detection_period,
            suspected_paths: HashMap::new(),
            reporter: Rc::new(reporter),
            cache_channel_tx: Arc::new(tx),
            cache_channel_rx: rx,
        }
    }

    fn rescan_paths(&mut self, path_collection: &mut NvmePathNameCollection) {
        // Update epoch before scanning controllers.
        self.epoch += 1;

        tracing::trace!(epoch=self.epoch, suspected=?self.suspected_paths, "Rescanning");

        // Scan all reported NVMe paths on system and check for connectivity.
        for ctrlr in path_collection.get_entries() {
            // Silently ignore errors when we can't get a subsystem for target path,
            // as we might see lots of false-positive errors when removing a failed path
            // from a multi-pathed NVMe subsystem.
            if let Ok(subsystem) = Subsystem::new(ctrlr.path()) {
                match subsystem.state.as_str() {
                    "connecting" => {
                        // Add a new record in case no record exists for target NQN.
                        let rec = self
                            .suspected_paths
                            .entry(subsystem.nqn)
                            .or_insert_with(|| {
                                PathRecords::new(PathRecord::new(
                                    ctrlr.nqn().to_owned(),
                                    self.epoch,
                                    ctrlr.path().to_string_lossy().to_string(),
                                    self.reporter.clone(),
                                ))
                            });
                        rec.report_connecting(ctrlr, self.epoch, &self.reporter);
                    }
                    "live" => {
                        if let Some(rec) = self.suspected_paths.get_mut(&subsystem.nqn) {
                            rec.report_live(ctrlr, self.epoch, &self.reporter);
                        }
                    }
                    _ => {}
                }
            }
        }

        // Remove all existing records that don't have underlying NVMe controllers:
        // can happen in case controller was removed after it had been identified as
        // broken/suspected. Stalled/outdated records have a different (old) epoch number
        // since they have not been touched during the current iteration.
        self.suspected_paths.values_mut().for_each(|v| {
            v.paths_mut().retain(|path, rec| {
                if rec.get_epoch() != self.epoch {
                    tracing::debug!(nqn = rec.get_nqn(), path, "Removing stalled NQN path");
                }
                rec.get_epoch() == self.epoch
            })
        });
        // ReviewQuestion: If the paths are now good, we don't need to keep them right?
        self.suspected_paths
            .retain(|_nqn, recs| recs.has_broken_paths());
    }

    /// Handle command for NVMe cache.
    fn handle_cache_command(&self, cmd: NvmePathCacheCommand) -> NvmePathCacheCommandResponse {
        match cmd {
            NvmePathCacheCommand::LookupNvmeControllers(nqn) => {
                match self.suspected_paths.get(&nqn) {
                    Some(c) => {
                        NvmePathCacheCommandResponse::CachedNvmeControllers(
                            Vec::<NvmeController>::from(c),
                        )
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
    /// Lookup a controller by its NVMe NQN.
    pub(crate) async fn lookup_controllers(
        &self,
        nqn: String,
    ) -> anyhow::Result<Vec<NvmeController>> {
        let (tx, mut rx) = channel(1);

        self.channel
            .send((NvmePathCacheCommand::LookupNvmeControllers(nqn.clone()), tx))
            .await
            .map_err(|_| anyhow::Error::msg("No Nvme cache available"))?;

        match rx.recv().await {
            Some(NvmePathCacheCommandResponse::CachedNvmeControllers(ctrlr)) => Ok(ctrlr),
            _ => Err(anyhow::Error::msg(format!(
                "Can't find Nvme controller for NQN: {}",
                nqn
            ))),
        }
    }
}
