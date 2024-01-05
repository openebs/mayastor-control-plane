use crate::{cluster_agent_client, Cli};
use grpc::operations::ha_node::traits::ClusterAgentOperations;
use stor_port::types::v0::transport::{FailedPath, ReportFailedPaths};

use tokio::{
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    time::{sleep, Duration},
};

/// Initial size of a batch.
const DEFAULT_BATCH_SIZE: usize = 16;

/// Entity that reports failed NVMe paths to HA Cluster agent via gRPC.
/// Failed NVMe paths are always aggregated before sending, which avoids
/// many gRPC invocations for ebery individual failed path and fully utilizes
/// the ability to report multiple failed paths in one call.
#[derive(Debug)]
pub struct PathReporter {
    node_name: String,
    channel: UnboundedSender<String>,
    retransmission_period: Duration,
    aggregation_period: Duration,
    ana_enabled: bool,
}

impl PathReporter {
    /// Get a new `Self` with the given parameters.
    pub fn new(
        node_name: String,
        retransmission_period: Duration,
        aggregation_period: Duration,
        ha_enabled: bool,
    ) -> Self {
        let (tx, rx) = unbounded_channel();

        let reporter = Self {
            channel: tx,
            node_name,
            retransmission_period,
            aggregation_period,
            ana_enabled: ha_enabled,
        };

        reporter.start(rx);
        reporter
    }

    /// Start main loop for reporter.
    fn start(&self, path_receiver: UnboundedReceiver<String>) {
        let node_name = self.node_name.clone();
        let retransmission_period = self.retransmission_period;
        let aggregation_period = self.aggregation_period;

        tracing::info!(
            ?retransmission_period,
            ?aggregation_period,
            "Starting path reporter"
        );

        tokio::spawn(async move {
            let mut aggregator = RequestAggregator::new(path_receiver, aggregation_period);

            // Phase 1: wait till a path batch is available.
            while let Ok(batch) = aggregator.receive_batch().await {
                // Phase 2: send all aggregated paths in one shot.
                Self::process_batch(batch, &node_name, retransmission_period);
            }
        });
    }

    fn process_batch(batch: PathBatch, node_name: &str, retransmission_period: Duration) {
        let failed_paths = batch
            .into_paths()
            .into_iter()
            .map(FailedPath::new)
            .collect::<Vec<FailedPath>>();
        let node_ep = Cli::args().grpc_endpoint;
        let mut req = ReportFailedPaths::new(node_name.to_string(), failed_paths, node_ep);

        // Report all paths in a separate task, continue till transmission succeeds.
        tokio::spawn(async move {
            let client = cluster_agent_client();

            // todo: should we check if we still ought to report failed paths after error?
            //       otherwise when we finally report it could be very outdated?
            while !req.failed_paths().is_empty() {
                match client.report_failed_nvme_paths(&req, None).await {
                    Ok(report) if report.is_empty() => {
                        break;
                    }
                    Ok(report) => {
                        tracing::error!(?report, "Failed paths reporting");
                        for fail in report.into_failed_paths() {
                            match fail.status_code {
                                tonic::Code::Ok
                                | tonic::Code::InvalidArgument
                                | tonic::Code::AlreadyExists => {
                                    req.remove_path(&fail.failed_nqn);
                                }
                                // anything else, retry
                                _ => {
                                    tracing::error!(?fail, "Failed to report failed NVMe paths");
                                }
                            }
                        }
                    }
                    Err(error) => {
                        tracing::error!(%error, "Failed to report failed NVMe paths");
                    }
                }
                sleep(retransmission_period).await;
            }
        });
    }

    /// Reports the given NVMe NQN as a failed path.
    pub fn report_failed_path(&self, nqn: String) {
        if !self.ana_enabled {
            tracing::warn!(
                path.nqn = nqn,
                "NVMe ANA multipathing is not supported, not reporting the failed path"
            );
        } else {
            self.channel.send(nqn).ok();
        }
    }

    /// Get node name.
    pub fn node_name(&self) -> &str {
        &self.node_name
    }
}

#[derive(Debug)]
struct PathBatch {
    paths: Vec<String>,
}

impl PathBatch {
    fn new() -> Self {
        Self {
            paths: Vec::with_capacity(DEFAULT_BATCH_SIZE),
        }
    }

    /// Convert `Self` into its paths.
    fn into_paths(self) -> Vec<String> {
        self.paths
    }

    /// Add a new path to the list.
    fn add_path(&mut self, path: String) {
        self.paths.push(path)
    }
}

/// Batched aggregator which aggregates reported paths over
/// time window and produces one batch for all paths reported
/// within this time window.
struct RequestAggregator {
    batch_receiver: UnboundedReceiver<PathBatch>,
    aggregation_period: Duration,
}

impl RequestAggregator {
    fn new(path_receiver: UnboundedReceiver<String>, aggregation_period: Duration) -> Self {
        let (tx, rx) = unbounded_channel();

        let receiver = Self {
            batch_receiver: rx,
            aggregation_period,
        };

        receiver.start(path_receiver, tx);
        receiver
    }

    fn start(
        &self,
        mut path_receiver: UnboundedReceiver<String>,
        batch_sender: UnboundedSender<PathBatch>,
    ) {
        let aggregation_period = self.aggregation_period;

        tokio::spawn(async move {
            // Phase 1: wait for the first path to trigger batch aggregation.
            while let Some(path) = path_receiver.recv().await {
                // Phase 2: add all subsequent reported paths to the batch.
                let mut batch = PathBatch::new();
                batch.add_path(path);

                loop {
                    tokio::select! {
                        receiver = path_receiver.recv() => {
                            match receiver {
                                Some(path) => {
                                    batch.add_path(path);
                                }
                                None => break
                            }
                        },
                        _ = sleep(aggregation_period) => {
                            break;
                        }
                    }
                }

                if batch_sender.send(batch).is_err() {
                    break;
                }
            }
        });
    }

    /// Wait till a batch of reported paths is available.
    pub async fn receive_batch(&mut self) -> anyhow::Result<PathBatch> {
        self.batch_receiver
            .recv()
            .await
            .ok_or_else(|| anyhow::Error::msg("Batch producer's channel disappeared"))
    }
}
