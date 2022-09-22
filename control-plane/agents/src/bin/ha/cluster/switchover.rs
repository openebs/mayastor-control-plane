use crate::{etcd::EtcdStore, nodes::NodeList};
use chrono::Utc;
use common_lib::types::v0::{
    store::{
        switchover::{Operation, OperationState, SwitchOverSpec, SwitchOverTime},
        SpecTransaction,
    },
    transport::VolumeId,
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::info;
use utils::NVME_TARGET_NQN_PREFIX;

use std::convert::TryFrom;

/// Stage represents the steps for switchover request.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub enum Stage {
    /// Initialize switchover request.
    Init,
    /// Shutdown original/old volume target.
    ShutdownOriginal,
    /// Create new volume target.
    ReconstructTarget,
    /// Update volume with new volume target.
    SwitchOverTarget,
    /// Delete old target.
    DeleteTarget,
    /// Publish updated path of volume to node-agent.
    PublishPath,
    /// Represet failed switchover request.
    Errored,
}

/// SwitchOverRequest defines spec for switchover.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct SwitchOverRequest {
    callback_uri: String,
    pub volume_id: VolumeId,
    stage: Stage,
    // Timestamp when switchover request was initialized.
    timestamp: SwitchOverTime,
}

impl From<&SwitchOverRequest> for SwitchOverSpec {
    fn from(req: &SwitchOverRequest) -> Self {
        let op = OperationState::new(req.stage.clone().into(), None);
        Self {
            callback_uri: req.callback_uri.clone(),
            volume: req.volume_id.clone(),
            operation: Some(op),
            timestamp: req.timestamp,
        }
    }
}

impl From<Stage> for Operation {
    fn from(stage: Stage) -> Self {
        match stage {
            Stage::Init => Operation::Init,
            Stage::ShutdownOriginal => Operation::ShutdownOriginal,
            Stage::ReconstructTarget => Operation::ReconstructTarget,
            Stage::SwitchOverTarget => Operation::SwitchOverTarget,
            Stage::DeleteTarget => Operation::DeleteTarget,
            Stage::PublishPath => Operation::PublishPath,
            Stage::Errored => Operation::Errored("".to_string()),
        }
    }
}

impl From<Operation> for Stage {
    fn from(op: Operation) -> Self {
        match op {
            Operation::Init => Stage::Init,
            Operation::ShutdownOriginal => Stage::ShutdownOriginal,
            Operation::ReconstructTarget => Stage::ReconstructTarget,
            Operation::SwitchOverTarget => Stage::SwitchOverTarget,
            Operation::DeleteTarget => Stage::DeleteTarget,
            Operation::PublishPath => Stage::PublishPath,
            Operation::Errored(_) => Stage::Errored,
        }
    }
}

impl From<&SwitchOverSpec> for SwitchOverRequest {
    fn from(req: &SwitchOverSpec) -> Self {
        let mut stage = Stage::Init;
        if let Some(op) = req.operation() {
            stage = op.into();
        }

        Self {
            callback_uri: req.callback_uri.clone(),
            volume_id: req.volume.clone(),
            stage,
            timestamp: req.timestamp,
        }
    }
}

/// SwitchOverEngine defines spec for switchover engine.
#[derive(Debug, Clone)]
pub struct SwitchOverEngine {
    etcd: EtcdStore,
    nodes: NodeList,
    channel: UnboundedSender<SwitchOverRequest>,
}

impl SwitchOverRequest {
    pub fn new(callback_uri: String, volume: VolumeId) -> SwitchOverRequest {
        SwitchOverRequest {
            callback_uri,
            volume_id: volume,
            stage: Stage::Init,
            timestamp: Utc::now(),
        }
    }

    pub fn with_stage(&mut self, stage: Stage) {
        self.stage = stage;
    }

    pub fn stage(&self) -> Stage {
        self.stage.clone()
    }

    pub fn timestamp(&self) -> SwitchOverTime {
        self.timestamp
    }

    /// Update stage with next stage.
    /// If a stage is PublishPath or Errored then it will not be updated.
    pub fn update_next_stage(&mut self) {
        self.stage = match self.stage {
            Stage::Init => Stage::ShutdownOriginal,
            Stage::ShutdownOriginal => Stage::ReconstructTarget,
            Stage::ReconstructTarget => Stage::SwitchOverTarget,
            Stage::SwitchOverTarget => Stage::DeleteTarget,
            Stage::DeleteTarget => Stage::PublishPath,
            // PublishPath and Errored stage mark request as complete, so no need to update
            Stage::PublishPath => Stage::PublishPath,
            Stage::Errored => Stage::Errored,
        };
    }

    /// Start_op must be called before starting the respective stage operation on the request.
    /// Start_op will store the request with updated stage in formation in etcd.
    pub async fn start_op(&self, stage: Stage, etcd: &EtcdStore) -> Result<(), anyhow::Error> {
        let mut spec: SwitchOverSpec = self.into();
        spec.start_op(stage.into());
        etcd.store_obj(&spec).await
    }

    /// Complete_op must be called after completion of the respective stage operation on the
    /// request.
    /// Complete_op will store the request in etcd with either updated stage or error message.
    pub async fn complete_op(
        &self,
        result: bool,
        msg: String,
        etcd: &EtcdStore,
    ) -> Result<(), anyhow::Error> {
        let mut spec = SwitchOverSpec::try_from(self)?;

        if result {
            spec.set_op_result(result);
            spec.commit_op();
        } else {
            spec.set_error_msg(msg);
        }
        etcd.store_obj(&spec).await?;

        Ok(())
    }

    pub async fn delete_request(&self, etcd: &EtcdStore) -> Result<(), anyhow::Error> {
        let spec = SwitchOverSpec::try_from(self)?;
        etcd.delete_obj(&spec).await
    }

    /// Initialize the switchover request.
    async fn initialize(&mut self, etcd: &EtcdStore) -> Result<(), anyhow::Error> {
        self.start_op(Stage::Init, etcd).await?;

        info!(volume.uuid=%self.volume_id, "Initializing");

        self.complete_op(true, "".to_string(), etcd).await?;
        self.update_next_stage();
        Ok(())
    }

    /// Shutdown original/old volume target.
    async fn shutdown_original_target(&mut self, etcd: &EtcdStore) -> Result<(), anyhow::Error> {
        self.start_op(Stage::ShutdownOriginal, etcd).await?;

        info!(volume.uuid=%self.volume_id, "Shutting down traget");

        self.complete_op(true, "".to_string(), etcd).await?;
        self.update_next_stage();
        Ok(())
    }

    /// Create new volume target for the switchover.
    async fn reconstruct_target(&mut self, etcd: &EtcdStore) -> Result<(), anyhow::Error> {
        self.start_op(Stage::ReconstructTarget, etcd).await?;

        info!(volume.uuid=%self.volume_id, "Reconstructing volume target");

        self.complete_op(true, "".to_string(), etcd).await?;
        self.update_next_stage();
        Ok(())
    }

    /// Update volume with newly constructed target.
    async fn switchover_target(&mut self, etcd: &EtcdStore) -> Result<(), anyhow::Error> {
        self.start_op(Stage::SwitchOverTarget, etcd).await?;

        info!(volume.uuid=%self.volume_id, "Switching volume target");

        self.complete_op(true, "".to_string(), etcd).await?;
        self.update_next_stage();
        Ok(())
    }

    /// Destroy old/original target.
    async fn delete_target(&mut self, etcd: &EtcdStore) -> Result<(), anyhow::Error> {
        self.start_op(Stage::DeleteTarget, etcd).await?;

        info!(volume.uuid=%self.volume_id, "Deleting volume target");

        self.complete_op(true, "".to_string(), etcd).await?;
        self.update_next_stage();
        Ok(())
    }

    /// Publish updated path for the volume to node-agent.
    async fn publish_path(
        &mut self,
        etcd: &EtcdStore,
        nodes: &NodeList,
    ) -> Result<(), anyhow::Error> {
        self.start_op(Stage::PublishPath, etcd).await?;

        info!(volume.uuid=%self.volume_id, "Publishing volume target");

        nodes
            .remove_failed_path(format!("{}{}", NVME_TARGET_NQN_PREFIX, self.volume_id))
            .await;
        self.complete_op(true, "".to_string(), etcd).await?;
        Ok(())
    }
}

impl SwitchOverEngine {
    pub fn new(etcd: EtcdStore, nodes: NodeList) -> Self {
        let (rq_tx, rq_rx) = unbounded_channel();

        let sw = SwitchOverEngine {
            channel: rq_tx,
            etcd,
            nodes,
        };

        sw.mover(rq_rx);
        sw
    }

    /// Mover is responsible for processing the switchover request sequentially.
    pub fn mover(&self, mut recv: UnboundedReceiver<SwitchOverRequest>) {
        let etcd = self.etcd.clone();
        let nodes = self.nodes.clone();

        tokio::spawn(async move {
            loop {
                if let Some(mut q) = recv.recv().await {
                    loop {
                        // TODO: error handling
                        let _res = match q.stage {
                            Stage::Init => q.initialize(&etcd).await,
                            Stage::ShutdownOriginal => q.shutdown_original_target(&etcd).await,
                            Stage::ReconstructTarget => q.reconstruct_target(&etcd).await,
                            Stage::SwitchOverTarget => q.switchover_target(&etcd).await,
                            Stage::DeleteTarget => q.delete_target(&etcd).await,
                            Stage::PublishPath => match q.publish_path(&etcd, &nodes).await {
                                Ok(_) => break,
                                Err(e) => Err(e),
                            },
                            _ => break,
                        };
                    }
                }
            }
        });
    }

    pub fn initiate(&self, req: SwitchOverRequest) {
        self.channel.send(req).expect("sending req failed");
    }
}
