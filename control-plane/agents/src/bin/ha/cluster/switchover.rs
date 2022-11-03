use crate::{core_grpc, etcd::EtcdStore, nodes::NodeList};
use chrono::Utc;
use common_lib::types::v0::{
    store::{
        switchover::{Operation, OperationState, SwitchOverSpec, SwitchOverTime},
        SpecTransaction,
    },
    transport::{
        DestroyShutdownTargets, ReplacePath, RepublishVolume, VolumeId, VolumeShareProtocol,
    },
};
use grpc::operations::{ha_node::client::NodeAgentClient, volume::traits::VolumeOperations};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::{error, info};
use utils::NVME_TARGET_NQN_PREFIX;

use anyhow::anyhow;
use common_lib::transport_api::ReplyErrorKind;
use grpc::operations::ha_node::traits::NodeAgentOperations;
use std::{convert::TryFrom, net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tonic::transport::Uri;

fn client() -> impl VolumeOperations {
    core_grpc().volume()
}

/// Stage represents the steps for switchover request.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub enum Stage {
    /// Initialize switchover request.
    Init,
    /// Shutdown original/old volume target. Create new nexus for existing vol obj.
    RepublishVolume,
    /// Publish updated path of volume to node-agent.
    PublishPath,
    /// Delete original/old volume target.
    DeleteTarget,
    /// Marks switchover process as Complete.
    Successful,
    /// Represent failed switchover request.
    Errored,
}

/// SwitchOverRequest defines spec for switchover.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct SwitchOverRequest {
    callback_uri: SocketAddr,
    pub volume_id: VolumeId,
    stage: Stage,
    // Timestamp when switchover request was initialized.
    timestamp: SwitchOverTime,
    // Failed nexus path of the volume.
    existing_nqn: String,
    // New nexus path of the volume.
    new_path: Option<String>,
    // Number of failed attempts in the current Stage.
    retry_count: u64,
}

impl SwitchOverRequest {
    /// Create a new switchover request for every failed Nvme path.
    pub fn new(
        callback_uri: SocketAddr,
        volume: VolumeId,
        existing_path: String,
    ) -> SwitchOverRequest {
        SwitchOverRequest {
            callback_uri,
            volume_id: volume,
            stage: Stage::Init,
            timestamp: Utc::now(),
            existing_nqn: existing_path,
            new_path: None,
            retry_count: 0,
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
            Stage::Init => Stage::RepublishVolume,
            Stage::RepublishVolume => Stage::PublishPath,
            Stage::PublishPath => Stage::DeleteTarget,
            Stage::DeleteTarget => Stage::Successful,
            // Successful and Errored stage mark request as complete, so no need to update
            Stage::Successful => Stage::Successful,
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

    async fn republish_volume(&mut self, etcd: &EtcdStore) -> Result<(), anyhow::Error> {
        self.start_op(Stage::RepublishVolume, etcd).await?;
        info!(volume.uuid=%self.volume_id, "Republishing");
        let republish_req = RepublishVolume {
            uuid: self.volume_id.clone(),
            target_node: None,
            share: VolumeShareProtocol::Nvmf,
            force: false,
        };
        let vol = client().republish(&republish_req, None).await?;
        self.new_path = match vol.state().target {
            Some(target) => Some(target.device_uri),
            _ => None,
        };
        if self.new_path.is_none() {
            error!(volume.uuid=%self.volume_id, "Could not find device uri for the volume. Marking request as errored");
            return Err(anyhow!("Couldnt find device uri for the volume"));
        }
        self.complete_op(true, "".to_string(), etcd).await?;
        self.update_next_stage();
        Ok(())
    }

    /// Destroy old/original target.
    async fn delete_target(&mut self, etcd: &EtcdStore) -> Result<(), anyhow::Error> {
        self.start_op(Stage::DeleteTarget, etcd).await?;

        info!(volume.uuid=%self.volume_id, "Deleting volume target");
        let destroy_request = DestroyShutdownTargets {
            uuid: self.volume_id.clone(),
        };
        client()
            .destroy_shutdown_target(&destroy_request, None)
            .await?;
        self.complete_op(true, "".to_string(), etcd).await?;
        self.update_next_stage();
        Ok(())
    }

    /// Deletes Successful Switchover request from etcd.
    async fn delete_switchover(&mut self, etcd: &EtcdStore) -> Result<(), anyhow::Error> {
        self.start_op(Stage::Successful, etcd).await?;
        info!(volume.uuid=%self.volume_id, "Deleting Switchover request from etcd as request completed successfully");
        match self.delete_request(etcd).await {
            Ok(_) => Ok(()),
            Err(_) => Err(anyhow!(
                "Encountered error while trying to delete SwitchOverSpec from etcd for {}",
                self.volume_id
            )),
        }
    }

    /// Deletes Errored Switchover request from etcd.
    async fn errored_switchover(&mut self, etcd: &EtcdStore) -> Result<(), anyhow::Error> {
        self.start_op(Stage::Errored, etcd).await?;
        info!(volume.uuid=%self.volume_id, "Error occurred while processing Switchover request");
        match self.delete_request(etcd).await {
            Ok(_) => Ok(()),
            Err(_) => Err(anyhow!(
                "Encountered error while trying to delete SwitchOverSpec from etcd for {}",
                self.volume_id
            )),
        }
    }

    /// Publish updated path for the volume to node-agent.
    async fn publish_path(
        &mut self,
        etcd: &EtcdStore,
        nodes: &NodeList,
    ) -> Result<(), anyhow::Error> {
        self.start_op(Stage::PublishPath, etcd).await?;
        info!(volume.uuid=%self.volume_id, "Publishing new volume target to node agent");
        if let Ok(uri) = Uri::builder()
            .scheme("http")
            .authority(self.callback_uri.to_string())
            .path_and_query("")
            .build()
        {
            info!(uri=%uri, "Creating node agent client using callback uri");
            if let Some(new_path) = self.new_path.clone() {
                let replace_request = ReplacePath::new(self.existing_nqn.clone(), new_path.clone());
                let client = NodeAgentClient::new(uri, None).await;
                if let Err(e) = client.replace_path(&replace_request, None).await {
                    return if e.kind == ReplyErrorKind::FailedPrecondition {
                        error!(nexus.path=%self.existing_nqn, "HA Node agent could not find failed Nvme path");
                        info!(volume.uuid=%self.volume_id, "Moving Switchover request to Errored state");
                        self.stage = Stage::Errored;
                        Err(anyhow!("HA Node agent could not lookup old Nvme path"))
                    } else {
                        Err(anyhow!("Nvme path replacement failed"))
                    };
                }
                nodes
                    .remove_failed_path(format!("{}{}", NVME_TARGET_NQN_PREFIX, self.volume_id))
                    .await;
                self.complete_op(true, "".to_string(), etcd).await?;
                self.update_next_stage();
                Ok(())
            } else {
                Err(anyhow!("Could not to get new nexus target for the volume"))
            }
        } else {
            self.stage = Stage::Errored;
            error!(volume.uuid=%self.volume_id, "Could not get grpc address of the node agent. Moving Switchover req to Errored");
            Err(anyhow!("Could not get grpc address of the node agent"))
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

/// Number of Switchover worker tasks to be spawned.
const WORKER_NUM: u8 = 4;

enum ReQueue {
    /// Sleep duration during Fast requeue phase.
    Fast = 10,
    /// Sleep duration during Slow requeue phase.
    Slow = 30,
    /// Number of Fast request requeue per stage.
    NumFast = 18,
}

impl SwitchOverEngine {
    /// Creates a new switchover engine to process Nvme path failures.
    pub fn new(etcd: EtcdStore, nodes: NodeList) -> Self {
        let (rq_tx, rq_rx) = unbounded_channel();

        let sw = SwitchOverEngine {
            channel: rq_tx,
            etcd,
            nodes,
        };

        sw.init_worker(Arc::new(Mutex::new(rq_rx)));
        sw
    }

    /// Instantiates worker task to asynchronously process Switchover request.
    pub fn init_worker(&self, recv: Arc<Mutex<UnboundedReceiver<SwitchOverRequest>>>) {
        for i in 0 .. WORKER_NUM {
            info!("Starting worker-{} of Switchover Engine", i);
            let cloned_self = self.clone();
            let cloned_channel = recv.clone();
            tokio::spawn(async move { cloned_self.worker(cloned_channel, i).await });
        }
    }

    /// Switchover request to be handled synchronously in each worker task.
    async fn worker(self, recv: Arc<Mutex<UnboundedReceiver<SwitchOverRequest>>>, worker_num: u8) {
        loop {
            let mut lo = recv.lock().await;
            if let Some(mut q) = lo.recv().await {
                drop(lo);
                info!(
                    volume.uuid = %q.volume_id,
                    worker = worker_num,
                    "Volume switchover picked by worker",
                );
                loop {
                    // TODO: handle error handling properly
                    let result = match q.stage {
                        Stage::Init => q.initialize(&self.etcd).await,
                        Stage::RepublishVolume => q.republish_volume(&self.etcd).await,
                        Stage::PublishPath => q.publish_path(&self.etcd, &self.nodes).await,
                        Stage::DeleteTarget => q.delete_target(&self.etcd).await,
                        Stage::Errored => match q.errored_switchover(&self.etcd).await {
                            Ok(_) => break,
                            Err(e) => Err(e),
                        },
                        Stage::Successful => match q.delete_switchover(&self.etcd).await {
                            Ok(_) => break,
                            Err(e) => Err(e),
                        },
                    };
                    match result {
                        Ok(_) => {
                            // reset retry count back to the start after successfully completing a
                            // stage.
                            q.retry_count = 0;
                        }
                        Err(_) => {
                            info!(volume.uuid=%q.volume_id, "Sending failed Switchover request back to the channel");
                            q.retry_count += 1;
                            self.enqueue(q);
                            break;
                        }
                    }
                }
            }
        }
    }

    /// Sends Switchover request to the Channel after sleeping for a specified duration if
    /// specified.
    pub fn enqueue(&self, req: SwitchOverRequest) {
        let errored_request = req.retry_count > 0;
        let retry_delay = if req.retry_count < ReQueue::NumFast as u64 {
            ReQueue::Fast as u64
        } else {
            ReQueue::Slow as u64
        };
        let tx_clone = self.channel.clone();
        tokio::spawn(async move {
            if errored_request {
                tokio::time::sleep(Duration::from_secs(retry_delay)).await;
            }
            tx_clone.send(req)
        });
    }
}

impl From<&SwitchOverRequest> for SwitchOverSpec {
    fn from(req: &SwitchOverRequest) -> Self {
        let op = OperationState::new(req.stage.clone().into(), None);
        Self {
            callback_uri: req.callback_uri,
            volume: req.volume_id.clone(),
            operation: Some(op),
            timestamp: req.timestamp,
            existing_nqn: req.existing_nqn.clone(),
            new_path: req.new_path.clone(),
            retry_count: req.retry_count,
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
            callback_uri: req.callback_uri,
            volume_id: req.volume.clone(),
            stage,
            timestamp: req.timestamp,
            existing_nqn: req.existing_nqn.clone(),
            new_path: req.new_path.clone(),
            retry_count: req.retry_count,
        }
    }
}

impl From<Stage> for Operation {
    fn from(stage: Stage) -> Self {
        match stage {
            Stage::Init => Operation::Init,
            Stage::RepublishVolume => Operation::RepublishVolume,
            Stage::PublishPath => Operation::PublishPath,
            Stage::DeleteTarget => Operation::DeleteTarget,
            Stage::Successful => Operation::Successful,
            Stage::Errored => Operation::Errored("".to_string()),
        }
    }
}

impl From<Operation> for Stage {
    fn from(op: Operation) -> Self {
        match op {
            Operation::Init => Stage::Init,
            Operation::RepublishVolume => Stage::RepublishVolume,
            Operation::PublishPath => Stage::PublishPath,
            Operation::DeleteTarget => Stage::DeleteTarget,
            Operation::Successful => Stage::Successful,
            Operation::Errored(_) => Stage::Errored,
        }
    }
}
