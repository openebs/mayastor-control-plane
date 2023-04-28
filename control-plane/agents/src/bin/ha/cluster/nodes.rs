use crate::{
    switchover::{Stage, SwitchOverRequest, SwitchOverStage},
    volume::VolumeMover,
};
use stor_port::{
    transport_api::{ReplyError, ResourceKind},
    types::v0::transport::NodeId,
};

use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::sync::Mutex;
use tracing::info;

/// A record about a failed path reported by the ha node agent.
#[derive(Debug)]
struct PathRecord {
    _socket: SocketAddr,
    stage: SwitchOverStage,
}
impl PathRecord {
    fn stage(&self) -> Stage {
        self.stage.read()
    }
}

/// Store node information and reported failed path.
#[derive(Debug, Default, Clone)]
pub(crate) struct NodeList {
    list: Arc<Mutex<HashMap<NodeId, SocketAddr>>>,
    failed_path: Arc<Mutex<HashMap<String, PathRecord>>>,
}

impl NodeList {
    /// Get a new `Self`.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Register node and its endpoint.
    /// If the node is already registered then update its details.
    pub(crate) async fn register_node(&self, name: NodeId, endpoint: SocketAddr) {
        let mut list = self.list.lock().await;
        list.insert(name, endpoint);
    }

    /// Remove path from failed_path list.
    pub(crate) async fn remove_failed_path(&self, path: &str) {
        let mut failed_path = self.failed_path.lock().await;
        failed_path.remove(path);
    }
    /// Add a failed switchover request to the failed paths.
    /// Useful when reloading from the pstor after a crash/restart.
    pub(crate) async fn insert_failed_request(&self, request: &SwitchOverRequest) {
        let record = PathRecord {
            _socket: request.socket(),
            stage: request.stage_arc(),
        };
        self.failed_path
            .lock()
            .await
            .insert(request.nqn().into(), record);
    }

    /// Send request to the switchover engine for the reported node and path.
    pub(crate) async fn report_failed_path(
        self,
        node: NodeId,
        path: String,
        mover: VolumeMover,
        endpoint: SocketAddr,
    ) -> Result<(), ReplyError> {
        // Check if node is registered in the hashmap. Register if not.
        self.list
            .lock()
            .await
            .entry(node.clone())
            .or_insert(endpoint);

        let mut failed_path = self.failed_path.lock().await;

        if let Some(record) = failed_path.get(&path) {
            return match record.stage() {
                Stage::ReplacePath | Stage::DeleteTarget | Stage::Successful | Stage::Errored => {
                    Err(ReplyError::failed_precondition(
                        ResourceKind::NvmePath,
                        path,
                        "Path is already reported for switchover".to_owned(),
                    ))
                }
                Stage::Init | Stage::RepublishVolume => Err(ReplyError::already_exist(
                    ResourceKind::NvmePath,
                    path,
                    "Path is already reported for switchover".to_owned(),
                )),
            };
        }

        info!(node.id=%node, %path, "Sending switchover");

        let stage = mover.switchover(node, endpoint, path.clone()).await?;
        let record = PathRecord {
            _socket: endpoint,
            stage,
        };
        failed_path.insert(path, record);
        Ok(())
    }
}
