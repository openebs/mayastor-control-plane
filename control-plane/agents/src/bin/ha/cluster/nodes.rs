use crate::volume::VolumeMover;
use common_lib::types::v0::transport::NodeId;
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::sync::Mutex;
use tracing::info;

/// Store node information and reported failed path.
#[derive(Debug, Default, Clone)]
pub struct NodeList {
    list: Arc<Mutex<HashMap<NodeId, SocketAddr>>>,
    failed_path: Arc<Mutex<HashMap<String, SocketAddr>>>,
}

impl NodeList {
    /// Get a new `Self`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register node and its endpoint.
    /// If the node is already registered then update its details.
    pub async fn register_node(&self, name: NodeId, endpoint: SocketAddr) {
        let mut list = self.list.lock().await;
        list.insert(name, endpoint);
    }

    /// Remove path from failed_path list.
    pub async fn remove_failed_path(&self, path: String) {
        let mut failed_path = self.failed_path.lock().await;
        failed_path.remove(&path);
    }

    /// Send request to the switchover engine for the reported node and path.
    pub async fn report_failed_path(
        self,
        node: NodeId,
        path: String,
        mover: VolumeMover,
        endpoint: SocketAddr,
    ) -> Result<(), anyhow::Error> {
        // Check if node is registered in the hashmap. Register if not.
        self.list
            .lock()
            .await
            .entry(node.clone())
            .or_insert(endpoint);

        let mut failed_path = self.failed_path.lock().await;

        if failed_path.get(&path).is_some() {
            anyhow::bail!("Path {} is already reported for switchover", path);
        };

        info!(node.id=%node, %path, "Sending switchover");

        mover.switchover(node, endpoint, path.clone()).await?;
        failed_path.insert(path, endpoint);
        Ok(())
    }
}
