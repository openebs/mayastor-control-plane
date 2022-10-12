use crate::volume::VolumeMover;
use common_lib::types::v0::transport::NodeId;
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::sync::Mutex;
use tracing::info;

/// Store node information and reported failed path.
#[derive(Debug, Clone)]
pub struct NodeList {
    list: Arc<Mutex<HashMap<NodeId, SocketAddr>>>,
    failed_path: Arc<Mutex<HashMap<String, SocketAddr>>>,
}

impl NodeList {
    pub fn new() -> Self {
        NodeList {
            list: Arc::new(Mutex::new(HashMap::new())),
            failed_path: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Register node and its endpoint.
    /// If node is already registered then update it details.
    pub async fn register_node(&self, name: NodeId, endpoint: SocketAddr) {
        let mut list = self.list.lock().await;
        list.insert(name, endpoint);
    }

    pub async fn remove_failed_path(&self, path: String) {
        let mut failed_path = self.failed_path.lock().await;
        failed_path.remove(&path);
    }

    /// Send switchover request to switchover engine for the
    /// reported node and path.
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

        let uri = {
            let list = self.list.lock().await;
            list.get(&node).copied()
        };

        let mut failed_path = self.failed_path.lock().await;

        if failed_path.get(&path).is_some() {
            anyhow::bail!("Path {} is already reported for switchover", path);
        };

        info!(node.id=%node, %path, "Sending switchover for path");

        if let Some(socket) = uri {
            info!(node.id=%node, %path, "Sending switchover for path");
            mover.switchover(socket, path.clone()).await?;
            failed_path.insert(path, socket);
            Ok(())
        } else {
            Err(anyhow::format_err!("Node {} is not registered", node))
        }
    }
}
