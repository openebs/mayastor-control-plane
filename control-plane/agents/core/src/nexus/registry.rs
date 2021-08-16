use crate::core::{registry::Registry, wrapper::*};
use common::errors::{NexusNotFound, SvcError};
use common_lib::types::v0::{
    message_bus::{Nexus, NexusId, NodeId},
    store::nexus_persistence::{NexusInfo, NexusInfoKey},
};
use snafu::OptionExt;

/// Nexus helpers
impl Registry {
    /// Get all nexuses from node `node_id` or from all nodes
    pub(crate) async fn get_node_opt_nexuses(
        &self,
        node_id: Option<NodeId>,
    ) -> Result<Vec<Nexus>, SvcError> {
        Ok(match node_id {
            None => self.get_nexuses().await,
            Some(node_id) => self.get_node_nexuses(&node_id).await?,
        })
    }

    /// Get all nexuses from node `node_id`
    pub(crate) async fn get_node_nexuses(&self, node_id: &NodeId) -> Result<Vec<Nexus>, SvcError> {
        let node = self.get_node_wrapper(node_id).await?;
        Ok(node.nexuses().await)
    }

    /// Get nexus `nexus_id` from node `node_id`
    pub(crate) async fn get_node_nexus(
        &self,
        node_id: &NodeId,
        nexus_id: &NexusId,
    ) -> Result<Nexus, SvcError> {
        let node = self.get_node_wrapper(node_id).await?;
        let nexus = node.nexus(nexus_id).await.context(NexusNotFound {
            nexus_id: nexus_id.clone(),
        })?;
        Ok(nexus)
    }

    /// Get nexus `nexus_id`
    pub(crate) async fn get_nexus(&self, nexus_id: &NexusId) -> Result<Nexus, SvcError> {
        let nodes = self.get_node_wrappers().await;
        for node in nodes {
            if let Some(nexus) = node.nexus(nexus_id).await {
                return Ok(nexus);
            }
        }
        Err(common::errors::SvcError::NexusNotFound {
            nexus_id: nexus_id.to_string(),
        })
    }

    /// Get all nexuses
    pub(crate) async fn get_nexuses(&self) -> Vec<Nexus> {
        let nodes = self.get_node_wrappers().await;
        let mut nexuses = vec![];
        for node in nodes {
            nexuses.extend(node.nexuses().await);
        }
        nexuses
    }

    /// Fetch the `NexusInfo` from the persistent store
    /// Returns an error if we fail to query the persistent store
    /// Returns Ok(None) if the entry does not exist
    /// allow_missing determines whether not finding the key is an allow or not
    pub(crate) async fn get_nexus_info(
        &self,
        nexus_uuid: Option<&NexusId>,
        allow_missing: bool,
    ) -> Result<Option<NexusInfo>, SvcError> {
        match nexus_uuid {
            None => Ok(None),
            Some(nexus_uuid) => {
                match self
                    .load_obj::<NexusInfo>(&NexusInfoKey::from(nexus_uuid))
                    .await
                {
                    Ok(mut info) => {
                        info.uuid = nexus_uuid.clone();
                        Ok(Some(info))
                    }
                    Err(SvcError::StoreMissingEntry { .. }) if allow_missing => Ok(None),
                    Err(error) => Err(error),
                }
            }
        }
    }
}
