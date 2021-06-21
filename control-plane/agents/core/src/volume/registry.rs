use crate::core::registry::Registry;
use common::errors::{SvcError, VolumeNotFound};
use mbus_api::v0::{Volume, VolumeId, VolumeState};
use snafu::OptionExt;

impl Registry {
    /// Get the volume status for the specified volume
    pub(crate) async fn get_volume_status(
        &self,
        volume_uuid: &VolumeId,
    ) -> Result<Volume, SvcError> {
        let nexuses = self.get_node_opt_nexuses(None).await?;
        let nexus_specs = self.specs.get_created_nexus_specs().await;
        let nexus_status = nexus_specs
            .iter()
            .filter(|n| n.owner.as_ref() == Some(volume_uuid))
            .map(|n| nexuses.iter().find(|nexus| nexus.uuid == n.uuid))
            .flatten()
            .collect::<Vec<_>>();
        let volume_spec = self
            .specs
            .get_volume(volume_uuid)
            .await
            .context(VolumeNotFound {
                vol_id: volume_uuid.to_string(),
            })?;
        let volume_spec = volume_spec.lock().await;

        Ok(if let Some(first_nexus_status) = nexus_status.get(0) {
            Volume {
                uuid: volume_uuid.to_owned(),
                size: first_nexus_status.size,
                state: first_nexus_status.state.clone(),
                protocol: first_nexus_status.share.clone(),
                children: nexus_status.iter().map(|&n| n.clone()).collect(),
            }
        } else {
            Volume {
                uuid: volume_uuid.to_owned(),
                size: volume_spec.size,
                state: if volume_spec.target_node.is_none() {
                    VolumeState::Online
                } else {
                    VolumeState::Unknown
                },
                protocol: volume_spec.protocol.clone(),
                children: vec![],
            }
        })
    }

    /// Get all volume status
    pub(super) async fn get_volumes_status(&self) -> Vec<Volume> {
        let mut volumes = vec![];
        let volume_specs = self.specs.get_volumes().await;
        for volume in volume_specs {
            if let Ok(status) = self.get_volume_status(&volume.uuid).await {
                volumes.push(status)
            }
        }
        volumes
    }
}
