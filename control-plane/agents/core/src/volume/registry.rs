use crate::core::registry::Registry;
use common::errors::{SvcError, VolumeNotFound};
use common_lib::types::v0::message_bus::{Volume, VolumeId, VolumeState, VolumeStatus};
use snafu::OptionExt;

impl Registry {
    /// Get the volume state for the specified volume
    pub(crate) async fn get_volume_state(
        &self,
        volume_uuid: &VolumeId,
    ) -> Result<VolumeState, SvcError> {
        let nexuses = self.get_node_opt_nexuses(None).await?;
        let nexus_specs = self.specs.get_volume_nexuses(volume_uuid);
        let nexus_states = nexus_specs
            .iter()
            .map(|n| nexuses.iter().find(|nexus| nexus.uuid == n.lock().uuid))
            .flatten()
            .collect::<Vec<_>>();

        let volume_spec = self
            .specs
            .get_locked_volume(volume_uuid)
            .context(VolumeNotFound {
                vol_id: volume_uuid.to_string(),
            })?;
        let volume_spec = volume_spec.lock();

        Ok(if let Some(first_nexus_state) = nexus_states.get(0) {
            VolumeState {
                uuid: volume_uuid.to_owned(),
                size: first_nexus_state.size,
                status: first_nexus_state.status.clone(),
                protocol: first_nexus_state.share.clone(),
                children: nexus_states.iter().map(|&n| n.clone()).collect(),
            }
        } else {
            VolumeState {
                uuid: volume_uuid.to_owned(),
                size: volume_spec.size,
                status: if volume_spec.target_node.is_none() {
                    VolumeStatus::Online
                } else {
                    VolumeStatus::Unknown
                },
                protocol: volume_spec.protocol.clone(),
                children: vec![],
            }
        })
    }
    /// Get all volumes
    pub(super) async fn get_volumes(&self) -> Vec<Volume> {
        let mut volumes = vec![];
        let volume_specs = self.specs.get_volumes();
        for spec in &volume_specs {
            volumes.push(Volume::new(
                spec,
                &self.get_volume_state(&spec.uuid).await.ok(),
            ));
        }
        volumes
    }

    /// Create and return a volume object corresponding to the ID.
    pub(crate) async fn get_volume(&self, id: &VolumeId) -> Result<Volume, SvcError> {
        Ok(Volume::new(
            &self.specs.get_volume(id)?,
            &self.get_volume_state(id).await.ok(),
        ))
    }
}
