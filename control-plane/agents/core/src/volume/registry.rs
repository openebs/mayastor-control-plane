use crate::core::registry::Registry;
use common::errors::{SvcError, VolumeNotFound};
use common_lib::types::v0::message_bus::{
    NexusStatus, Volume, VolumeId, VolumeState, VolumeStatus,
};

use snafu::OptionExt;

impl Registry {
    /// Get the volume state for the specified volume
    pub(crate) async fn get_volume_state(
        &self,
        volume_uuid: &VolumeId,
    ) -> Result<VolumeState, SvcError> {
        let replica_specs = self.specs().get_volume_replicas(volume_uuid);
        let volume_spec = self
            .specs()
            .get_locked_volume(volume_uuid)
            .context(VolumeNotFound {
                vol_id: volume_uuid.to_string(),
            })?;
        let volume_spec = volume_spec.lock().clone();
        let nexus_spec = self.specs().get_volume_target_nexus(&volume_spec);
        let nexus_state = match nexus_spec {
            None => None,
            Some(spec) => {
                let nexus_id = spec.lock().uuid.clone();
                self.get_nexus(&nexus_id).await.ok()
            }
        };

        Ok(if let Some(nexus_state) = nexus_state {
            VolumeState {
                uuid: volume_uuid.to_owned(),
                size: nexus_state.size,
                status: match nexus_state.status {
                    NexusStatus::Online
                        if nexus_state.children.len() != volume_spec.num_replicas as usize =>
                    {
                        VolumeStatus::Degraded
                    }
                    _ => nexus_state.status.clone(),
                },
                protocol: nexus_state.share,
                child: Some(nexus_state),
            }
        } else {
            VolumeState {
                uuid: volume_uuid.to_owned(),
                size: volume_spec.size,
                status: if volume_spec.target.is_none() {
                    if replica_specs.len() >= volume_spec.num_replicas as usize {
                        VolumeStatus::Online
                    } else if replica_specs.is_empty() {
                        VolumeStatus::Faulted
                    } else {
                        VolumeStatus::Degraded
                    }
                } else {
                    VolumeStatus::Unknown
                },
                protocol: volume_spec.protocol,
                child: None,
            }
        })
    }
    /// Get all volumes
    pub(super) async fn get_volumes(&self) -> Vec<Volume> {
        let mut volumes = vec![];
        let volume_specs = self.specs().get_volumes();
        for spec in &volume_specs {
            volumes.push(Volume::new(
                spec,
                &self.get_volume_state(&spec.uuid).await.ok(),
            ));
        }
        volumes
    }

    /// Return a volume object corresponding to the ID.
    pub(crate) async fn get_volume(&self, id: &VolumeId) -> Result<Volume, SvcError> {
        Ok(Volume::new(
            &self.specs().get_volume(id)?,
            &self.get_volume_state(id).await.ok(),
        ))
    }
}
