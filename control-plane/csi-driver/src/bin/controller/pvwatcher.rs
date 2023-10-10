use futures::TryStreamExt;
use k8s_openapi::api::core::v1::PersistentVolume;
use kube::{
    api::Api,
    runtime::{watcher, WatchStreamExt},
    Client, ResourceExt,
};
use tracing::{debug, error, info};
use uuid::Uuid;

use csi_driver::CSI_PLUGIN_NAME;

use crate::IoEngineApiClient;

/// Struct for PV Garbage collector
#[derive(Clone)]
pub(crate) struct PvGarbageCollector {
    pub(crate) pv_handle: Api<PersistentVolume>,
}

/// Methods implemented by PV Garbage Collector
impl PvGarbageCollector {
    /// Returns an instance of PV Garbage collector
    pub(crate) async fn new() -> anyhow::Result<Self> {
        let client = Client::try_default().await?;
        Ok(Self {
            pv_handle: Api::<PersistentVolume>::all(client),
        })
    }
    /// Starts watching PV events
    pub(crate) async fn run_watcher(&self) {
        info!("Starting PV Garbage Collector");
        let cloned_self = self.clone();
        tokio::spawn(async move {
            cloned_self.handle_missed_events().await;
        });
        watcher(self.pv_handle.clone(), watcher::Config::default())
            .touched_objects()
            .try_for_each(|pvol| async {
                self.process_object(pvol).await;
                Ok(())
            })
            .await
            .expect("Watcher unexpectedly terminated");
    }

    async fn process_object(&self, pv: PersistentVolume) {
        if pv.metadata.clone().deletion_timestamp.is_none() {
            return;
        }
        if let Some(provisioner) = &pv.spec.as_ref().unwrap().csi {
            if provisioner.driver != CSI_PLUGIN_NAME {
                return;
            }
        }

        if let Some(reclaim_policy) = &pv.spec.as_ref().unwrap().persistent_volume_reclaim_policy {
            if let Some(phase) = &pv.status.as_ref().unwrap().phase {
                if reclaim_policy == "Retain" && phase == "Released" {
                    debug!(pv.name = pv.name_any(), "PV is a deletion candidate");
                    if let Some(provisioner) = &pv.spec.as_ref().unwrap().csi {
                        delete_volume(&provisioner.volume_handle.to_string()).await;
                    }
                }
                if phase == "Bound" {
                    match self.pv_handle.get_opt(&pv.name_any()).await {
                        Ok(pvol) => match pvol {
                            Some(_) => debug!(pv.name = pv.name_any(), "PV present on API server"),
                            None => {
                                debug!(pv.name = pv.name_any(), "PV is a deletion candidate");
                                if let Some(provisioner) = &pv.spec.as_ref().unwrap().csi {
                                    delete_volume(&provisioner.volume_handle.to_string()).await;
                                }
                            }
                        },
                        Err(error) => error!(%error, "Error while verifying if PV is present"),
                    }
                }
            }
        }
    }

    /// Handle if there is any missed events at startup.
    async fn handle_missed_events(&self) {
        debug!("Handling if any missed events");
        match IoEngineApiClient::get_client()
            .list_volumes(0, "".to_string())
            .await
        {
            Ok(volume_list) => {
                for vol in volume_list.entries {
                    let pv = "pvc-".to_string() + &vol.spec.uuid.to_string();
                    if let Ok(pvol) = self.pv_handle.get_opt(&pv).await {
                        match pvol {
                            Some(_) => {}
                            None => {
                                debug!(pv.name = pv, "PV is a deletion candidate");
                                let vol_handle = &vol.spec.uuid.to_string();
                                delete_volume(vol_handle).await;
                            }
                        }
                    }
                }
            }
            Err(error) => error!(?error, "Unable to list volumes"),
        }
    }
}

/// Accepts volume id and calls Control plane api to delete the Volume
async fn delete_volume(vol_handle: &str) {
    let volume_uuid = Uuid::parse_str(vol_handle).unwrap();
    match IoEngineApiClient::get_client()
        .delete_volume(&volume_uuid)
        .await
    {
        Ok(_) => info!(volume.uuid = %volume_uuid, "Successfully deleted volume"),
        Err(error) => error!(?error, volume.uuid = %volume_uuid, "Failed to delete volume"),
    }
}
