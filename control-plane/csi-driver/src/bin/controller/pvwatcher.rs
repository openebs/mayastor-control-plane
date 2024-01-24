use crate::client::ListToken;
use futures::TryStreamExt;
use k8s_openapi::api::core::v1::PersistentVolume;
use kube::{
    api::Api,
    runtime::{watcher, WatchStreamExt},
    Client, ResourceExt,
};
use tracing::{debug, error, info};

/// Struct for PV Garbage collector
#[derive(Clone)]
pub(crate) struct PvGarbageCollector {
    pub(crate) pv_handle: Api<PersistentVolume>,
    orphan_period: Option<humantime::Duration>,
    rest_client: &'static crate::IoEngineApiClient,
}

/// Methods implemented by PV Garbage Collector
impl PvGarbageCollector {
    /// Returns an instance of PV Garbage collector
    pub(crate) async fn new(orphan_period: Option<humantime::Duration>) -> anyhow::Result<Self> {
        let client = Client::try_default().await?;
        Ok(Self {
            pv_handle: Api::<PersistentVolume>::all(client),
            orphan_period,
            rest_client: crate::IoEngineApiClient::get_client(),
        })
    }
    /// Starts watching PV events.
    pub(crate) async fn run_watcher(&self) {
        tokio::spawn(self.clone().orphan_volumes_watcher());
        info!("Starting PV Garbage Collector");
        watcher(self.pv_handle.clone(), watcher::Config::default())
            .touched_objects()
            .try_for_each(|pvol| async {
                self.process_object(pvol).await;
                Ok(())
            })
            .await
            .expect("Watcher unexpectedly terminated");
    }

    async fn process_object(&self, pv: PersistentVolume) -> Option<()> {
        let pv_name = pv.name_any();

        pv.metadata.deletion_timestamp?;

        let pv_spec = pv.spec?;
        let volume = pv_spec.csi?;
        if volume.driver != csi_driver::CSI_PLUGIN_NAME {
            return None;
        }
        let volume_uuid = uuid::Uuid::parse_str(&volume.volume_handle).ok()?;
        let reclaim_policy = pv_spec.persistent_volume_reclaim_policy?;
        let phase = pv.status?.phase?;

        if reclaim_policy == "Retain" && phase == "Released" {
            info!(
                pv.name = pv_name,
                pv.reclaim_policy = reclaim_policy,
                pv.phase = phase,
                "PV is a deletion candidate"
            );
            self.delete_volume(volume_uuid).await;
        } else if phase == "Bound" {
            match self.pv_handle.get_opt(&pv_name).await {
                Ok(pvol) => match pvol {
                    Some(_) => debug!(pv.name = pv_name, "PV present on API server"),
                    None => {
                        info!(
                            pv.name = pv_name,
                            pv.reclaim_policy = reclaim_policy,
                            pv.phase = phase,
                            "PV is a deletion candidate"
                        );
                        self.delete_volume(volume_uuid).await;
                    }
                },
                Err(error) => error!(%error, "Error while verifying if PV is present"),
            }
        }

        Some(())
    }

    async fn orphan_volumes_watcher(self) {
        info!("Starting Orphaned Volumes Garbage Collector");
        let Some(period) = self.orphan_period else {
            return self.delete_orphan_volumes().await;
        };
        let mut ticker = tokio::time::interval(period.into());
        loop {
            ticker.tick().await;
            self.delete_orphan_volumes().await;
        }
    }

    /// Deletes orphaned volumes (ie volumes with no corresponding PV) which can be useful:
    /// 1. if there is any missed events at startup
    /// 2. to tackle k8s bug where volumes are leaked when PV deletion is attempted before
    /// PVC deletion.
    async fn delete_orphan_volumes(&self) {
        let max_entries = 200;
        let mut starting_token = Some(0);
        while let Some(token) = starting_token {
            match self
                .rest_client
                .list_volumes(max_entries, ListToken::Number(token))
                .await
            {
                Ok(volumes) => {
                    starting_token = volumes.next_token;
                    for vol in volumes.entries {
                        if self.is_vol_orphaned(&vol.spec.uuid).await {
                            self.delete_volume(vol.spec.uuid).await;
                        }
                    }
                }
                Err(error) => {
                    error!(?error, "Unable to list volumes");
                    return;
                }
            }
        }
    }

    async fn is_vol_orphaned(&self, volume_uuid: &uuid::Uuid) -> bool {
        let pv_name = format!("pvc-{volume_uuid}");
        if let Ok(None) = self.pv_handle.get_opt(&pv_name).await {
            debug!(pv.name = pv_name, "PV is a deletion candidate");
            true
        } else {
            false
        }
    }

    /// Accepts volume id and calls Control plane api to delete the Volume.
    #[tracing::instrument(level = "info", skip(self, volume_uuid), fields(volume.uuid = %volume_uuid))]
    async fn delete_volume(&self, volume_uuid: uuid::Uuid) {
        // this shouldn't happy but to be safe, ensure we don't bump heads with the provisioning
        let Ok(_guard) = csi_driver::limiter::VolumeOpGuard::new(volume_uuid) else {
            error!("Volume cannot be deleted as it's in use within the csi-controller plugin");
            return;
        };
        match self.rest_client.delete_volume(&volume_uuid).await {
            Ok(_) => info!("Successfully deleted the volume"),
            Err(error) => error!(?error, "Failed to delete the volume"),
        }
    }
}
