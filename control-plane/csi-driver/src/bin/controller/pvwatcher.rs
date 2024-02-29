use crate::client::ListToken;
use futures::TryStreamExt;
use k8s_openapi::api::core::v1::{PersistentVolume, PersistentVolumeClaim};
use kube::{
    api::{Api, ListParams},
    runtime::{watcher, WatchStreamExt},
    Client, ResourceExt,
};
use tracing::{debug, error, info};

/// Struct for PV Garbage collector.
#[derive(Clone)]
pub(super) struct PvGarbageCollector {
    pv_handle: Api<PersistentVolume>,
    pvc_handle: Api<PersistentVolumeClaim>,
    orphan_period: Option<humantime::Duration>,
    rest_client: &'static crate::RestApiClient,
}

/// Methods implemented by PV Garbage Collector
impl PvGarbageCollector {
    /// Returns an instance of PV Garbage collector
    pub(crate) async fn new(orphan_period: Option<humantime::Duration>) -> anyhow::Result<Self> {
        let client = Client::try_default().await?;
        Ok(Self {
            pv_handle: Api::<PersistentVolume>::all(client.clone()),
            pvc_handle: Api::<PersistentVolumeClaim>::all(client),
            orphan_period,
            rest_client: crate::RestApiClient::get_client(),
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
        let volumes = self.collect_volume_ids().await;
        if volumes.is_empty() {
            return;
        }
        let Some(pvcs) = self.collect_pvc_ids().await else {
            return;
        };

        let mut gc_uids = Vec::with_capacity(volumes.len());
        for volume_uid in volumes {
            if self.is_vol_orphan(volume_uid, &pvcs).await {
                gc_uids.push(volume_uid);
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        for volume_uid in gc_uids {
            if self.is_vol_orphan(volume_uid, &pvcs).await {
                self.delete_volume(volume_uid).await;
            }
        }
    }

    /// Check if there are no PVC's or PV's for the given volume.
    async fn is_vol_orphan(&self, volume: uuid::Uuid, pvcs: &[String]) -> bool {
        let pvc_orphan = !pvcs.contains(&volume.to_string());
        pvc_orphan && self.is_vol_pv_orphaned(&volume).await
    }

    async fn collect_volume_ids(&self) -> Vec<uuid::Uuid> {
        let max_entries = 500i32;
        let mut starting_token = Some(0);
        let mut volume_ids = Vec::with_capacity(max_entries as usize);

        while let Some(token) = starting_token {
            match self
                .rest_client
                .list_volumes(max_entries, ListToken::Number(token))
                .await
            {
                Ok(volumes) => {
                    starting_token = volumes.next_token;
                    volume_ids.extend(volumes.entries.into_iter().map(|v| v.spec.uuid));
                }
                Err(error) => {
                    error!(?error, "Unable to list volumes");
                    break;
                }
            }
        }
        volume_ids
    }
    async fn collect_pvc_ids(&self) -> Option<Vec<String>> {
        let max_entries = 500i32;
        let mut pvc_ids = Vec::with_capacity(max_entries as usize);
        let mut params = ListParams::default().limit(max_entries as u32);

        loop {
            let list = self.pvc_handle.list_metadata(&params).await.ok()?;
            let pvcs = list.items.into_iter().filter_map(|p| p.uid());
            pvc_ids.extend(pvcs);
            match list.metadata.continue_ {
                Some(token) => {
                    params = params.continue_token(&token);
                }
                None => {
                    break;
                }
            }
        }
        Some(pvc_ids)
    }

    async fn is_vol_pv_orphaned(&self, volume_uuid: &uuid::Uuid) -> bool {
        let pv_name = format!("pvc-{volume_uuid}");
        matches!(self.pv_handle.get_opt(&pv_name).await, Ok(None))
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
            Ok(true) => info!("Successfully deleted the volume"),
            Ok(false) => debug!("The volume had already been deleted"),
            Err(error) => error!(?error, "Failed to delete the volume"),
        }
    }
}
