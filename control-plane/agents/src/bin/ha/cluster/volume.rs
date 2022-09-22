use crate::{
    etcd::EtcdStore,
    nodes::NodeList,
    switchover::{Stage, SwitchOverEngine, SwitchOverRequest},
};
use common_lib::types::v0::transport::VolumeId;
use std::convert::TryFrom;
use utils::NVME_TARGET_NQN_PREFIX;

/// Defines spec for VolumeMover.
#[derive(Debug, Clone)]
pub struct VolumeMover {
    etcd: EtcdStore,
    engine: SwitchOverEngine,
}

impl VolumeMover {
    pub fn new(etcd: EtcdStore, nodes: NodeList) -> Self {
        let sw = SwitchOverEngine::new(etcd.clone(), nodes);

        Self { engine: sw, etcd }
    }

    /// Switchover build the switchover request for the given nqn and send it to SwitchOverEngine.
    #[tracing::instrument(level = "info", skip(self), err)]
    pub async fn switchover(&self, uri: String, nqn: String) -> Result<(), anyhow::Error> {
        if !nqn.starts_with(NVME_TARGET_NQN_PREFIX) {
            return Err(anyhow::anyhow!("Invalid nqn"));
        }

        let volume = nqn
            .strip_prefix(NVME_TARGET_NQN_PREFIX)
            .ok_or_else(|| anyhow::anyhow!("Failed to parse volume UUID"))?;

        let volume_uuid = VolumeId::try_from(volume)?;

        let req = SwitchOverRequest::new(uri, volume_uuid);

        // calling start_op here to store the request in etcd
        req.start_op(Stage::Init, &self.etcd).await?;
        self.engine.initiate(req);
        Ok(())
    }

    /// Send batch of switchover request to SwitchOverEngine.
    #[tracing::instrument(level = "info", skip(self), err)]
    pub async fn send_switchover_req(
        &self,
        mut req: Vec<SwitchOverRequest>,
    ) -> Result<(), anyhow::Error> {
        req.sort_by_key(|r| r.timestamp());

        for entry in req {
            entry.start_op(entry.stage(), &self.etcd).await?;
            self.engine.initiate(entry);
        }
        Ok(())
    }
}
