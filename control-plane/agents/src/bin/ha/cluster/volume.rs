use crate::{
    etcd::EtcdStore,
    nodes::NodeList,
    switchover::{SwitchOverEngine, SwitchOverRequest, SwitchOverStage},
};
use std::{convert::TryFrom, net::SocketAddr};
use stor_port::{
    transport_api::{ReplyError, ResourceKind},
    types::v0::transport::{NodeId, VolumeId},
};
use utils::nvme_target_nqn_prefix;

/// Defines spec for VolumeMover.
#[derive(Debug, Clone)]
pub(crate) struct VolumeMover {
    etcd: EtcdStore,
    engine: SwitchOverEngine,
}

impl VolumeMover {
    /// Create a new `Self`.
    pub(crate) fn new(
        etcd: EtcdStore,
        fast_requeue: Option<humantime::Duration>,
        nodes: NodeList,
    ) -> Self {
        let engine = SwitchOverEngine::new(etcd.clone(), fast_requeue, nodes);
        Self { engine, etcd }
    }

    /// Switchover build the switchover request for the given nqn and send it to SwitchOverEngine.
    #[tracing::instrument(level = "info", skip(self), err)]
    pub(crate) async fn switchover(
        &self,
        node: NodeId,
        uri: SocketAddr,
        nqn: String,
    ) -> Result<SwitchOverStage, ReplyError> {
        // todo: this won't work for nqn prefix upgrades
        let prefix = format!("{}:", nvme_target_nqn_prefix());
        let volume = nqn.strip_prefix(&prefix).ok_or_else(|| {
            ReplyError::invalid_argument(ResourceKind::NvmePath, "nqn", nqn.to_owned())
        })?;

        let volume_uuid = VolumeId::try_from(volume).map_err(|error| {
            ReplyError::invalid_argument(ResourceKind::Volume, "volume", error.to_string())
        })?;

        let req = SwitchOverRequest::new(uri, volume_uuid, node, nqn);
        let stage_arc = req.stage_arc();

        // calling start_op here to store the request in etcd
        req.start_op(&self.etcd).await.map_err(|error| {
            ReplyError::failed_persist(
                ResourceKind::NvmePath,
                error.to_string(),
                "Failed to start switchover request".into(),
            )
        })?;
        self.engine.enqueue(req);
        Ok(stage_arc)
    }

    /// Send batch of switchover request to SwitchOverEngine.
    #[tracing::instrument(level = "info", skip(self), err)]
    pub(crate) async fn send_switchover_req(
        &self,
        mut requests: Vec<SwitchOverRequest>,
    ) -> Result<(), anyhow::Error> {
        requests.sort();
        for entry in requests {
            self.engine.nodes().insert_failed_request(&entry).await;
            entry.start_op(&self.etcd).await?;
            self.engine.enqueue(entry);
        }
        Ok(())
    }
}
