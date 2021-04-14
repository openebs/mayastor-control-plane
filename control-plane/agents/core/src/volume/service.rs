use crate::core::{registry::Registry, wrapper::ClientOps};
use common::errors::{NodeNotFound, SvcError};
use mbus_api::v0::{
    AddNexusChild,
    Child,
    CreateNexus,
    CreateVolume,
    DestroyNexus,
    DestroyVolume,
    Filter,
    GetNexuses,
    GetVolumes,
    Nexus,
    Nexuses,
    RemoveNexusChild,
    ShareNexus,
    UnshareNexus,
    Volume,
    Volumes,
};
use snafu::OptionExt;

#[derive(Debug, Clone)]
pub(super) struct Service {
    registry: Registry,
}

impl Service {
    pub(super) fn new(registry: Registry) -> Self {
        Self {
            registry,
        }
    }

    /// Get nexuses according to the filter
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn get_nexuses(
        &self,
        request: &GetNexuses,
    ) -> Result<Nexuses, SvcError> {
        let filter = request.filter.clone();
        let mut nexuses = match filter {
            Filter::None => self.registry.get_node_opt_nexuses(None).await?,
            Filter::Node(node_id) => {
                self.registry.get_node_nexuses(&node_id).await?
            }
            Filter::NodeNexus(node_id, nexus_id) => {
                let nexus =
                    self.registry.get_node_nexus(&node_id, &nexus_id).await?;
                vec![nexus]
            }
            Filter::Nexus(nexus_id) => {
                let nexus = self.registry.get_nexus(&nexus_id).await?;
                vec![nexus]
            }
            _ => {
                return Err(SvcError::InvalidFilter {
                    filter,
                })
            }
        };
        let nexus_specs = self.registry.specs.get_created_nexus_specs().await;
        nexus_specs.iter().for_each(|spec| {
            // if we can't find a nexus state, then report the nexus with
            // unknown state
            if !nexuses.iter().any(|r| r.uuid == spec.uuid) {
                nexuses.push(Nexus::from(spec));
            }
        });
        Ok(Nexuses(nexuses))
    }

    /// Create nexus
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn create_nexus(
        &self,
        request: &CreateNexus,
    ) -> Result<Nexus, SvcError> {
        self.registry
            .specs
            .create_nexus(&self.registry, request)
            .await
    }

    /// Destroy nexus
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn destroy_nexus(
        &self,
        request: &DestroyNexus,
    ) -> Result<(), SvcError> {
        self.registry
            .specs
            .destroy_nexus(&self.registry, request, true)
            .await
    }

    /// Share nexus
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn share_nexus(
        &self,
        request: &ShareNexus,
    ) -> Result<String, SvcError> {
        self.registry
            .specs
            .share_nexus(&self.registry, request)
            .await
    }

    /// Unshare nexus
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn unshare_nexus(
        &self,
        request: &UnshareNexus,
    ) -> Result<(), SvcError> {
        self.registry
            .specs
            .unshare_nexus(&self.registry, request)
            .await
    }

    /// Add nexus child
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn add_nexus_child(
        &self,
        request: &AddNexusChild,
    ) -> Result<Child, SvcError> {
        let node = self
            .registry
            .get_node_wrapper(&request.node)
            .await
            .context(NodeNotFound {
                node_id: request.node.clone(),
            })?;
        node.add_child(request).await
    }

    /// Remove nexus child
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn remove_nexus_child(
        &self,
        request: &RemoveNexusChild,
    ) -> Result<(), SvcError> {
        let node = self
            .registry
            .get_node_wrapper(&request.node)
            .await
            .context(NodeNotFound {
                node_id: request.node.clone(),
            })?;
        node.remove_child(request).await
    }

    /// Get volumes
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn get_volumes(
        &self,
        request: &GetVolumes,
    ) -> Result<Volumes, SvcError> {
        let nexuses = self.get_nexuses(&Default::default()).await?.0;
        let nexus_specs = self.registry.specs.get_created_nexus_specs().await;
        let volumes = nexuses
            .iter()
            .map(|n| {
                let uuid = nexus_specs
                    .iter()
                    .find(|nn| nn.uuid == n.uuid)
                    .map(|nn| nn.owner.clone())
                    .flatten();
                if let Some(uuid) = uuid {
                    Some(Volume {
                        uuid,
                        size: n.size,
                        state: n.state.clone(),
                        children: vec![n.clone()],
                    })
                } else {
                    None
                }
            })
            .flatten()
            .collect::<Vec<_>>();

        let volumes = match &request.filter {
            Filter::None => volumes,
            Filter::NodeVolume(node, volume) => volumes
                .iter()
                .filter(|v| {
                    v.children.iter().any(|c| &c.node == node)
                        && &v.uuid == volume
                })
                .cloned()
                .collect(),
            Filter::Volume(volume) => volumes
                .iter()
                .filter(|v| &v.uuid == volume)
                .cloned()
                .collect(),
            filter => {
                return Err(SvcError::InvalidFilter {
                    filter: filter.clone(),
                })
            }
        };
        Ok(Volumes(volumes))
    }

    /// Create volume
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn create_volume(
        &self,
        request: &CreateVolume,
    ) -> Result<Volume, SvcError> {
        self.registry
            .specs
            .create_volume(&self.registry, request)
            .await
    }

    /// Destroy volume
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn destroy_volume(
        &self,
        request: &DestroyVolume,
    ) -> Result<(), SvcError> {
        self.registry
            .specs
            .destroy_volume(&self.registry, request)
            .await
    }
}
