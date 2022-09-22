use crate::controller::{
    registry::Registry,
    resources::{
        operations::{
            ResourceLifecycle, ResourceOffspring, ResourceSharing, ResourceShutdownOperations,
        },
        operations_helper::{GuardedOperationsHelper, OperationSequenceGuard},
        OperationGuardArc,
    },
    wrapper::{ClientOps, GetterOps},
};
use agents::errors::{SvcError, SvcError::CordonedNode};
use common_lib::types::v0::{
    store::{
        nexus::{NexusOperation, NexusSpec},
        nexus_child::NexusChild,
    },
    transport::{
        child::Child,
        nexus::{CreateNexus, DestroyNexus, Nexus, ShareNexus, UnshareNexus},
        AddNexusChild, RemoveNexusChild, ShutdownNexus,
    },
};

#[async_trait::async_trait]
impl ResourceLifecycle for OperationGuardArc<NexusSpec> {
    type Create = CreateNexus;
    type CreateOutput = (Self, Nexus);
    type Destroy = DestroyNexus;

    async fn create(
        registry: &Registry,
        request: &Self::Create,
    ) -> Result<Self::CreateOutput, SvcError> {
        let specs = registry.specs();

        if registry.node_cordoned(&request.node)? {
            return Err(CordonedNode {
                node_id: request.node.to_string(),
            });
        }

        let node = registry.get_node_wrapper(&request.node).await?;

        let nexus = specs
            .get_or_create_nexus(request)
            .operation_guard_wait()
            .await?;
        let _ = nexus.start_create(registry, request).await?;

        let result = node.create_nexus(request).await;
        specs.on_create_set_owners(request, &nexus, &result);

        let nexus_state = nexus.complete_create(result, registry).await?;
        Ok((nexus, nexus_state))
    }

    async fn destroy(
        &mut self,
        registry: &Registry,
        request: &Self::Destroy,
    ) -> Result<(), SvcError> {
        Some(self).destroy(registry, request).await
    }
}

#[async_trait::async_trait]
impl ResourceLifecycle for Option<&mut OperationGuardArc<NexusSpec>> {
    type Create = CreateNexus;
    type CreateOutput = Nexus;
    type Destroy = DestroyNexus;

    async fn create(
        _registry: &Registry,
        _request: &Self::Create,
    ) -> Result<Self::CreateOutput, SvcError> {
        unimplemented!()
    }

    async fn destroy(
        &mut self,
        registry: &Registry,
        request: &Self::Destroy,
    ) -> Result<(), SvcError> {
        let node = registry.get_node_wrapper(&request.node).await?;

        if let Some(nexus) = self {
            nexus
                .start_destroy_by(registry, request.disowners())
                .await?;

            let result = node.destroy_nexus(request).await;
            registry.specs().on_delete_disown_replicas(nexus);
            nexus.complete_destroy(result, registry).await
        } else {
            node.destroy_nexus(request).await
        }
    }
}

#[async_trait::async_trait]
impl ResourceSharing for OperationGuardArc<NexusSpec> {
    type Share = ShareNexus;
    type ShareOutput = String;
    type Unshare = UnshareNexus;
    type UnshareOutput = ();

    async fn share(
        &mut self,
        registry: &Registry,
        request: &Self::Share,
    ) -> Result<Self::ShareOutput, SvcError> {
        Some(self).share(registry, request).await
    }

    async fn unshare(
        &mut self,
        registry: &Registry,
        request: &Self::Unshare,
    ) -> Result<Self::UnshareOutput, SvcError> {
        Some(self).unshare(registry, request).await
    }
}

#[async_trait::async_trait]
impl ResourceSharing for Option<&mut OperationGuardArc<NexusSpec>> {
    type Share = ShareNexus;
    type ShareOutput = String;
    type Unshare = UnshareNexus;
    type UnshareOutput = ();

    async fn share(
        &mut self,
        registry: &Registry,
        request: &Self::Share,
    ) -> Result<Self::ShareOutput, SvcError> {
        let node = registry.get_node_wrapper(&request.node).await?;

        if let Some(nexus) = self {
            let status = registry.get_nexus(&request.uuid).await?;
            let spec_clone = nexus
                .start_update(registry, &status, NexusOperation::Share(request.protocol))
                .await?;

            let result = node.share_nexus(request).await;
            nexus.complete_update(registry, result, spec_clone).await
        } else {
            node.share_nexus(request).await
        }
    }

    async fn unshare(
        &mut self,
        registry: &Registry,
        request: &Self::Unshare,
    ) -> Result<Self::UnshareOutput, SvcError> {
        let node = registry.get_node_wrapper(&request.node).await?;

        if let Some(nexus) = self {
            let status = registry.get_nexus(&request.uuid).await?;
            let spec_clone = nexus
                .start_update(registry, &status, NexusOperation::Unshare)
                .await?;

            let result = node.unshare_nexus(request).await;
            nexus.complete_update(registry, result, spec_clone).await
        } else {
            node.unshare_nexus(request).await
        }
    }
}

#[async_trait::async_trait]
impl ResourceOffspring for OperationGuardArc<NexusSpec> {
    type Add = AddNexusChild;
    type AddOutput = Child;
    type Remove = RemoveNexusChild;

    async fn add_child(
        &mut self,
        registry: &Registry,
        request: &Self::Add,
    ) -> Result<Self::AddOutput, SvcError> {
        Some(self).add_child(registry, request).await
    }

    async fn remove_child(
        &mut self,
        registry: &Registry,
        request: &Self::Remove,
    ) -> Result<(), SvcError> {
        Some(self).remove_child(registry, request).await
    }
}

#[async_trait::async_trait]
impl ResourceOffspring for Option<&mut OperationGuardArc<NexusSpec>> {
    type Add = AddNexusChild;
    type AddOutput = Child;
    type Remove = RemoveNexusChild;

    async fn add_child(
        &mut self,
        registry: &Registry,
        request: &Self::Add,
    ) -> Result<Self::AddOutput, SvcError> {
        let node = registry.get_node_wrapper(&request.node).await?;

        if let Some(nexus) = self {
            let status = registry.get_nexus(&request.nexus).await?;
            let spec_clone = nexus
                .start_update(
                    registry,
                    &status,
                    NexusOperation::AddChild(NexusChild::from(&request.uri)),
                )
                .await?;

            let result = node.add_child(request).await;
            nexus.complete_update(registry, result, spec_clone).await
        } else {
            node.add_child(request).await
        }
    }

    async fn remove_child(
        &mut self,
        registry: &Registry,
        request: &Self::Remove,
    ) -> Result<(), SvcError> {
        let node = registry.get_node_wrapper(&request.node).await?;

        if let Some(nexus) = self {
            let status = registry.get_nexus(&request.nexus).await?;
            let spec_clone = nexus
                .start_update(
                    registry,
                    &status,
                    NexusOperation::RemoveChild(NexusChild::from(&request.uri)),
                )
                .await?;

            let result = node.remove_child(request).await;
            nexus.complete_update(registry, result, spec_clone).await
        } else {
            node.remove_child(request).await
        }
    }
}

#[async_trait::async_trait]
impl ResourceShutdownOperations for OperationGuardArc<NexusSpec> {
    type RemoveShutdownTargets = ();
    type Shutdown = ShutdownNexus;

    async fn shutdown(
        &mut self,
        registry: &Registry,
        request: &Self::Shutdown,
    ) -> Result<(), SvcError> {
        let node_id = self.as_ref().node.clone();
        let nexus_id = self.uuid();
        let node = registry.get_node_wrapper(&node_id).await?;
        let nexus_state = match node.nexus(nexus_id).await {
            None => self.as_ref().into(),
            Some(state) => state,
        };
        let spec_clone = self
            .start_update(registry, &nexus_state, NexusOperation::Shutdown)
            .await?;

        let result = node.shutdown_nexus(request).await;
        self.complete_update(registry, result, spec_clone).await?;
        Ok(())
    }

    async fn remove_shutdown_targets(
        &mut self,
        _registry: &Registry,
        _request: &Self::RemoveShutdownTargets,
    ) -> Result<(), SvcError> {
        // Not applicable for nexus
        unimplemented!()
    }
}
