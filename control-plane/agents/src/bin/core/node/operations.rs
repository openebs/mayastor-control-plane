use agents::errors::SvcError;

use stor_port::types::v0::store::node::{DrainingVolumes, NodeOperation, NodeSpec};

use crate::controller::{
    registry::Registry,
    resources::{
        operations::{ResourceCordon, ResourceDrain},
        operations_helper::GuardedOperationsHelper,
        OperationGuardArc,
    },
};

/// Resource Cordon Operations.
#[async_trait::async_trait]
impl ResourceCordon for OperationGuardArc<NodeSpec> {
    type CordonOutput = NodeSpec;
    type UncordonOutput = NodeSpec;

    /// Cordon a node via operation guard functions.
    async fn cordon(
        &mut self,
        registry: &Registry,
        label: String,
    ) -> Result<Self::CordonOutput, SvcError> {
        let cloned_node_spec = self.lock().clone();
        let spec_clone = self
            .start_update(registry, &cloned_node_spec, NodeOperation::Cordon(label))
            .await?;

        self.complete_update(registry, Ok(self.as_ref().clone()), spec_clone)
            .await
    }

    /// Uncordon a node via operation guard functions.
    async fn uncordon(
        &mut self,
        registry: &Registry,
        label: String,
    ) -> Result<Self::UncordonOutput, SvcError> {
        let cloned_node_spec = self.lock().clone();
        let spec_clone = self
            .start_update(registry, &cloned_node_spec, NodeOperation::Uncordon(label))
            .await?;

        self.complete_update(registry, Ok(self.as_ref().clone()), spec_clone)
            .await
    }
}

/// Resource Drain Operations.
#[async_trait::async_trait]
impl ResourceDrain for OperationGuardArc<NodeSpec> {
    type DrainOutput = NodeSpec;

    /// Drain a node via operation guard functions.
    async fn drain(
        &mut self,
        registry: &Registry,
        label: String,
    ) -> Result<Self::DrainOutput, SvcError> {
        let cloned_node_spec = self.lock().clone();
        let spec_clone = self
            .start_update(registry, &cloned_node_spec, NodeOperation::Drain(label))
            .await?;

        self.complete_update(registry, Ok(self.as_ref().clone()), spec_clone)
            .await
    }

    /// Mark a node as drained via operation guard functions.
    async fn set_drained(&mut self, registry: &Registry) -> Result<Self::DrainOutput, SvcError> {
        let cloned_node_spec = self.lock().clone();
        let spec_clone = self
            .start_update(registry, &cloned_node_spec, NodeOperation::SetDrained())
            .await?;

        self.complete_update(registry, Ok(self.as_ref().clone()), spec_clone)
            .await
    }
}

/// Node drain Operations.
impl OperationGuardArc<NodeSpec> {
    /// Drain the set of draining volumes to the stored set.
    pub(crate) async fn add_draining_volumes(
        &mut self,
        registry: &Registry,
        volumes: DrainingVolumes,
    ) -> Result<NodeSpec, SvcError> {
        let cloned_node_spec = self.lock().clone();
        let spec_clone = self
            .start_update(
                registry,
                &cloned_node_spec,
                NodeOperation::AddDrainingVolumes(volumes),
            )
            .await?;

        self.complete_update(registry, Ok(self.as_ref().clone()), spec_clone)
            .await
    }

    /// Remove the set of draining volumes from the stored set.
    pub(crate) async fn remove_draining_volumes(
        &mut self,
        registry: &Registry,
        volumes: DrainingVolumes,
    ) -> Result<NodeSpec, SvcError> {
        let cloned_node_spec = self.lock().clone();
        let spec_clone = self
            .start_update(
                registry,
                &cloned_node_spec,
                NodeOperation::RemoveDrainingVolumes(volumes),
            )
            .await?;

        self.complete_update(registry, Ok(self.as_ref().clone()), spec_clone)
            .await
    }

    /// Remove all draining volumes from the stored set.
    pub(crate) async fn remove_all_draining_volumes(
        &mut self,
        registry: &Registry,
    ) -> Result<NodeSpec, SvcError> {
        let cloned_node_spec = self.lock().clone();
        let spec_clone = self
            .start_update(
                registry,
                &cloned_node_spec,
                NodeOperation::RemoveAllDrainingVolumes(),
            )
            .await?;

        self.complete_update(registry, Ok(self.as_ref().clone()), spec_clone)
            .await
    }
}
