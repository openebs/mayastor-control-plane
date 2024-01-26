use agents::errors::SvcError;

use stor_port::types::v0::store::node::{DrainingVolumes, NodeOperation, NodeSpec};

use crate::controller::{
    registry::Registry,
    resources::{
        operations::{ResourceCordon, ResourceDrain, ResourceLabel},
        operations_helper::GuardedOperationsHelper,
        OperationGuardArc,
    },
};
use std::collections::HashMap;

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

        self.complete_update(registry, Ok(()), spec_clone).await?;
        Ok(self.as_ref().clone())
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

        self.complete_update(registry, Ok(()), spec_clone).await?;
        Ok(self.as_ref().clone())
    }
}

/// Resource Label Operations.
#[async_trait::async_trait]
impl ResourceLabel for OperationGuardArc<NodeSpec> {
    type LabelOutput = NodeSpec;
    type UnlabelOutput = NodeSpec;

    /// Label a node via operation guard functions.
    async fn label(
        &mut self,
        registry: &Registry,
        label: HashMap<String, String>,
        overwrite: bool,
    ) -> Result<Self::LabelOutput, SvcError> {
        let cloned_node_spec = self.lock().clone();
        let spec_clone = self
            .start_update(
                registry,
                &cloned_node_spec,
                NodeOperation::Label(label, overwrite),
            )
            .await?;

        self.complete_update(registry, Ok(()), spec_clone).await?;
        Ok(self.as_ref().clone())
    }

    /// Unlabel a node via operation guard functions.
    async fn unlabel(
        &mut self,
        registry: &Registry,
        label: String,
    ) -> Result<Self::UnlabelOutput, SvcError> {
        let cloned_node_spec = self.lock().clone();
        let spec_clone = self
            .start_update(registry, &cloned_node_spec, NodeOperation::Unlabel(label))
            .await?;

        self.complete_update(registry, Ok(()), spec_clone).await?;
        Ok(self.as_ref().clone())
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

        self.complete_update(registry, Ok(()), spec_clone).await?;
        Ok(self.as_ref().clone())
    }

    /// Mark a node as drained via operation guard functions.
    async fn set_drained(&mut self, registry: &Registry) -> Result<Self::DrainOutput, SvcError> {
        let cloned_node_spec = self.lock().clone();
        let spec_clone = self
            .start_update(registry, &cloned_node_spec, NodeOperation::SetDrained())
            .await?;

        self.complete_update(registry, Ok(()), spec_clone).await?;
        Ok(self.as_ref().clone())
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

        self.complete_update(registry, Ok(()), spec_clone).await?;
        Ok(self.as_ref().clone())
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

        self.complete_update(registry, Ok(()), spec_clone).await?;
        Ok(self.as_ref().clone())
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

        self.complete_update(registry, Ok(()), spec_clone).await?;
        Ok(self.as_ref().clone())
    }
}
