use agents::errors::SvcError;

use stor_port::types::v0::{
    store::node::{DrainingVolumes, NodeOperation, NodeSpec},
    transport::{DestroyNode, DestroyPool, NodeDeleteResult, NodeId, NodeStatus},
};

use crate::controller::{
    registry::Registry,
    resources::{
        operations::{ResourceCordon, ResourceDrain, ResourceLabel, ResourceLifecycle},
        operations_helper::{analyze_snapshot_loss, analyze_volume_loss, GuardedOperationsHelper},
        OperationGuardArc,
    },
};
use grpc::operations::pool::traits::PoolCordonRequest;
use std::collections::{HashMap, HashSet};

/// Resource Cordon Operations.
#[async_trait::async_trait]
impl ResourceCordon for OperationGuardArc<NodeSpec> {
    type CordonOutput = NodeSpec;
    type UncordonOutput = NodeSpec;
    type Request = String;

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
                NodeOperation::Label((label, overwrite).into()),
            )
            .await?;

        self.complete_update(registry, Ok(()), spec_clone).await?;
        Ok(self.as_ref().clone())
    }

    /// Unlabel a node via operation guard functions.
    async fn unlabel(
        &mut self,
        registry: &Registry,
        label_key: String,
    ) -> Result<Self::UnlabelOutput, SvcError> {
        let cloned_node_spec = self.lock().clone();
        let spec_clone = self
            .start_update(
                registry,
                &cloned_node_spec,
                NodeOperation::Unlabel(label_key.into()),
            )
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

#[async_trait::async_trait]
impl ResourceLifecycle for OperationGuardArc<NodeSpec> {
    type Create = ();
    type CreateOutput = ();
    type Destroy = DestroyNode;
    type DestroyOutput = NodeDeleteResult;

    async fn create(
        _registry: &Registry,
        _request: &Self::Create,
    ) -> Result<Self::CreateOutput, SvcError> {
        unimplemented!(
            "Nodes self-register via keep-alives; they are not created via ResourceLifecycle"
        )
    }

    async fn destroy(
        &mut self,
        registry: &Registry,
        request: &Self::Destroy,
    ) -> Result<Self::DestroyOutput, SvcError> {
        let purge_result = self.purge_node(registry, request).await?;

        // Remove the node's runtime state (wrapper).
        {
            let mut nodes = registry.nodes().write().await;
            nodes.remove(request.id.as_ref());
        }

        Ok(purge_result)
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

    /// Purge a node and all its resources (pools, replicas, nexuses, snapshots).
    ///
    /// Pre-flight validation (offline, cordoned, accept flags, loss analysis)
    /// runs before any operation is logged. Once `start_destroy_for_purge` marks
    /// the node as Purging, all destructive work is captured and passed through
    /// `complete_destroy` — ensuring the pending op is always resolved.
    ///
    /// If the destructive work fails, `complete_destroy(Err)` clears the pending
    /// op while keeping Purging status. The `NodePurgeReconciler` will resume.
    pub(crate) async fn purge_node(
        &mut self,
        registry: &Registry,
        request: &DestroyNode,
    ) -> Result<NodeDeleteResult, SvcError> {
        let node_id = request.id.clone();
        let node_spec = self.as_ref().clone();
        let resuming = node_spec.status.purging();

        if !resuming {
            // Pre-flight validation — no op logged yet, safe to return early.
            Self::validate_purge_preconditions(registry, request, &node_spec).await?;
        }

        // Mark node as Purging BEFORE any destructive work.
        // From here on, errors must go through complete_destroy.
        self.start_destroy_for_purge(registry).await?;

        // Capture all destructive work results.
        let purge_result = Self::purge_node_resources(registry, &node_id, request).await;

        // Always complete the op:
        // - Ok(()) deletes the node spec from etcd and memory.
        // - Err(error) clears the pending op, keeps Purging for reconciler retry.
        self.complete_destroy(purge_result, registry).await
    }

    /// Validate all preconditions for node purge.
    /// Called before any operation is logged — safe to return errors directly.
    async fn validate_purge_preconditions(
        registry: &Registry,
        request: &DestroyNode,
        node_spec: &NodeSpec,
    ) -> Result<(), SvcError> {
        let node_id = &request.id;

        // 1. Validate node is offline.
        let node_is_online = match registry.node_state(node_id).await {
            Ok(state) => state.status == NodeStatus::Online,
            Err(_) => false,
        };
        if node_is_online {
            return Err(SvcError::NodeIsOnline {
                node_id: node_id.clone(),
            });
        }

        // 2. Validate node is cordoned.
        if !node_spec.cordoned() {
            return Err(SvcError::NodeNotCordoned {
                node_id: node_id.clone(),
            });
        }

        // 3. Collect pools on this node.
        let node_pools = registry.get_node_opt_pools(Some(node_id.clone())).await?;

        let has_resources = !node_pools.is_empty();

        // 4. If node has resources but purge=false, reject.
        if has_resources && !request.purge {
            return Err(SvcError::NodeHasResources {
                node_id: node_id.clone(),
            });
        }

        // 5. If node has resources, accept=true is required.
        if has_resources && !request.accept {
            return Err(SvcError::NodePurgeAcceptRequired {
                node_id: node_id.clone(),
                pool_count: node_pools.len(),
            });
        }

        // 5a. Analyze volume loss across ALL pools on this node.
        let pool_ids: HashSet<_> = node_pools.iter().map(|p| p.id().clone()).collect();
        let volume_ids: HashSet<_> = pool_ids
            .iter()
            .flat_map(|pid| registry.specs().pool_replicas(pid))
            .filter_map(|r| r.lock().owners.volume().cloned())
            .collect();

        if let Some(info) = analyze_volume_loss(registry, &pool_ids, &volume_ids).await? {
            if !request.accept_volume_loss {
                return Err(SvcError::NodePurgeVolumeLossAcceptRequired {
                    node_id: node_id.clone(),
                    pool_count: pool_ids.len(),
                    volume_count: info.volumes.len(),
                    volume_loss: info,
                });
            }
        }

        // 5b. Analyze snapshot loss across ALL pools.
        if let Some(info) = analyze_snapshot_loss(registry, &pool_ids)? {
            if !request.accept_snapshot_loss {
                return Err(SvcError::NodePurgeSnapshotLossAcceptRequired {
                    node_id: node_id.clone(),
                    pool_count: pool_ids.len(),
                    snapshot_count: info.snapshots.len(),
                    snapshot_loss: info,
                });
            }
        }

        Ok(())
    }

    /// Perform all destructive purge work: pool cordon+purge and nexus cleanup.
    ///
    /// Called after `start_destroy_for_purge`. The caller wraps the result in
    /// `complete_destroy` to ensure the pending op is always resolved.
    async fn purge_node_resources(
        registry: &Registry,
        node_id: &NodeId,
        request: &DestroyNode,
    ) -> Result<NodeDeleteResult, SvcError> {
        // Re-collect pools (in the resume case, pre-flight was skipped).
        let node_pools = registry.get_node_opt_pools(Some(node_id.clone())).await?;

        let mut result = NodeDeleteResult::new(node_id.clone());

        // Cordon and purge each pool.
        for pool in &node_pools {
            let mut pool_guard = match registry.specs().pool_guard(pool.id()).await? {
                Some(guard) => guard,
                None => continue,
            };

            // Best-effort cordon — pool purge's own validate_purge_cordon is the
            // real enforcement. Log failures but don't abort.
            let cordon_request = PoolCordonRequest {
                node_id: None,
                pool_id: pool.id().clone(),
                replicas: true,
                snapshots: true,
                restores: false,
                import: false,
            };
            if let Err(error) = pool_guard.cordon(registry, cordon_request).await {
                tracing::warn!(
                    node.id = %node_id,
                    pool.id = %pool.id(),
                    %error,
                    "Failed to auto-cordon pool during node purge (pool purge will validate)"
                );
            }

            let destroy_pool = DestroyPool::purge(pool.node(), pool.id().clone())
                .with_accept(request.accept)
                .with_accept_volume_loss(request.accept_volume_loss)
                .with_accept_snapshot_loss(request.accept_snapshot_loss);

            if let Some(pool_result) = pool_guard.destroy(registry, &destroy_pool).await? {
                result.merge_pool_result(&pool_result);
            }
        }

        /* TODO: Add Nexus removal
         * The node where the nexus is, is deleted, what happens to the volume?
         * HA is enabled, but the replicas are in offline nodes, what happens to this volume?
         * What happen to a volume if their last healthy replica is lost?
         */

        tracing::info!(
            node.id = %node_id,
            pools_deleted = node_pools.len(),
            volume_loss = result.has_volume_loss(),
            snapshot_loss = result.has_snapshot_loss(),
            "Node purged successfully"
        );

        Ok(result)
    }
}
