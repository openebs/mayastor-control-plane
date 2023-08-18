use crate::controller::{
    reconciler::{PollContext, TaskPoller},
    resources::{
        operations::{ResourceDrain, ResourcePublishing},
        operations_helper::OperationSequenceGuard,
        OperationGuard, OperationGuardArc, ResourceMutex,
    },
    task_poller::{PollEvent, PollResult, PollTimer, PollTriggerEvent, PollerState},
};
use agents::errors::SvcError;
use itertools::Either;
use std::{collections::HashSet, time::Duration};
use stor_port::types::v0::{
    store::{
        node::{DrainingVolumes, NodeSpec},
        volume::VolumeSpec,
    },
    transport::{NodeId, RepublishVolume, VolumeId, VolumeShareProtocol},
};

const DRAINING_VOLUME_TIMEOUT_SECONDS: u64 = 120;

/// Node drain reconciler.
#[derive(Debug)]
pub(super) struct NodeNexusReconciler {
    counter: PollTimer,
}
impl NodeNexusReconciler {
    /// Return a new `Self`.
    pub(super) fn new() -> Self {
        Self {
            counter: PollTimer::from(1), // sets the reconciler polling rate
        }
    }
}

#[async_trait::async_trait]
impl TaskPoller for NodeNexusReconciler {
    /// Identify draining nodes and attempt to drain them.
    async fn poll(&mut self, context: &PollContext) -> PollResult {
        let nodes = context.specs().nodes();
        let mut results = Vec::with_capacity(nodes.len());

        for node in nodes {
            let mut node_spec = context.registry().specs().guarded_node(node.id()).await?;
            results.push(check_and_drain_node(context, &mut node_spec).await);
        }
        Self::squash_results(results)
    }

    async fn poll_timer(&mut self, _context: &PollContext) -> bool {
        self.counter.poll()
    }

    async fn poll_event(&mut self, context: &PollContext) -> bool {
        matches!(
            context.event(),
            PollEvent::TimedRun | PollEvent::Triggered(PollTriggerEvent::NodeDrain)
        )
    }
}

/// Republish the specified volume on any node other than its current node.
async fn republish_volume(
    volume: &mut OperationGuard<ResourceMutex<VolumeSpec>, VolumeSpec>,
    context: &PollContext,
    vol_uuid: &VolumeId,
    frontend_node: &NodeId,
    target_node: Option<NodeId>,
) -> Result<(), SvcError> {
    let request = RepublishVolume::new(
        vol_uuid.clone(),
        target_node,
        frontend_node.clone(),
        VolumeShareProtocol::Nvmf,
        false,
        false,
    );
    tracing::info!(
        volume.uuid = vol_uuid.as_str(),
        target_node = ?request.target_node,
        "Attempting to republish volume"
    );

    volume.republish(context.registry(), &request).await?;

    Ok(())
}

/// Identify volumes that were on the drained node that contain shutdown nexuses.
async fn find_shutdown_volumes(
    context: &PollContext,
    node_spec: &mut OperationGuardArc<NodeSpec>,
) -> Result<(), SvcError> {
    let draining_starttime = node_spec.node_draining_timestamp().await;

    let Some(draining_starttime) = draining_starttime else {
        return Ok(());
    };
    let elapsed = draining_starttime.elapsed();
    if elapsed.is_ok() && elapsed.unwrap() < Duration::from_secs(DRAINING_VOLUME_TIMEOUT_SECONDS) {
        let draining_volumes = node_spec.node_draining_volumes().await;
        let mut draining_volumes_to_remove: HashSet<VolumeId> = HashSet::new();

        for vi in draining_volumes {
            let shutdown_nexuses = context
                .registry()
                .specs()
                .volume_shutdown_nexuses(&vi)
                .await;
            if !shutdown_nexuses.is_empty() {
                // if it still has shutdown nexuses
                tracing::info!(
                    node.id = node_spec.as_ref().id().as_str(),
                    volume.uuid = vi.as_str(),
                    nexus.count = shutdown_nexuses.len(),
                    "Shutdown nexuses remain"
                );
            } else {
                tracing::info!(
                    node.id = node_spec.as_ref().id().as_str(),
                    volume.uuid = vi.as_str(),
                    "Removing volume from the draining volume list"
                );
                draining_volumes_to_remove.insert(vi);
            }
        }
        node_spec
            .remove_draining_volumes(
                context.registry(),
                DrainingVolumes::new(draining_volumes_to_remove),
            )
            .await?;
    } else {
        // else the drain operation is timed out
        node_spec
            .remove_all_draining_volumes(context.registry())
            .await?;
    }
    Ok(())
}

/// Drain the specified node if in draining state
async fn check_and_drain_node(
    context: &PollContext,
    node_spec: &mut OperationGuardArc<NodeSpec>,
) -> PollResult {
    if !node_spec.as_ref().is_draining() {
        return PollResult::Ok(PollerState::Idle);
    }

    // In case this pod has restarted, set the timestamp of the draining node to now.
    node_spec.set_draining_timestamp_if_none().await;

    tracing::trace!(node.id = node_spec.as_ref().id().as_str(), "Draining node");
    let vol_specs = context.specs().volumes_rsc();

    let mut move_failures = false;

    let mut new_draining_volumes: HashSet<VolumeId> = HashSet::new();

    // Iterate through all the volumes, find those with a nexus hosted on the
    // node and move each one away via republish. Add each drained volume to the
    // set of draining volumes stored in the node spec.
    for vol_spec in vol_specs {
        match vol_spec.operation_guard() {
            Ok(guarded_vol_spec) => {
                match drain_volume_target(context, node_spec, guarded_vol_spec).await {
                    Either::Left(true) => {
                        move_failures = true;
                    }
                    Either::Left(false) => {}
                    Either::Right(vol_id) => {
                        new_draining_volumes.insert(vol_id);
                    }
                }
            }
            Err(_) => {
                // we can't get to the volume so we don't know if it belongs to this node
                move_failures = true;
            }
        };
    }
    if let Err(error) = node_spec
        .add_draining_volumes(
            context.registry(),
            DrainingVolumes::new(new_draining_volumes),
        )
        .await
    {
        tracing::error!(
            %error,
            node.id = node_spec.as_ref().id().as_str(),
            "Failed to add draining volumes"
        );
        return PollResult::Err(error);
    }
    if !move_failures {
        // All volumes on the node are republished.
        // Determine whether we can mark the node as drained by checking
        // that all drained volumes do not have shutdown nexuses.
        // If that is not the case, the next reconciliation loop will check again.
        find_shutdown_volumes(context, node_spec).await?;

        match node_spec.node_draining_volume_count().await {
            // if there are no more shutdown volumes, change the node state to "drained"
            0 => {
                if let Err(error) = node_spec.set_drained(context.registry()).await {
                    tracing::error!(
                        %error,
                        node.id = node_spec.as_ref().id().as_str(),
                        "Failed to set node to state drained"
                    );
                    return PollResult::Err(error);
                }
                tracing::info!(
                    node.id = node_spec.as_ref().id().as_str(),
                    "Set node to state drained",
                );
            }
            remaining => {
                tracing::info!(
                    node.id = node_spec.as_ref().id().as_str(),
                    nexus.count = remaining,
                    "Shutdown nexuses remain"
                );
            }
        }
    }
    PollResult::Ok(PollerState::Idle)
}

async fn drain_volume_target(
    context: &PollContext,
    node_spec: &OperationGuardArc<NodeSpec>,
    mut guarded_vol_spec: OperationGuardArc<VolumeSpec>,
) -> Either<bool, VolumeId> {
    let Some(target) = guarded_vol_spec.as_ref().target() else {
        return Either::Left(false);
    };

    let drain_node_id = node_spec.as_ref().id();
    let Some(target_node) =
        select_drain_target_node(context, target.node(), drain_node_id, &guarded_vol_spec)
    else {
        return Either::Left(false);
    };

    let nexus_id = target.nexus().clone();
    let vol_id = guarded_vol_spec.as_ref().uuid.clone();

    let Some(config) = guarded_vol_spec.as_ref().config() else {
        tracing::error!(volume.id = vol_id.as_str(), "Failed to get volume config");
        return Either::Left(true);
    };
    let frontend_node = config.frontend().node_name().unwrap_or_default();
    let frontend_node_id: NodeId = frontend_node.into();
    // frontend_node could be "", republish will still be allowed.
    tracing::info!(
        volume.uuid = vol_id.as_str(),
        nexus.uuid = nexus_id.as_str(),
        node.id = drain_node_id.as_str(),
        "Moving volume"
    );
    if let Err(error) = republish_volume(
        &mut guarded_vol_spec,
        context,
        &vol_id,
        &frontend_node_id,
        target_node,
    )
    .await
    {
        tracing::error!(
            %error,
            volume.uuid = vol_id.as_str(),
            nexus.uuid = nexus_id.as_str(),
            node.id = drain_node_id.as_str(),
            "Failed to republish volume"
        );
        return Either::Left(true);
    }
    tracing::info!(
        volume.uuid = vol_id.as_str(),
        nexus.uuid = nexus_id.as_str(),
        node.id = drain_node_id.as_str(),
        "Moved volume"
    );

    Either::Right(vol_id)
}

/// Select a desired target node where we'll move the volume target to.
/// For multi-replica volumes, leave as None which we'll allow the core scheduling logic to
/// select the most appropriate node.
/// For single-replica volumes, move target together with the replica if not already.
fn select_drain_target_node(
    context: &PollContext,
    target_node: &NodeId,
    drain_node: &NodeId,
    volume: &OperationGuardArc<VolumeSpec>,
) -> Option<Option<NodeId>> {
    if volume.as_ref().num_replicas != 1 {
        return if drain_node != target_node {
            // Nothing to drain here..
            None
        } else {
            // Let scheduler decide where to move it to..
            Some(None)
        };
    }
    select_drain_1r_target(context, target_node, drain_node, volume)
}
fn select_drain_1r_target(
    context: &PollContext,
    target_node: &NodeId,
    drain_node: &NodeId,
    volume: &OperationGuardArc<VolumeSpec>,
) -> Option<Option<NodeId>> {
    if drain_node != target_node {
        // Nothing to drain here..
        // todo: we might want to move targets to the replica node!
        return None;
    }

    let mut nodes = context.specs().volume_replica_nodes(volume.uuid());
    let Some(replica_node) = nodes.pop() else {
        // Can't find the replica node.. move anywhere?
        return Some(None);
    };

    if target_node == &replica_node {
        // All local already, don't move the target!
        None
    } else {
        // Since we're moving the target, move it to the replica node!
        Some(Some(replica_node))
    }
}
