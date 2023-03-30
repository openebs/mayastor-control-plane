use crate::controller::{
    reconciler::{PollContext, TaskPoller},
    resources::{
        operations::{ResourceDrain, ResourcePublishing},
        operations_helper::OperationSequenceGuard,
        OperationGuard, OperationGuardArc, ResourceMutex,
    },
    task_poller::{PollEvent, PollResult, PollTimer, PollerState},
};
use agents::errors::SvcError;
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
    /// Identify draining nodes and attempt to drain them,
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
        matches!(context.event(), PollEvent::TimedRun)
    }
}

/// Republish the specified volume on any node other than its current node.
async fn republish_volume(
    volume: &mut OperationGuard<ResourceMutex<VolumeSpec>, VolumeSpec>,
    context: &PollContext,
    vol_uuid: &VolumeId,
    frontend_node: &NodeId,
) -> Result<(), SvcError> {
    let request = RepublishVolume::new(
        vol_uuid.clone(),
        None,
        frontend_node.clone(),
        VolumeShareProtocol::Nvmf,
        false,
        false,
    );
    tracing::info!(
        volume.uuid = vol_uuid.as_str(),
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
            Ok(mut guarded_vol_spec) => {
                if let Some(target) = guarded_vol_spec.as_ref().target() {
                    if target.node() != node_spec.as_ref().id() {
                        continue; // some other node's volume, ignore
                    }
                    let nexus_id = target.nexus().clone();
                    let vol_id = guarded_vol_spec.as_ref().uuid.clone();

                    let config = match guarded_vol_spec.as_ref().config() {
                        None => {
                            tracing::error!(
                                volume.id = vol_id.as_str(),
                                "Failed to get volume config"
                            );
                            move_failures = true;
                            continue;
                        }
                        Some(config) => config,
                    };
                    let frontend_node = config.frontend().node_name().unwrap_or_default();
                    let frontend_node_id: NodeId = frontend_node.into();
                    // frontend_node could be "", republish will still be allowed.
                    tracing::info!(
                        volume.uuid = vol_id.as_str(),
                        nexus.uuid = nexus_id.as_str(),
                        node.id = node_spec.as_ref().id().as_str(),
                        "Moving volume"
                    );
                    if let Err(e) =
                        republish_volume(&mut guarded_vol_spec, context, &vol_id, &frontend_node_id)
                            .await
                    {
                        tracing::error!(
                            error=%e,
                            volume.uuid = vol_id.as_str(),
                            nexus.uuid = nexus_id.as_str(),
                            node.id = node_spec.as_ref().id().as_str(),
                            "Failed to republish volume"
                        );
                        move_failures = true;
                        continue;
                    }
                    tracing::info!(
                        volume.uuid = vol_id.as_str(),
                        nexus.uuid = nexus_id.as_str(),
                        node.id = node_spec.as_ref().id().as_str(),
                        "Moved volume"
                    );
                    new_draining_volumes.insert(vol_id.clone());
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
