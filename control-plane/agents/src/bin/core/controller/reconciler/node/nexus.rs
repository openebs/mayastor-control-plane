use crate::controller::{
    reconciler::{PollContext, TaskPoller},
    resources::{
        operations::ResourcePublishing, operations_helper::OperationSequenceGuard, OperationGuard,
        ResourceMutex,
    },
    task_poller::{PollEvent, PollResult, PollTimer, PollerState},
};
use agents::errors::SvcError;
use std::time::Duration;
use stor_port::types::v0::{
    store::{node::NodeSpec, volume::VolumeSpec},
    transport::{NodeId, RepublishVolume, VolumeId, VolumeShareProtocol},
};

const DRAINING_VOLUME_TIMOUT_SECONDS: u64 = 120;

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
    async fn poll(&mut self, context: &PollContext) -> PollResult {
        let nodes = context.specs().nodes();
        let mut results = Vec::with_capacity(nodes.len());

        for node in nodes {
            results.push(check_and_drain_node(context, &node).await);
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

async fn find_shutdown_volumes(context: &PollContext, node_id: &NodeId) -> Result<(), SvcError> {
    let draining_starttime = context.specs().get_node_draining_timestamp(node_id).await?;

    if draining_starttime.is_some() {
        let elapsed = draining_starttime.unwrap().elapsed();
        if elapsed.is_ok() && elapsed.unwrap() < Duration::from_secs(DRAINING_VOLUME_TIMOUT_SECONDS)
        {
            let draining_volumes = context.specs().get_node_draining_volumes(node_id).await?;

            for vi in draining_volumes {
                let shutdown_nexuses = context
                    .registry()
                    .specs()
                    .volume_shutdown_nexuses(&vi)
                    .await;
                if !shutdown_nexuses.is_empty() {
                    // if it still has shutdown nexuses
                    tracing::info!(
                        node.id = node_id.as_str(),
                        volume.id = vi.as_str(),
                        nexus.count = shutdown_nexuses.len(),
                        "Shutdown nexuses remain"
                    );
                } else {
                    tracing::info!(
                        node.id = node_id.as_str(),
                        volume.id = vi.as_str(),
                        "Removing volume from the draining volume list"
                    );
                    context
                        .specs()
                        .remove_node_draining_volume(context.registry(), node_id, &vi)
                        .await?;
                }
            }
        } else {
            // else it is timed out or no longer exists
            context
                .specs()
                .remove_node_draining_volumes(context.registry(), node_id)
                .await?;
        }
    }
    Ok(())
}

/// Drain the specified node if in draining state
async fn check_and_drain_node(context: &PollContext, node_spec: &NodeSpec) -> PollResult {
    if !node_spec.is_draining() {
        return PollResult::Ok(PollerState::Idle);
    }
    let node_id = node_spec.id();

    // In case this pod has restarted, set the timestamp of the draining node to now.
    context
        .specs()
        .set_draining_timestamp_if_none(node_id)
        .await?;

    tracing::trace!(node.id = node_id.as_str(), "Draining node");
    let vol_specs = context.specs().volumes_rsc();

    let mut move_failures = false;

    // Iterate through all the volumes, find those with a nexus hosted on the
    // node and move each one away via republish. Add each drained volume to the
    // vector of draining volumes stored in the node spec.
    for vol_spec in vol_specs {
        match vol_spec.operation_guard() {
            Ok(mut guarded_vol_spec) => {
                if let Some(target) = guarded_vol_spec.as_ref().target() {
                    if target.node() != node_id {
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
                        node.id = node_spec.id().as_str(),
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
                            node.id = node_spec.id().as_str(),
                            "Failed to republish volume"
                        );
                        move_failures = true;
                        continue;
                    }
                    tracing::info!(
                        volume.uuid = vol_id.as_str(),
                        nexus.uuid = nexus_id.as_str(),
                        node.id = node_spec.id().as_str(),
                        "Moved volume"
                    );
                    if let Err(error) = context
                        .specs()
                        .add_node_draining_volume(context.registry(), node_spec.id(), &vol_id)
                        .await
                    {
                        tracing::error!(
                            %error,
                            node.id = node_id.as_str(),
                            volume.id = vol_id.as_str(),
                            "Failed to add draining volume"
                        );
                        return PollResult::Err(error);
                    }
                }
            }
            Err(_) => {
                // we can't get to the volume so we don't know if it belongs to this node
                move_failures = true;
            }
        };
    }
    if !move_failures {
        // All volumes on the node are republished.
        // Determine whether we can mark the node as drained by checking
        // that all drained volumes do not have shutdown nexuses.
        // If that is not the case, the next reconciliation loop will check again.
        find_shutdown_volumes(context, node_id).await?;

        match context
            .specs()
            .get_node_draining_volume_count(node_id)
            .await?
        {
            // if there are no more shutdown volumes, change the node state to "drained"
            0 => {
                if let Err(error) = context
                    .specs()
                    .set_node_drained(context.registry(), node_id)
                    .await
                {
                    tracing::error!(
                        %error,
                        node.id = node_id.as_str(),
                        "Failed to set node to state drained"
                    );
                    return PollResult::Err(error);
                }
                tracing::info!(node.id = node_id.as_str(), "Set node to state drained");
            }
            remaining => {
                tracing::info!(
                    node.id = node_id.as_str(),
                    nexus.count = remaining,
                    "Shutdown nexuses remain"
                );
            }
        }
    }
    PollResult::Ok(PollerState::Idle)
}
