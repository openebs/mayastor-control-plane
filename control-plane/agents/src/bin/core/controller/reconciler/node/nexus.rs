use crate::controller::{
    reconciler::{PollContext, TaskPoller},
    resources::{
        operations::ResourcePublishing, operations_helper::OperationSequenceGuard, OperationGuard,
        ResourceMutex,
    },
    task_poller::{PollEvent, PollResult, PollTimer, PollerState},
};
use agents::errors::SvcError;
use common_lib::types::v0::{
    store::{node::NodeSpec, volume::VolumeSpec},
    transport::{NodeId, RepublishVolume, VolumeId, VolumeShareProtocol},
};
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
        let nodes = context.specs().get_nodes();
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
    );
    tracing::info!(
        volume.uuid = vol_uuid.as_str(),
        "Attempting to republish volume"
    );

    volume.republish(context.registry(), &request).await?;

    Ok(())
}

/// Drain the specified node if in draining state
async fn check_and_drain_node(context: &PollContext, node_spec: &NodeSpec) -> PollResult {
    if !node_spec.is_draining() {
        return PollResult::Ok(PollerState::Idle);
    }

    let node_id = node_spec.id();
    tracing::trace!(node.id = node_spec.id().as_str(), "Draining node");
    let vol_specs = context.specs().get_locked_volumes();

    let mut move_failures = false;

    // Iterate through all the volumes, find those with a nexus hosted on the
    // node and move each one away.
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
                            tracing::error!("Failed to get config");
                            move_failures = true;
                            continue;
                        }
                        Some(config) => config,
                    };
                    let frontend_node = config
                        .frontend()
                        .node_names()
                        .first()
                        .unwrap_or(&String::default())
                        .clone();
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
                }
            }
            Err(_) => {
                // we can't get to the volume so we don't know if it belongs to this node
                move_failures = true;
            }
        };
    }
    // Change the node state to "drained"
    if !move_failures {
        if let Err(e) = context
            .specs()
            .set_node_drained(context.registry(), node_spec.id())
            .await
        {
            tracing::error!(
                error=%e,
                node.id = node_id.as_str(),
                "Failed to set node to state drained"
            );
            return PollResult::Err(e);
        }
        tracing::info!(node.id = node_id.as_str(), "Set node to state drained");
    }
    PollResult::Ok(PollerState::Idle)
}
