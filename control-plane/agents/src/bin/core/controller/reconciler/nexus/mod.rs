pub(super) mod capacity;
mod garbage_collector;

use crate::{
    controller::{
        io_engine::{NexusApi, NexusChildActionApi},
        policies::rebuild_policies::RuleSet,
        reconciler::{ReCreate, Reconciler},
        resources::{
            operations::ResourceSharing,
            operations_helper::{OperationSequenceGuard, SpecOperationsHelper},
            OperationGuardArc, TraceSpan, TraceStrLog,
        },
        scheduling::resources::HealthyChildItems,
        task_poller::{
            squash_results, PollContext, PollPeriods, PollResult, PollTimer, PollerState,
            TaskPoller,
        },
        wrapper::NodeWrapper,
    },
    nexus::scheduling::healthy_nexus_children,
    node::wrapper::NexusChildActionContextNode,
};
use agents::errors::SvcError;
use capacity::enospc_children_onliner;
use garbage_collector::GarbageCollector;

use stor_port::{
    transport_api::ErrorChain,
    types::v0::{
        store::{
            nexus::{NexusSpec, ReplicaUri},
            nexus_child::NexusChild,
        },
        transport::{
            Child, ChildUri, CreateNexus, Nexus, NexusChildActionContext, NexusShareProtocol,
            NexusStatus, NodeStatus, ReplicaId, ShareNexus, UnshareNexus,
        },
    },
};

use std::{
    convert::TryFrom,
    sync::Arc,
    time::{Duration, SystemTime},
};

use tokio::sync::RwLock;
use tracing::Instrument;

/// Nexus Reconciler loop
#[derive(Debug)]
pub(crate) struct NexusReconciler {
    counter: PollTimer,
    poll_targets: Vec<Box<dyn TaskPoller>>,
}
impl NexusReconciler {
    /// Return new `Self` with the provided period
    pub(crate) fn from(period: PollPeriods) -> Self {
        NexusReconciler {
            counter: PollTimer::from(period),
            poll_targets: vec![Box::new(GarbageCollector::new())],
        }
    }
    /// Return new `Self` with the default period
    pub(crate) fn new() -> Self {
        Self::from(1)
    }
}

#[async_trait::async_trait]
impl TaskPoller for NexusReconciler {
    async fn poll(&mut self, context: &PollContext) -> PollResult {
        let mut results = vec![];
        for nexus in context.specs().nexuses() {
            let skip = {
                let nexus = nexus.lock();
                nexus.owned() || nexus.dirty()
            };
            if skip {
                continue;
            }
            let mut nexus = match nexus.operation_guard() {
                Ok(guard) => guard,
                Err(_) => continue,
            };
            if !nexus.as_ref().managed {
                results.push(nexus.recreate_state(context).await);
            }
            results.push(nexus.reconcile(context).await);
        }
        for target in &mut self.poll_targets {
            results.push(target.try_poll(context).await);
        }
        Self::squash_results(results)
    }

    async fn poll_timer(&mut self, _context: &PollContext) -> bool {
        self.counter.poll()
    }
}

#[async_trait::async_trait]
impl Reconciler for OperationGuardArc<NexusSpec> {
    async fn reconcile(&mut self, context: &PollContext) -> PollResult {
        nexus_reconciler(self, context).await
    }
}

#[async_trait::async_trait]
impl ReCreate for OperationGuardArc<NexusSpec> {
    async fn recreate_state(&mut self, context: &PollContext) -> PollResult {
        missing_nexus_recreate(self, context).await
    }
}

async fn nexus_reconciler(
    nexus: &mut OperationGuardArc<NexusSpec>,
    context: &PollContext,
) -> PollResult {
    let reconcile = {
        let nexus_spec = nexus.lock();
        nexus_spec.status().created() && !nexus_spec.is_shutdown()
    };

    if reconcile {
        match squash_results(vec![
            handle_faulted_children(nexus, context).await,
            unknown_children_remover(nexus, context).await,
            missing_children_remover(nexus, context).await,
            fixup_nexus_protocol(nexus, context).await,
            enospc_children_onliner(nexus, context).await,
        ]) {
            Err(SvcError::NexusNotFound { .. }) => PollResult::Ok(PollerState::Idle),
            other => other,
        }
    } else {
        PollResult::Ok(PollerState::Idle)
    }
}

/// Checks if nexus is Degraded and any child is Faulted. If yes, Depending on rebuild policy for
/// child it performs rebuild operation.
#[tracing::instrument(skip(nexus, context), level = "trace", fields(nexus.uuid = %nexus.uuid(), nexus.node = %nexus.as_ref().node, request.reconcile = true))]
pub(super) async fn handle_faulted_children(
    nexus: &mut OperationGuardArc<NexusSpec>,
    context: &PollContext,
) -> PollResult {
    let nexus_uuid = nexus.uuid();
    let nexus_state = context.registry().nexus(nexus_uuid).await?;
    let child_count = nexus_state.children.len();
    if nexus_state.status == NexusStatus::Degraded && child_count > 1 {
        for child in nexus_state.children.iter().filter(|c| c.state.faulted()) {
            let _ = handle_faulted_child(nexus, child, &nexus_state, context).await;
        }
    }
    Ok(PollerState::Idle)
}

/// Handles faulted nexus children.
/// Depending on the fault policy it either waits for specified duration until the child comes back
/// or it opts for a full rebuilding by faulting the child instead.
async fn handle_faulted_child(
    nexus_spec: &mut OperationGuardArc<NexusSpec>,
    child: &Child,
    nexus: &Nexus,
    context: &PollContext,
) -> Result<(), SvcError> {
    let wait_duration = RuleSet::faulted_child_wait(nexus, context.registry());
    let can_partial_rebuild = child.has_io_log == Some(true);

    let Some(child_uuid) = nexus_spec.as_ref().replica_uri(&child.uri).map(|r| r.uuid()) else {
        tracing::warn!(%child.uri, "Unknown Child found, a full rebuild is required");
        return faulted_children_remover(nexus_spec, child, context).await;
    };
    let Some(faulted_at) = child.faulted_at else {
        tracing::warn!(%child.uri, child.uuid=%child_uuid, "Child faulted without a timestamp set, a full rebuild required");
        return faulted_children_remover(nexus_spec, child, context).await;
    };

    if !can_partial_rebuild {
        tracing::warn!(%child.uri, child.uuid=%child_uuid, ?child.state_reason, "No IO log available, this child cannot be partially rebuilt");
        faulted_children_remover(nexus_spec, child, context).await?;
    } else if child_replica_is_online(child_uuid, context).await {
        let child_uuid = child_uuid.clone();
        tracing::info!(%child.uri, child.uuid=%child_uuid, ?child.state_reason, "Child's replica is back online within the partial rebuild window");
        if let Err(error) = online_nexus_child(nexus_spec, &child.uri, context).await {
            tracing::warn!(
                %child.uri,
                child.uuid=%child_uuid,
                ?child.state_reason,
                %error,
                "Failed to online child, a full rebuild is required"
            );
            faulted_children_remover(nexus_spec, child, context).await?;
        }
    } else if let Some(elapsed) =
        wait_duration_elapsed(&child.uri, child_uuid, faulted_at, wait_duration)
    {
        tracing::warn!(%child.uri, child.uuid=%child_uuid, ?child.state_reason, ?elapsed, "Partial rebuild window elapsed, a full rebuild is required");
        faulted_children_remover(nexus_spec, child, context).await?;
    }
    Ok(())
}

/// Removes child from nexus children list to initiate Full rebuild of child.
async fn faulted_children_remover(
    nexus: &mut OperationGuardArc<NexusSpec>,
    child: &Child,
    context: &PollContext,
) -> Result<(), SvcError> {
    let faulted_at = child.faulted_at.map(chrono::DateTime::<chrono::Utc>::from);
    let nexus_uuid = nexus.uuid();
    let nexus_state = context.registry().nexus(nexus_uuid).await?;
    nexus.warn_span(|| {
        tracing::warn!(%child.uri, %child.state, %child.state_reason, ?faulted_at, "Attempting to remove faulted child")
    });
    nexus
        .remove_child_by_uri(context.registry(), &nexus_state, &child.uri)
        .await?;

    nexus.warn_span(|| {
        tracing::warn!(%child.uri, %child.state, %child.state_reason, ?faulted_at, "Successfully removed faulted child")
    });
    Ok(())
}

/// Returns elapsed time, if time elapsed from child fault time is greater or equal to the
/// wait time duration.
fn wait_duration_elapsed(
    child_uri: &ChildUri,
    replica_uuid: &ReplicaId,
    fault_time: SystemTime,
    wait_duration: Duration,
) -> Option<Duration> {
    match fault_time.elapsed() {
        Ok(elapsed) if elapsed >= wait_duration => Some(elapsed),
        Ok(elapsed) => {
            tracing::info!(
                child.uri = %child_uri,
                replica.uuid = %replica_uuid,
                "Waited {elapsed:?} for the child's replica to come back (wait time is {wait_duration:?})"
            );
            None
        }
        // if time drifts by this much then just keep waiting
        Err(error) if error.duration() < Duration::from_secs(5) => None,
        // otherwise just rebuild now anyway, assume wait_duration expired.
        Err(error) => {
            tracing::error!(
                "SystemTime has drifted (time is -{error:?}), assuming wait-duration expired as worst-case"
            );
            Some(wait_duration)
        }
    }
}

/// Check if a child's replica is online.
/// If the node has gone offline the replica will not be present in the registry.
async fn child_replica_is_online(child_uuid: &ReplicaId, context: &PollContext) -> bool {
    context.registry().replica(child_uuid).await.is_ok()
}

/// Tries to mark child as Online. Returns error on failing to do so.
pub(super) async fn online_nexus_child(
    nexus: &mut OperationGuardArc<NexusSpec>,
    child: &ChildUri,
    context: &PollContext,
) -> Result<(), SvcError> {
    let nexus_node = &nexus.as_ref().node;
    let node = context.registry().node_wrapper(nexus_node).await?;
    let online_context = NexusChildActionContext::new(nexus_node, nexus.uuid(), child);
    let ctx = NexusChildActionContextNode::new(online_context, context.registry());
    // TODO: Check if we can handle things differently for different SvcError.
    node.online_child(ctx).await?;
    nexus.info_span(|| tracing::info!(child.uri = %child.as_str(), "Successfully onlined child"));
    Ok(())
}

/// Find and removes unknown children from the given nexus
/// If the child is a replica it also disowns and destroys it
#[tracing::instrument(skip(nexus, context), level = "trace", fields(nexus.uuid = %nexus.uuid(), request.reconcile = true))]
pub(super) async fn unknown_children_remover(
    nexus: &mut OperationGuardArc<NexusSpec>,
    context: &PollContext,
) -> PollResult {
    let nexus_state = context.registry().nexus(nexus.uuid()).await?;
    let state_children = nexus_state.children.iter();
    let spec_children = nexus.lock().children.clone();

    let unknown_children = state_children
        .filter(|c| !spec_children.iter().any(|spec| spec.uri() == c.uri))
        .cloned()
        .collect::<Vec<_>>();

    if !unknown_children.is_empty() {
        let span = tracing::info_span!("unknown_children_remover", nexus.uuid = %nexus.uuid(), request.reconcile = true);
        async move {
            for child in unknown_children {
                nexus.warn_span(|| {
                    tracing::warn!("Attempting to remove unknown child '{}'", child.uri)
                });
                if let Err(error) = nexus
                    .remove_child_by_uri(context.registry(), &nexus_state, &child.uri)
                    .await
                {
                    nexus.error(&format!(
                        "Failed to remove unknown child '{}', error: '{}'",
                        child.uri,
                        error.full_string(),
                    ));
                } else {
                    nexus.info(&format!(
                        "Successfully removed unknown child '{}'",
                        child.uri,
                    ));
                }
            }
        }
        .instrument(span)
        .await
    }

    PollResult::Ok(PollerState::Idle)
}

/// Find missing children from the given nexus
/// They are removed from the spec as we don't know why they got removed, so it's safer
/// to just disown and destroy them.
#[tracing::instrument(skip(nexus, context), level = "trace", fields(nexus.uuid = %nexus.uuid(), request.reconcile = true))]
pub(super) async fn missing_children_remover(
    nexus: &mut OperationGuardArc<NexusSpec>,
    context: &PollContext,
) -> PollResult {
    let nexus_uuid = nexus.uuid();
    let nexus_state = context.registry().nexus(nexus_uuid).await?;
    let spec_children = nexus.lock().children.clone().into_iter();

    let mut result = PollResult::Ok(PollerState::Idle);
    for child in
        spec_children.filter(|spec| !nexus_state.children.iter().any(|c| c.uri == spec.uri()))
    {
        nexus.warn_span(|| tracing::warn!(
            "Attempting to remove missing child '{}'. It may have been removed for a reason so it will be replaced with another",
            child.uri(),
        ));

        if let Err(error) = nexus
            .remove_child_by_uri(context.registry(), &nexus_state, &child.uri())
            .await
        {
            nexus.error_span(|| {
                tracing::error!(
                    "Failed to remove child '{}' from the nexus spec, error: '{}'",
                    child.uri(),
                    error.full_string(),
                )
            });
            result = PollResult::Err(error);
        } else {
            nexus.info_span(|| {
                tracing::info!(
                    "Successfully removed missing child '{}' from the nexus spec",
                    child.uri(),
                )
            });
        }
    }

    result
}

/// Recreate the given nexus on its associated node
/// Only healthy and online replicas are reused in the nexus recreate request
pub(super) async fn missing_nexus_recreate(
    nexus: &mut OperationGuardArc<NexusSpec>,
    context: &PollContext,
) -> PollResult {
    let nexus_uuid = nexus.uuid();

    if context.registry().nexus(nexus_uuid).await.is_ok() {
        return PollResult::Ok(PollerState::Idle);
    }

    #[tracing::instrument(skip(nexus_guard, context), fields(nexus.uuid = %nexus_guard.uuid(), request.reconcile = true))]
    async fn missing_nexus_recreate(
        nexus_guard: &mut OperationGuardArc<NexusSpec>,
        context: &PollContext,
    ) -> PollResult {
        let warn_missing = |nexus_spec: &NexusSpec, node_status: NodeStatus| {
            nexus_spec.debug_span(|| {
                tracing::debug!(
                    node.id = %nexus_spec.node,
                    node.status = %node_status.to_string(),
                    "Attempted to recreate missing nexus, but the node is not online"
                )
            });
        };

        let nexus = nexus_guard.as_ref();

        let node = match context.registry().node_wrapper(&nexus.node).await {
            Ok(node) if !node.read().await.is_online() => {
                let node_status = node.read().await.status();
                warn_missing(nexus, node_status);
                return PollResult::Ok(PollerState::Idle);
            }
            Err(_) => {
                warn_missing(nexus, NodeStatus::Unknown);
                return PollResult::Ok(PollerState::Idle);
            }
            Ok(node) => node,
        };

        nexus.warn_span(|| tracing::warn!("Attempting to recreate missing nexus"));

        let children = healthy_nexus_children(nexus, context.registry()).await?;

        let mut nexus_replicas = vec![];
        for item in children.candidates() {
            // just in case the replica gets somehow shared/unshared?
            match nexus_guard
                .make_me_replica_accessible(context.registry(), item.state())
                .await
            {
                Ok(uri) => {
                    nexus_replicas.push(NexusChild::Replica(ReplicaUri::new(
                        &item.spec().uuid,
                        &uri,
                    )));
                }
                Err(error) => {
                    nexus_guard.error_span(|| {
                        tracing::error!(nexus.node=%nexus_guard.as_ref().node, replica.uuid = %item.spec().uuid, error=%error, "Failed to make the replica available on the nexus node");
                    });
                }
            }
        }

        let mut nexus = nexus_guard.as_ref().clone();

        nexus.children = match children {
            HealthyChildItems::One(_, _) => nexus_replicas.first().into_iter().cloned().collect(),
            HealthyChildItems::All(_, _) => nexus_replicas,
        };

        if nexus.children.is_empty() {
            if let Some(info) = children.nexus_info() {
                if info.no_healthy_replicas() {
                    nexus.error_span(|| {
                        tracing::error!("No healthy replicas found - manual intervention required")
                    });
                    return PollResult::Ok(PollerState::Idle);
                }
            }

            nexus.warn_span(|| {
                tracing::warn!("No nexus children are available. Will retry later...")
            });
            return PollResult::Ok(PollerState::Idle);
        }

        match node.create_nexus(&CreateNexus::from(&nexus)).await {
            Ok(_) => {
                nexus.info_span(|| tracing::info!("Nexus successfully recreated"));
                PollResult::Ok(PollerState::Idle)
            }
            Err(error) => {
                nexus.error_span(|| tracing::error!(error=%error, "Failed to recreate the nexus"));
                Err(error)
            }
        }
    }

    missing_nexus_recreate(nexus, context).await
}

/// Fixup the nexus share protocol if it does not match what the specs says
/// If the nexus is shared but the protocol is not the same as the spec, then we must first
/// unshare the nexus, and then share it via the correct protocol
#[tracing::instrument(skip(nexus, context), level = "debug", fields(nexus.uuid = %nexus.uuid(), request.reconcile = true))]
pub(super) async fn fixup_nexus_protocol(
    nexus: &mut OperationGuardArc<NexusSpec>,
    context: &PollContext,
) -> PollResult {
    let nexus_uuid = nexus.uuid();
    if let Ok(nexus_state) = context.registry().nexus(nexus_uuid).await {
        let nexus_spec = nexus.lock().clone();
        if nexus_spec.share != nexus_state.share {
            nexus_spec.warn_span(|| {
                tracing::warn!(
                    "Attempting to fix wrong nexus share protocol, current: '{}', expected: '{}'",
                    nexus_state.share.to_string(),
                    nexus_spec.share.to_string()
                )
            });

            // if the protocols mismatch, we must first unshare the nexus!
            if (nexus_state.share.shared() && nexus_spec.share.shared())
                || !nexus_spec.share.shared()
            {
                nexus
                    .unshare(context.registry(), &UnshareNexus::from(&nexus_state))
                    .await?;
            }
            if nexus_spec.share.shared() {
                match NexusShareProtocol::try_from(nexus_spec.share) {
                    Ok(protocol) => {
                        let allowed_host = nexus.lock().allowed_hosts.clone();
                        nexus
                            .share(
                                context.registry(),
                                &ShareNexus::new(&nexus_state, protocol, allowed_host),
                            )
                            .await?;
                        nexus_spec
                            .info_span(|| tracing::info!("Nexus protocol changed successfully"));
                    }
                    Err(error) => {
                        nexus_spec.error_span(|| {
                            tracing::error!(error=%error, "Invalid configuration for nexus protocol, cannot apply it...")
                        });
                    }
                }
            }
        }
    }

    PollResult::Ok(PollerState::Idle)
}

/// Given a published self-healing volume
/// When its nexus target is faulted
/// And one or more of its healthy replicas are back online
/// Then the nexus shall be removed from its associated node
pub(super) async fn faulted_nexus_remover(
    nexus: &mut OperationGuardArc<NexusSpec>,
    context: &PollContext,
) -> PollResult {
    let Ok(nexus_state) = context.registry().nexus(nexus.uuid()).await else {
        return PollResult::Ok(PollerState::Idle);
    };

    if nexus_state.status != NexusStatus::Faulted && nexus_state.status != NexusStatus::Shutdown {
        return PollResult::Ok(PollerState::Idle);
    }

    let nexus_spec = nexus.as_ref();
    let healthy_children = healthy_nexus_children(nexus_spec, context.registry()).await?;
    let node = context.registry().node_wrapper(&nexus_spec.node).await?;

    #[tracing::instrument(skip(nexus, node), fields(nexus.uuid = %nexus.uuid(), request.reconcile = true))]
    async fn faulted_nexus_remover(
        nexus: &mut OperationGuardArc<NexusSpec>,
        node: Arc<RwLock<NodeWrapper>>,
    ) -> PollResult {
        nexus.warn("Removing Faulted Nexus so it can be recreated with its healthy children");

        // destroy the nexus - it will be recreated by the missing_nexus reconciler!
        match node.destroy_nexus(&nexus.as_ref().clone().into()).await {
            Ok(_) => {
                nexus.info("Faulted Nexus successfully removed");
            }
            Err(error) => {
                nexus.info_span(|| tracing::error!(error=%error.full_string(), "Failed to remove Faulted Nexus"));
                return Err(error);
            }
        }

        PollResult::Ok(PollerState::Idle)
    }

    let node_online = node.read().await.is_online();
    // only remove the faulted nexus when the children are available again
    if !node_online || healthy_children.candidates().is_empty() {
        return PollResult::Ok(PollerState::Idle);
    }

    if nexus_state.children.iter().all(|c| c.enospc()) && !nexus_state.children.is_empty() {
        let mut fault_nexus = false;
        for child in nexus_state.children.iter().filter(|c| c.enospc()) {
            if capacity::child_enospc_onlineable(nexus, child, context.registry())
                .await
                .is_ok()
            {
                fault_nexus = true;
                break;
            }
        }
        if !fault_nexus {
            nexus.warn_span(|| tracing::warn!("Faulted ENOSPC Nexus has no onlineable children"));
            return PollResult::Ok(PollerState::Idle);
        }
        nexus.warn_span(|| {
            tracing::warn!(
                "Removing faulted ENOSPC Nexus as one or more children are deemed to be onlineable"
            )
        });
    }
    faulted_nexus_remover(nexus, node).await
}
