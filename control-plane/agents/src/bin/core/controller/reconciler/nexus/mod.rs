mod garbage_collector;

use crate::{
    controller::{
        resources::operations_helper::{OperationSequenceGuard, SpecOperationsHelper},
        scheduling::resources::HealthyChildItems,
        task_poller::{
            squash_results, PollContext, PollPeriods, PollResult, PollTimer, PollerState,
            TaskPoller,
        },
        wrapper::ClientOps,
    },
    nexus::scheduling::get_healthy_nexus_children,
};
use common_lib::{
    transport_api::ErrorChain,
    types::v0::{
        store::{
            nexus::{NexusSpec, ReplicaUri},
            nexus_child::NexusChild,
        },
        transport::{CreateNexus, NexusShareProtocol, NodeStatus, ShareNexus, UnshareNexus},
    },
};
use garbage_collector::GarbageCollector;

use crate::controller::{
    reconciler::{ReCreate, Reconciler},
    resources::{operations::ResourceSharing, OperationGuardArc, TraceSpan, TraceStrLog},
    wrapper::NodeWrapper,
};
use agents::errors::SvcError;
use common_lib::types::v0::transport::{FaultNexusChild, NexusStatus};
use std::{convert::TryFrom, sync::Arc};
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
        for nexus in context.specs().get_nexuses() {
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
            faulted_children_remover(nexus, context).await,
            unknown_children_remover(nexus, context).await,
            missing_children_remover(nexus, context).await,
            fixup_nexus_protocol(nexus, context).await,
            enospc_children_faulter(nexus, context).await,
        ]) {
            Err(SvcError::NexusNotFound { .. }) => PollResult::Ok(PollerState::Idle),
            other => other,
        }
    } else {
        PollResult::Ok(PollerState::Idle)
    }
}

/// Find and removes faulted children from the given nexus
/// If the child is a replica it also disowns and destroys it
#[tracing::instrument(skip(nexus, context), level = "trace", fields(nexus.uuid = %nexus.uuid(), request.reconcile = true))]
pub(super) async fn faulted_children_remover(
    nexus: &mut OperationGuardArc<NexusSpec>,
    context: &PollContext,
) -> PollResult {
    let nexus_uuid = nexus.uuid();
    let nexus_state = context.registry().get_nexus(nexus_uuid).await?;
    let child_count = nexus_state.children.len();

    // Remove faulted children only from a degraded nexus with other healthy children left
    if nexus_state.status == NexusStatus::Degraded && child_count > 1 {
        let span = tracing::info_span!("faulted_children_remover", nexus.uuid = %nexus_uuid, request.reconcile = true);
        async {
            let nexus_spec_clone = nexus.lock().clone();
            for child in nexus_state.children.iter().filter(|c| c.state.faulted()) {
                nexus_spec_clone.warn_span(|| {
                    tracing::warn!("Attempting to remove faulted child '{}'", child.uri)
                });
                if let Err(error) = context
                    .specs()
                    .remove_nexus_child_by_uri(
                        context.registry(),
                        nexus,
                        &nexus_state,
                        &child.uri,
                        true,
                    )
                    .await
                {
                    nexus_spec_clone.error_span(|| {
                        tracing::error!(
                            error = %error.full_string().as_str(),
                            child.uri = %child.uri.as_str(),
                            "Failed to remove faulted child"
                        )
                    });
                } else {
                    nexus_spec_clone.info_span(|| {
                        tracing::info!(
                            child.uri = %child.uri.as_str(),
                            "Successfully removed faulted child",
                        )
                    });
                }
            }
        }
        .instrument(span)
        .await
    }

    PollResult::Ok(PollerState::Idle)
}

/// Find and removes unknown children from the given nexus
/// If the child is a replica it also disowns and destroys it
#[tracing::instrument(skip(nexus, context), level = "trace", fields(nexus.uuid = %nexus.uuid(), request.reconcile = true))]
pub(super) async fn unknown_children_remover(
    nexus: &mut OperationGuardArc<NexusSpec>,
    context: &PollContext,
) -> PollResult {
    let nexus_state = context.registry().get_nexus(nexus.uuid()).await?;
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
                if let Err(error) = context
                    .specs()
                    .remove_nexus_child_by_uri(
                        context.registry(),
                        nexus,
                        &nexus_state,
                        &child.uri,
                        false,
                    )
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
    let nexus_state = context.registry().get_nexus(nexus_uuid).await?;
    let spec_children = nexus.lock().children.clone().into_iter();

    let mut result = PollResult::Ok(PollerState::Idle);
    for child in
        spec_children.filter(|spec| !nexus_state.children.iter().any(|c| c.uri == spec.uri()))
    {
        nexus.warn_span(|| tracing::warn!(
            "Attempting to remove missing child '{}'. It may have been removed for a reason so it will be replaced with another",
            child.uri(),
        ));

        if let Err(error) = context
            .specs()
            .remove_nexus_child_by_uri(context.registry(), nexus, &nexus_state, &child.uri(), true)
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

    if context.registry().get_nexus(nexus_uuid).await.is_ok() {
        return PollResult::Ok(PollerState::Idle);
    }

    #[tracing::instrument(skip(nexus, context), fields(nexus.uuid = %nexus.uuid, request.reconcile = true))]
    async fn missing_nexus_recreate(mut nexus: NexusSpec, context: &PollContext) -> PollResult {
        let warn_missing = |nexus_spec: &NexusSpec, node_status: NodeStatus| {
            nexus_spec.debug_span(|| {
                tracing::debug!(
                    node.id = %nexus_spec.node,
                    node.status = %node_status.to_string(),
                    "Attempted to recreate missing nexus, but the node is not online"
                )
            });
        };

        let node = match context.registry().get_node_wrapper(&nexus.node).await {
            Ok(node) if !node.read().await.is_online() => {
                let node_status = node.read().await.status();
                warn_missing(&nexus, node_status);
                return PollResult::Ok(PollerState::Idle);
            }
            Err(_) => {
                warn_missing(&nexus, NodeStatus::Unknown);
                return PollResult::Ok(PollerState::Idle);
            }
            Ok(node) => node,
        };

        nexus.warn_span(|| tracing::warn!("Attempting to recreate missing nexus"));

        let children = get_healthy_nexus_children(&nexus, context.registry()).await?;

        let mut nexus_replicas = vec![];
        for item in children.candidates() {
            // just in case the replica gets somehow shared/unshared?
            match context
                .specs()
                .make_replica_accessible(context.registry(), item.state(), &nexus.node)
                .await
            {
                Ok(uri) => {
                    nexus_replicas.push(NexusChild::Replica(ReplicaUri::new(
                        &item.spec().uuid,
                        &uri,
                    )));
                }
                Err(error) => {
                    nexus.error_span(|| {
                        tracing::error!(nexus.node=%nexus.node, replica.uuid = %item.spec().uuid, error=%error, "Failed to make the replica available on the nexus node");
                    });
                }
            }
        }

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

    let nexus = nexus.lock().clone();
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
    if let Ok(nexus_state) = context.registry().get_nexus(nexus_uuid).await {
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
    let nexus_uuid = nexus.uuid();

    if let Ok(nexus_state) = context.registry().get_nexus(nexus_uuid).await {
        if nexus_state.status == NexusStatus::Faulted {
            let nexus = nexus.lock().clone();
            let healthy_children = get_healthy_nexus_children(&nexus, context.registry()).await?;
            let node = context.registry().get_node_wrapper(&nexus.node).await?;

            #[tracing::instrument(skip(nexus, node), fields(nexus.uuid = %nexus.uuid, request.reconcile = true))]
            async fn faulted_nexus_remover(
                nexus: NexusSpec,
                node: Arc<RwLock<NodeWrapper>>,
            ) -> PollResult {
                nexus.warn(
                    "Removing Faulted Nexus so it can be recreated with its healthy children",
                );

                // destroy the nexus - it will be recreated by the missing_nexus reconciler!
                match node.destroy_nexus(&nexus.clone().into()).await {
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
            if node_online && !healthy_children.candidates().is_empty() {
                faulted_nexus_remover(nexus, node).await?;
            }
        }
    }

    PollResult::Ok(PollerState::Idle)
}

/// Find thin-provisioned Nexus children which are degraded due to ENOSPC and fault them.
/// fixme: This is just a place-holder for the actual logic.
#[tracing::instrument(skip(nexus, context), level = "trace", fields(nexus.uuid = %nexus.uuid(), request.reconcile = true))]
pub(super) async fn enospc_children_faulter(
    nexus: &mut OperationGuardArc<NexusSpec>,
    context: &PollContext,
) -> PollResult {
    let nexus_uuid = nexus.uuid();
    let nexus_state = context.registry().get_nexus(nexus_uuid).await?;
    let child_count = nexus_state.children.len();

    if nexus_state.status == NexusStatus::Degraded && child_count > 1 {
        for child in nexus_state.children.iter().filter(|c| c.enospc()) {
            nexus.warn_span(|| {
                tracing::info!(child.uri = child.uri.as_str(), "Found child with enospc")
            });
            let node = context
                .registry()
                .get_node_wrapper(&nexus_state.node)
                .await?;
            node.fault_child(&FaultNexusChild {
                node: nexus_state.node.clone(),
                nexus: nexus_state.uuid.clone(),
                uri: child.uri.clone(),
            })
            .await?;
        }
    }

    PollResult::Ok(PollerState::Idle)
}
