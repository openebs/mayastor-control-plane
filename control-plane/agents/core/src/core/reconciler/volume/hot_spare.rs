use crate::{
    core::{
        reconciler::{nexus, PollContext, TaskPoller},
        specs::OperationSequenceGuard,
        task_poller::{squash_results, PollResult, PollerState},
    },
    volume::specs::get_volume_replica_candidates,
};

use common::errors::NexusNotFound;
use common_lib::{
    mbus_api::ErrorChain,
    types::v0::{
        message_bus::{VolumeState, VolumeStatus},
        store::{nexus::NexusSpec, volume::VolumeSpec, OperationMode},
    },
};

use common_lib::types::v0::store::{TraceSpan, TraceStrLog};
use parking_lot::Mutex;
use snafu::OptionExt;
use std::{cmp::Ordering, sync::Arc};

/// Volume HotSpare reconciler
#[derive(Debug)]
pub(super) struct HotSpareReconciler {}
impl HotSpareReconciler {
    /// Return a new `Self`
    pub(super) fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl TaskPoller for HotSpareReconciler {
    async fn poll(&mut self, context: &PollContext) -> PollResult {
        let mut results = vec![];
        let volumes = context.specs().get_locked_volumes();
        for volume in volumes {
            results.push(hot_spare_reconcile(&volume, context).await);
        }
        Self::squash_results(results)
    }
}

#[tracing::instrument(level = "debug", skip(context, volume_spec), fields(volume.uuid = %volume_spec.lock().uuid, request.reconcile = true))]
async fn hot_spare_reconcile(
    volume_spec: &Arc<Mutex<VolumeSpec>>,
    context: &PollContext,
) -> PollResult {
    let uuid = volume_spec.lock().uuid.clone();
    let volume_state = context.registry().get_volume_state(&uuid).await?;
    let _guard = match volume_spec.operation_guard(OperationMode::ReconcileStart) {
        Ok(guard) => guard,
        Err(_) => return PollResult::Ok(PollerState::Busy),
    };
    let mode = OperationMode::ReconcileStep;

    if !volume_spec.lock().policy.self_heal {
        return PollResult::Ok(PollerState::Idle);
    }
    if !volume_spec.lock().status.created() {
        return PollResult::Ok(PollerState::Idle);
    }

    match volume_state.status {
        VolumeStatus::Online => volume_replica_count_reconciler(volume_spec, context, mode).await,
        VolumeStatus::Unknown | VolumeStatus::Degraded => {
            hot_spare_nexus_reconcile(volume_spec, &volume_state, context).await
        }
        VolumeStatus::Faulted => PollResult::Ok(PollerState::Idle),
    }
}

async fn hot_spare_nexus_reconcile(
    volume_spec: &Arc<Mutex<VolumeSpec>>,
    volume_state: &VolumeState,
    context: &PollContext,
) -> PollResult {
    let mode = OperationMode::ReconcileStep;
    let mut results = vec![];

    if let Some(nexus) = &volume_state.target {
        let nexus_spec = context.specs().get_nexus(&nexus.uuid);
        let nexus_spec = nexus_spec.context(NexusNotFound {
            nexus_id: nexus.uuid.to_string(),
        })?;
        let _guard = match nexus_spec.operation_guard(OperationMode::ReconcileStart) {
            Ok(guard) => guard,
            Err(_) => return PollResult::Ok(PollerState::Busy),
        };

        // generic nexus reconciliation (does not matter that it belongs to a volume)
        results.push(generic_nexus_reconciler(&nexus_spec, context, mode).await);

        // fixup the volume replica count: creates new replicas when we're behind
        // removes extra replicas but only if they're UNUSED (by a nexus)
        results.push(volume_replica_count_reconciler(volume_spec, context, mode).await);
        // fixup the nexus replica count to match the volume's replica count
        results.push(nexus_replica_count_reconciler(volume_spec, &nexus_spec, context, mode).await);
    } else {
        results.push(volume_replica_count_reconciler(volume_spec, context, mode).await);
    }

    squash_results(results)
}

#[tracing::instrument(skip(context, nexus_spec, mode), fields(nexus.uuid = %nexus_spec.lock().uuid))]
async fn generic_nexus_reconciler(
    nexus_spec: &Arc<Mutex<NexusSpec>>,
    context: &PollContext,
    mode: OperationMode,
) -> PollResult {
    let mut results = vec![];
    results.push(faulted_children_remover(nexus_spec, context, mode).await);
    results.push(unknown_children_remover(nexus_spec, context, mode).await);
    results.push(missing_children_remover(nexus_spec, context, mode).await);
    squash_results(results)
}

/// Given a degraded volume
/// When a nexus state has faulty children
/// Then they should eventually be removed from the state and spec
/// And the replicas should eventually be destroyed
async fn faulted_children_remover(
    nexus_spec: &Arc<Mutex<NexusSpec>>,
    context: &PollContext,
    mode: OperationMode,
) -> PollResult {
    nexus::faulted_children_remover(nexus_spec, context, mode).await
}

/// Given a degraded volume
/// When a nexus state has children that are not present in the spec
/// Then the children should eventually be removed from the state
/// And the uri's should not be destroyed
async fn unknown_children_remover(
    nexus_spec: &Arc<Mutex<NexusSpec>>,
    context: &PollContext,
    mode: OperationMode,
) -> PollResult {
    nexus::unknown_children_remover(nexus_spec, context, mode).await
}

/// Given a degraded volume
/// When a nexus spec has children that are not present in the state
/// Then the children should eventually be removed from the spec
/// And the replicas should eventually be destroyed
async fn missing_children_remover(
    nexus_spec: &Arc<Mutex<NexusSpec>>,
    context: &PollContext,
    mode: OperationMode,
) -> PollResult {
    nexus::missing_children_remover(nexus_spec, context, mode).await
}

/// Given a degraded volume
/// When the nexus spec has a different number of children to the number of volume replicas
/// Then the nexus spec should eventually have as many children as the number of volume replicas
#[tracing::instrument(skip(context, volume_spec, nexus_spec, mode), fields(nexus.uuid = %nexus_spec.lock().uuid))]
async fn nexus_replica_count_reconciler(
    volume_spec: &Arc<Mutex<VolumeSpec>>,
    nexus_spec: &Arc<Mutex<NexusSpec>>,
    context: &PollContext,
    mode: OperationMode,
) -> PollResult {
    let nexus_uuid = nexus_spec.lock().uuid.clone();
    let nexus_state = context.registry().get_nexus(&nexus_uuid).await?;

    let vol_spec_clone = volume_spec.lock().clone();
    let nexus_spec_clone = nexus_spec.lock().clone();
    let volume_replicas = vol_spec_clone.num_replicas as usize;
    let nexus_replica_children =
        nexus_spec_clone
            .children
            .iter()
            .fold(0usize, |mut counter, child| {
                // only account for children which are lvol replicas
                if let Some(replica) = child.as_replica() {
                    if context.specs().get_replica(replica.uuid()).is_some() {
                        counter += 1;
                    }
                }
                counter
            });

    match nexus_replica_children.cmp(&volume_replicas) {
        Ordering::Less => {
            nexus_spec_clone.warn_span(|| {
                tracing::warn!(
                    "The nexus only has '{}' replica(s) but the volume requires '{}' replica(s)",
                    nexus_replica_children,
                    volume_replicas
                )
            });
            context
                .specs()
                .attach_replicas_to_nexus(
                    context.registry(),
                    volume_spec,
                    nexus_spec,
                    &nexus_state,
                    mode,
                )
                .await?;
        }
        Ordering::Greater => {
            nexus_spec_clone.warn_span(|| {
                tracing::warn!(
                    "The nexus has more replicas(s) ('{}') than the required replica count ('{}')",
                    nexus_replica_children,
                    volume_replicas
                )
            });
            context
                .specs()
                .remove_excess_replicas_from_nexus(
                    context.registry(),
                    volume_spec,
                    nexus_spec,
                    &nexus_state,
                    mode,
                )
                .await?;
        }
        Ordering::Equal => {}
    }

    PollResult::Ok(if nexus_spec.lock().children.len() == volume_replicas {
        PollerState::Idle
    } else {
        PollerState::Busy
    })
}

/// Given a degraded volume
/// When the number of created volume replicas is different to the required number of replicas
/// Then the number of created volume replicas should eventually match the required number of
/// replicas
#[tracing::instrument(level = "debug", skip(context, volume_spec), fields(volume.uuid = %volume_spec.lock().uuid))]
async fn volume_replica_count_reconciler(
    volume_spec: &Arc<Mutex<VolumeSpec>>,
    context: &PollContext,
    mode: OperationMode,
) -> PollResult {
    let volume_spec_clone = volume_spec.lock().clone();
    let volume_uuid = volume_spec_clone.uuid.clone();
    let required_replica_count = volume_spec_clone.num_replicas as usize;

    let current_replicas = context.specs().get_volume_replicas(&volume_uuid);
    let mut current_replica_count = current_replicas.len();

    match current_replica_count.cmp(&required_replica_count) {
        Ordering::Less => {
            volume_spec_clone.warn_span(|| {
                tracing::warn!(
                    "The volume has '{}' replica(s) but it should have '{}'. Creating more...",
                    current_replica_count,
                    required_replica_count
                )
            });

            let diff = required_replica_count - current_replica_count;
            let candidates =
                get_volume_replica_candidates(context.registry(), &volume_spec_clone).await?;

            match context
                .specs()
                .create_volume_replicas(
                    context.registry(),
                    &volume_spec_clone,
                    candidates,
                    diff,
                    mode,
                )
                .await
            {
                result if result > 0 => {
                    current_replica_count += result;

                    volume_spec_clone.info_span(|| {
                        tracing::info!("Successfully created '{}' new replica(s)", result)
                    });
                }
                _ => {
                    volume_spec_clone.error("Failed to create replicas");
                }
            }
        }
        Ordering::Greater => {
            volume_spec_clone.warn_span(|| {
                tracing::warn!(
                    "The volume has '{}' replica(s) but it should only have '{}'. Removing...",
                    current_replica_count,
                    required_replica_count
                )
            });

            let diff = current_replica_count - required_replica_count;
            match context
                .specs()
                .remove_unused_volume_replicas(context.registry(), volume_spec, diff, mode)
                .await
            {
                Ok(_) => {
                    volume_spec_clone.info("Successfully removed unused replicas");
                }
                Err(error) => {
                    volume_spec_clone.warn_span(|| {
                        tracing::warn!(
                            "Failed to remove unused replicas from volume, error: '{}'",
                            error.full_string()
                        )
                    });
                }
            }
        }
        Ordering::Equal => {}
    }

    PollResult::Ok(if current_replica_count == required_replica_count {
        PollerState::Idle
    } else {
        PollerState::Busy
    })
}
