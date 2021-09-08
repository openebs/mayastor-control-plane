use crate::core::task_poller::{PollContext, PollResult, PollerState};
use common_lib::{
    mbus_api::ErrorChain,
    types::v0::store::{nexus::NexusSpec, OperationMode},
};

use common_lib::types::v0::store::{TraceSpan, TraceStrLog};
use parking_lot::Mutex;
use std::sync::Arc;

/// Find and removes faulted children from the given nexus
/// If the child is a replica it also disowns and destroys it
#[tracing::instrument(skip(nexus_spec, context, mode), fields(nexus.uuid = %nexus_spec.lock().uuid))]
pub(super) async fn faulted_children_remover(
    nexus_spec: &Arc<Mutex<NexusSpec>>,
    context: &PollContext,
    mode: OperationMode,
) -> PollResult {
    let nexus_spec_clone = nexus_spec.lock().clone();
    let nexus_uuid = nexus_spec_clone.uuid.clone();
    let nexus_state = context.registry().get_nexus(&nexus_uuid).await?;
    for child in nexus_state.children.iter().filter(|c| c.state.faulted()) {
        nexus_spec_clone
            .warn_span(|| tracing::warn!("Attempting to remove faulted child '{}'", child.uri));
        if let Err(error) = context
            .specs()
            .remove_nexus_child_by_uri(context.registry(), &nexus_state, &child.uri, true, mode)
            .await
        {
            nexus_spec_clone.error(&format!(
                "Failed to remove faulted child '{}', error: '{}'",
                child.uri,
                error.full_string(),
            ));
        } else {
            nexus_spec_clone.info(&format!(
                "Successfully removed faulted child '{}'",
                child.uri,
            ));
        }
    }

    PollResult::Ok(PollerState::Idle)
}

/// Find and removes unknown children from the given nexus
/// If the child is a replica it also disowns and destroys it
#[tracing::instrument(skip(nexus_spec, context, mode), fields(nexus.uuid = %nexus_spec.lock().uuid))]
pub(super) async fn unknown_children_remover(
    nexus_spec: &Arc<Mutex<NexusSpec>>,
    context: &PollContext,
    mode: OperationMode,
) -> PollResult {
    let nexus_spec_clone = nexus_spec.lock().clone();
    let nexus_uuid = nexus_spec_clone.uuid.clone();
    let nexus_state = context.registry().get_nexus(&nexus_uuid).await?;
    let state_children = nexus_state.children.iter();
    let spec_children = nexus_spec.lock().children.clone();

    for child in state_children.filter(|c| !spec_children.iter().any(|spec| spec.uri() == c.uri)) {
        nexus_spec_clone
            .warn_span(|| tracing::warn!("Attempting to remove unknown child '{}'", child.uri));
        if let Err(error) = context
            .specs()
            .remove_nexus_child_by_uri(context.registry(), &nexus_state, &child.uri, false, mode)
            .await
        {
            nexus_spec_clone.error(&format!(
                "Failed to remove unknown child '{}', error: '{}'",
                child.uri,
                error.full_string(),
            ));
        } else {
            nexus_spec_clone.info(&format!(
                "Successfully removed unknown child '{}'",
                child.uri,
            ));
        }
    }

    PollResult::Ok(PollerState::Idle)
}

/// Find missing children from the given nexus
/// They are removed from the spec as we don't know why they got removed, so it's safer
/// to just disown and destroy them.
#[tracing::instrument(skip(nexus_spec, context, mode), fields(nexus.uuid = %nexus_spec.lock().uuid))]
pub(super) async fn missing_children_remover(
    nexus_spec: &Arc<Mutex<NexusSpec>>,
    context: &PollContext,
    mode: OperationMode,
) -> PollResult {
    let nexus_spec_clone = nexus_spec.lock().clone();
    let nexus_uuid = nexus_spec_clone.uuid.clone();
    let nexus_state = context.registry().get_nexus(&nexus_uuid).await?;
    let spec_children = nexus_spec.lock().children.clone().into_iter();

    let mut result = PollResult::Ok(PollerState::Idle);
    for child in
        spec_children.filter(|spec| !nexus_state.children.iter().any(|c| c.uri == spec.uri()))
    {
        nexus_spec_clone.warn_span(|| tracing::warn!(
            "Attempting to remove missing child '{}'. It may have been removed for a reason so it will be replaced with another",
            child.uri(),
        ));

        if let Err(error) = context
            .specs()
            .remove_nexus_child_by_uri(context.registry(), &nexus_state, &child.uri(), true, mode)
            .await
        {
            nexus_spec_clone.error_span(|| {
                tracing::error!(
                    "Failed to remove child '{}' from the nexus spec, error: '{}'",
                    child.uri(),
                    error.full_string(),
                )
            });
            result = PollResult::Err(error);
        } else {
            nexus_spec_clone.info_span(|| {
                tracing::info!(
                    "Successfully removed missing child '{}' from the nexus spec",
                    child.uri(),
                )
            });
        }
    }

    result
}
