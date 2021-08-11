use crate::core::task_poller::{PollContext, PollResult, PollerState};
use common_lib::{
    mbus_api::ErrorChain,
    types::v0::store::{nexus::NexusSpec, OperationMode},
};

use parking_lot::Mutex;
use std::sync::Arc;

/// Find and removes faulted children from the given nexus
/// If the child is a replica it also disowns and destroys it
pub(super) async fn faulted_children_remover(
    nexus_spec: &Arc<Mutex<NexusSpec>>,
    context: &PollContext,
    mode: OperationMode,
) -> PollResult {
    let nexus_uuid = nexus_spec.lock().uuid.clone();
    let nexus_state = context.registry().get_nexus(&nexus_uuid).await?;
    for child in nexus_state.children.iter().filter(|c| c.state.faulted()) {
        tracing::warn!(
            "Faulted child '{}' of Nexus '{}' needs to be replaced",
            child.uri,
            nexus_spec.lock().uuid
        );
        if let Err(error) = context
            .registry()
            .specs
            .remove_nexus_child_by_uri(context.registry(), &nexus_state, &child.uri, true, mode)
            .await
        {
            tracing::error!(
                "Failed to remove faulted child '{}' of nexus '{}', error: '{}'",
                child.uri,
                nexus_state.uuid,
                error.full_string(),
            );
        } else {
            tracing::info!(
                "Successfully removed faulted child '{}' of Nexus '{}'.",
                child.uri,
                nexus_spec.lock().uuid
            );
        }
    }

    PollResult::Ok(PollerState::Idle)
}

/// Find and removes unknown children from the given nexus
/// If the child is a replica it also disowns and destroys it
pub(super) async fn unknown_children_remover(
    nexus_spec: &Arc<Mutex<NexusSpec>>,
    context: &PollContext,
    mode: OperationMode,
) -> PollResult {
    let nexus_uuid = nexus_spec.lock().uuid.clone();
    let nexus_state = context.registry().get_nexus(&nexus_uuid).await?;
    let state_children = nexus_state.children.iter();
    let spec_children = nexus_spec.lock().children.clone();

    for child in state_children.filter(|c| !spec_children.iter().any(|spec| spec.uri() == c.uri)) {
        tracing::warn!(
            "Unknown child '{}' of Nexus '{}' needs to be removed",
            child.uri,
            nexus_spec.lock().uuid
        );
        if let Err(error) = context
            .registry()
            .specs
            .remove_nexus_child_by_uri(context.registry(), &nexus_state, &child.uri, false, mode)
            .await
        {
            tracing::error!(
                "Failed to remove unknown child '{}' of Nexus '{}', error: '{}'",
                child.uri,
                nexus_state.uuid,
                error.full_string(),
            );
        } else {
            tracing::info!(
                "Successfully removed unknown child '{}' of Nexus '{}'.",
                child.uri,
                nexus_spec.lock().uuid
            );
        }
    }

    PollResult::Ok(PollerState::Idle)
}

/// Find missing children from the given nexus
/// They are removed from the spec as we don't know why they got removed, so it's safer
/// to just disown and destroy them.
pub(super) async fn missing_children_remover(
    nexus_spec: &Arc<Mutex<NexusSpec>>,
    context: &PollContext,
    mode: OperationMode,
) -> PollResult {
    let nexus_uuid = nexus_spec.lock().uuid.clone();
    let nexus_state = context.registry().get_nexus(&nexus_uuid).await?;
    let spec_children = nexus_spec.lock().children.clone().into_iter();

    let mut result = PollResult::Ok(PollerState::Idle);
    for child in
        spec_children.filter(|spec| !nexus_state.children.iter().any(|c| c.uri == spec.uri()))
    {
        tracing::warn!(
            "Child '{}' is missing from Nexus '{}'. It may have been removed for a reason so it will be replaced with another",
            child.uri(),
            nexus_spec.lock().uuid
        );

        if let Err(error) = context
            .registry()
            .specs
            .remove_nexus_child_by_uri(context.registry(), &nexus_state, &child.uri(), true, mode)
            .await
        {
            tracing::error!(
                "Failed to remove child '{}' from the spec of nexus '{}', error: '{}'",
                child.uri(),
                nexus_state.uuid,
                error.full_string(),
            );
            result = PollResult::Err(error);
        }
    }

    result
}
