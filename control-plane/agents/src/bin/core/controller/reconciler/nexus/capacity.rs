use crate::{
    controller::{
        io_engine::NexusChildActionApi,
        registry::Registry,
        resources::{OperationGuardArc, TraceSpan},
        scheduling::pool::rebuild_space_required,
        task_poller::{PollContext, PollResult, PollerState},
    },
    node::wrapper::NexusChildActionContextNode,
};
use agents::errors::SvcError;

use stor_port::types::v0::{
    store::nexus::NexusSpec,
    transport::{Child, NexusChildActionContext, NexusStatus},
};

/// Find thin-provisioned Nexus children which are degraded due to ENOSPC and try to online them
/// if sufficient pool space has been freed.
#[tracing::instrument(skip(nexus, context), level = "trace", fields(nexus.uuid = %nexus.uuid(), request.reconcile = true))]
pub(crate) async fn enospc_children_onliner(
    nexus: &mut OperationGuardArc<NexusSpec>,
    context: &PollContext,
) -> PollResult {
    let nexus_uuid = nexus.uuid();
    let nexus_state = context.registry().nexus(nexus_uuid).await?;
    let child_count = nexus_state.children.len();

    if nexus_state.status == NexusStatus::Degraded && child_count > 1 {
        for child in nexus_state.children.iter().filter(|c| c.enospc()) {
            nexus.warn_span(|| {
                tracing::info!(child.uri = child.uri.as_str(), "Found child with enospc")
            });

            if let Err(error) = online_enospc(nexus, child, context.registry()).await {
                nexus.warn_span(|| {
                    tracing::error!(child.uri = child.uri.as_str(), %error, "Failed to online child");
                });
            }
        }
    }

    PollResult::Ok(PollerState::Idle)
}

pub(super) async fn child_enospc_onlineable(
    nexus: &mut OperationGuardArc<NexusSpec>,
    child: &Child,
    registry: &Registry,
) -> Result<(), SvcError> {
    let replica_uri = nexus
        .as_ref()
        .replica_uri(&child.uri)
        .ok_or(SvcError::Internal {
            // this should never happen for managed nexus..
            details: "Just a plain old uri, nothing we can do here..".into(),
        })?;
    let replica_spec = registry.specs().replica(replica_uri.uuid()).await?;
    let replica_state = registry.replica(replica_uri.uuid()).await?;

    let pool_name = replica_spec.as_ref().pool_name();
    let pool_wrapper = registry.pool_wrapper(pool_name).await?;

    // todo: Should we list pools to check for latest free space?
    tracing::debug!(
        pool.id = pool_wrapper.id.as_str(),
        pool.node = pool_wrapper.node.as_str(),
        pool.free_space = pool_wrapper.free_space(),
        "Reporting free space in pool"
    );

    // Don't bother proceeding if we don't have at least this much free space...
    let slack = 16 * 1024 * 1024;
    if pool_wrapper.free_space() < slack {
        nexus.info_span(|| {
            tracing::warn!(
                child.uri = %child.uri.as_str(),
                pool.id = pool_wrapper.id.as_str(),
                pool.node = pool_wrapper.node.as_str(),
                pool.free_space = pool_wrapper.free_space(),
                slack,
                "Not enough free_space slack to online enospc child",
            )
        });
        return Err(SvcError::NoCapacityToOnline {
            pool_id: pool_wrapper.id.to_string(),
            child: child.uri.to_string(),
            free_space: pool_wrapper.free_space(),
            required: slack,
        });
    }

    let children = nexus.as_ref().children.iter();
    let mut replicas = Vec::with_capacity(children.len());
    for r in children.flat_map(|c| c.as_replica()) {
        replicas.push(registry.replica(r.uuid()).await?);
    }
    // todo: this won't work for volumes with snapshots
    //  we need to figure out which blocks would require rebuild
    let min_allocated_bytes = replicas
        .into_iter()
        .flat_map(|r| r.space)
        .map(|r| r.allocated_bytes)
        .max()
        .unwrap_or_else(|| nexus.as_ref().size);

    let required_free_space = rebuild_space_required(min_allocated_bytes, &replica_state);
    if pool_wrapper.free_space() < required_free_space {
        nexus.info_span(|| {
            tracing::warn!(
                child.uri = %child.uri.as_str(),
                pool.free_space = pool_wrapper.free_space(),
                required_free_space,
                "Not enough free_space to online enospc child",
            )
        });
        return Err(SvcError::NoCapacityToOnline {
            pool_id: pool_wrapper.id.to_string(),
            child: child.uri.to_string(),
            free_space: pool_wrapper.free_space(),
            required: required_free_space,
        });
    }

    Ok(())
}

async fn online_enospc(
    nexus: &mut OperationGuardArc<NexusSpec>,
    child: &Child,
    registry: &Registry,
) -> Result<(), SvcError> {
    child_enospc_onlineable(nexus, child, registry).await?;

    let nexus_node = &nexus.as_ref().node;
    let node = registry.node_wrapper(nexus_node).await?;
    node.online_child(NexusChildActionContextNode::new(
        NexusChildActionContext::new(nexus_node, nexus.uuid(), &child.uri),
        registry,
    ))
    .await?;
    nexus.info_span(|| {
        tracing::info!(
            child.uri = %child.uri.as_str(),
            "Successfully onlined enospc child",
        )
    });

    Ok(())
}
