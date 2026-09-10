use crate::controller::{resources::operations::ResourceDrain, task_poller::{PollContext, PollResult, PollerState::Idle}};



pub(crate) async fn pool_drain_enqueuer(context: &PollContext) -> PollResult {
    // Ordered oldest drain request first, so promotion out of the queue is FIFO.
    let pools = context.specs().pools_rsc_drain_queued();
    
    for pool in pools {
    /*if let Ok(gp) = context.specs().guarded_pool(&pool.id).await {
        gp.drain(registry, request)
    }*/
    }
    Ok(Idle)
}