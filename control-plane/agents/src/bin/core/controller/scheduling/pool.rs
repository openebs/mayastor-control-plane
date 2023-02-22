use crate::controller::{
    registry::Registry,
    resources::{ResourceMutex, ResourceUid},
    scheduling::{ResourceData, ResourceFilter},
};
use agents::errors::SvcError;
use stor_port::types::v0::{
    store::{
        nexus::{NexusSpec, ReplicaUri},
        pool::PoolSpec,
        replica::ReplicaSpec,
    },
    transport::{Child, NexusId, NexusStatus, NodeId},
};

#[derive(Clone)]
pub(crate) struct NexusChildrenENoSpcPoolsCtx {
    #[allow(unused)]
    registry: Registry,
}

/// ResourceData context for getting pools with failed ENOSPC NexusChildren's replicas.
#[derive(Clone)]
pub(crate) struct NexusChildrenENoSpcPools {
    data: ResourceData<NexusChildrenENoSpcPoolsCtx, ENoSpcPool>,
}

#[async_trait::async_trait(?Send)]
impl ResourceFilter for NexusChildrenENoSpcPools {
    type Request = NexusChildrenENoSpcPoolsCtx;
    type Item = ENoSpcPool;

    fn data(&mut self) -> &mut ResourceData<Self::Request, Self::Item> {
        &mut self.data
    }

    fn collect(self) -> Vec<Self::Item> {
        self.data.list
    }
}

impl NexusChildrenENoSpcPools {
    async fn builder(registry: &Registry) -> Self {
        let list = nexus_enospc_pools(registry).await;
        Self {
            data: ResourceData::new(
                NexusChildrenENoSpcPoolsCtx {
                    registry: registry.clone(),
                },
                list,
            ),
        }
    }

    /// Builder used to retrieve a list of pools with their respective ENOSPC replicas.
    /// No filtering is done yet until we decide on what the best strategy is.
    pub(crate) async fn builder_with_defaults(registry: &Registry) -> Self {
        Self::builder(registry).await
    }
}

/// A pool spec with a list of its failed ENOSPC replicas.
/// # Note
/// The replica itself is not flagged as ENOSPC, rather it's via the `NexusChild`
/// that we find this information.
#[derive(Debug, Clone)]
pub(crate) struct ENoSpcPool {
    pool: PoolSpec,
    repl: Vec<ENoSpcReplica>,
}
/// A replica whose equivalent `NexusChild` has hit ENOSPC.
#[derive(Debug, Clone)]
pub(crate) struct ENoSpcReplica {
    nexus: ResourceMutex<NexusSpec>,
    replica: ReplicaSpec,
    child: ReplicaUri,
    repl_node: NodeId,
}
impl ENoSpcPool {
    /// Collect the spec and replicas.
    pub(crate) fn into_parts(self) -> (PoolSpec, Vec<ENoSpcReplica>) {
        (self.pool, self.repl)
    }
}
impl ENoSpcReplica {
    /// Get a reference to the replica.
    pub(crate) fn replica(&self) -> &ReplicaSpec {
        &self.replica
    }
    /// Get a reference to the nexus.
    pub(crate) fn nexus(&self) -> &ResourceMutex<NexusSpec> {
        &self.nexus
    }
    /// Get a reference to the replica node.
    pub(crate) fn repl_node(&self) -> &NodeId {
        &self.repl_node
    }
    /// Get a reference to the child.
    pub(crate) fn child(&self) -> &ReplicaUri {
        &self.child
    }
}

async fn nexus_enospc_pools(registry: &Registry) -> Vec<ENoSpcPool> {
    let mut enospc_children = Vec::new();

    for nexus in registry.specs().nexuses() {
        if let Ok(raw_children) = enospc_nexus_children(&nexus, registry).await {
            enospc_children.extend(raw_children.into_iter().map(|c| (nexus.clone(), c)));
        }
    }

    let mut pools = Vec::<ENoSpcPool>::new();
    for (nexus, child) in enospc_children {
        if let Ok(repl) = registry.specs().replica(child.uuid()).await {
            let pool = repl.as_ref().pool.pool_name();
            if let Ok(pool) = registry.specs().pool(pool) {
                let node = pool.node.clone();
                match pools.iter_mut().find(|p| p.pool.uid() == pool.uid()) {
                    None => {
                        pools.push(ENoSpcPool {
                            pool,
                            repl: vec![ENoSpcReplica {
                                nexus: nexus.clone(),
                                replica: repl.lock().clone(),
                                child,
                                repl_node: node,
                            }],
                        });
                    }
                    Some(pool) => {
                        pool.repl.push(ENoSpcReplica {
                            nexus: nexus.clone(),
                            replica: repl.lock().clone(),
                            child,
                            repl_node: node,
                        });
                    }
                }
            }
        }
    }

    pools
}

async fn enospc_nexus_children(
    nexus: &ResourceMutex<NexusSpec>,
    registry: &Registry,
) -> Result<Vec<ReplicaUri>, SvcError> {
    let children_states = enospc_children(nexus.uuid(), registry).await?;
    let children_specs = {
        let nexus = nexus.lock();
        children_states
            .into_iter()
            .flat_map(|state| nexus.replica_uri(&state.uri))
            .cloned()
            .collect::<Vec<_>>()
    };

    Ok(children_specs)
}

async fn enospc_children(
    nexus_uuid: &NexusId,
    registry: &Registry,
) -> Result<Vec<Child>, SvcError> {
    let nexus_state = registry.nexus(nexus_uuid).await?;
    if nexus_state.status == NexusStatus::Degraded {
        let enospc = nexus_state
            .children
            .into_iter()
            .filter(|c| c.enospc())
            .collect::<Vec<_>>();
        return Ok(enospc);
    }
    Ok(vec![])
}
