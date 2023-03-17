use crate::controller::{
    registry::Registry,
    resources::{
        operations::{ResourceLifecycle, ResourceOwnerUpdate},
        OperationGuardArc,
    },
};
use agents::{errors, errors::SvcError};
use snafu::OptionExt;
use stor_port::types::v0::{
    store::replica::{PoolRef, ReplicaSpec},
    transport::{DestroyReplica, NodeId, ReplicaOwners},
};

impl OperationGuardArc<ReplicaSpec> {
    /// Destroy the replica from its volume
    pub(crate) async fn destroy_volume_replica(
        &mut self,
        registry: &Registry,
        node_id: Option<&NodeId>,
    ) -> Result<(), SvcError> {
        let node_id = match node_id {
            Some(node_id) => node_id.clone(),
            None => {
                let replica_uuid = self.lock().uuid.clone();
                match registry.get_replica(&replica_uuid).await {
                    Ok(state) => state.node.clone(),
                    Err(_) => {
                        let pool_ref = self.lock().pool.clone();
                        let pool_id = match pool_ref {
                            PoolRef::Named(name) => name,
                            PoolRef::Uuid(name, _) => name,
                        };
                        let pool_spec = registry
                            .specs()
                            .pool_rsc(&pool_id)
                            .context(errors::PoolNotFound { pool_id })?;
                        let node_id = pool_spec.lock().node.clone();
                        node_id
                    }
                }
            }
        };

        self.destroy(
            registry,
            &self.destroy_request(ReplicaOwners::new_disown_all(), &node_id),
        )
        .await
    }

    /// Disown and destroy the replica from its volume
    pub(crate) async fn disown_and_destroy_replica(
        &mut self,
        registry: &Registry,
        node: &NodeId,
    ) -> Result<(), SvcError> {
        // disown it from the volume first, so at the very least it can be garbage collected
        // at a later point if the node is not accessible
        let disowner = ReplicaOwners::new_disown_all();
        self.remove_owners(registry, &disowner, true).await?;
        self.destroy_volume_replica(registry, Some(node)).await
    }

    /// Return a `DestroyReplica` request based on the provided arguments
    pub(crate) fn destroy_request(&self, by: ReplicaOwners, node: &NodeId) -> DestroyReplica {
        let spec = self.as_ref().clone();
        let pool_id = match spec.pool.clone() {
            PoolRef::Named(id) => id,
            PoolRef::Uuid(id, _) => id,
        };
        let pool_uuid = match spec.pool {
            PoolRef::Named(_) => None,
            PoolRef::Uuid(_, uuid) => Some(uuid),
        };
        DestroyReplica {
            node: node.clone(),
            pool_id,
            pool_uuid,
            uuid: spec.uuid,
            name: spec.name.into(),
            disowners: by,
        }
    }
}
