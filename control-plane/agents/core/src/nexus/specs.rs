use crate::controller::{
    registry::Registry,
    specs::{GuardedSpecOperations, ResourceSpecs, ResourceSpecsLocked, SpecOperations},
    wrapper::ClientOps,
};
use common::errors::SvcError;
use common_lib::{
    transport_api::{ErrorChain, ResourceKind},
    types::v0::{
        store::{
            nexus::{NexusOperation, NexusSpec},
            nexus_child::NexusChild,
            replica::ReplicaSpec,
            SpecStatus, SpecTransaction, TraceSpan,
        },
        transport::{
            AddNexusChild, AddNexusReplica, Child, ChildUri, CreateNexus, DestroyNexus, Nexus,
            NexusId, NexusStatus, RemoveNexusChild, RemoveNexusReplica, ReplicaOwners, ShareNexus,
            UnshareNexus,
        },
    },
};

use crate::controller::specs::OperationSequenceGuard;
use common::errors::SvcError::CordonedNode;
use common_lib::types::v0::store::OperationGuardArc;
use parking_lot::Mutex;
use std::sync::Arc;

#[async_trait::async_trait]
impl GuardedSpecOperations for OperationGuardArc<NexusSpec> {
    type Create = CreateNexus;
    type Owners = ();
    type Status = NexusStatus;
    type State = Nexus;
    type UpdateOp = NexusOperation;
    type Inner = NexusSpec;

    fn remove_spec(&self, registry: &Registry) {
        let uuid = self.lock().uuid.clone();
        registry.specs().remove_nexus(&uuid);
    }
}

#[async_trait::async_trait]
impl SpecOperations for NexusSpec {
    type Create = CreateNexus;
    type Owners = ();
    type Status = NexusStatus;
    type State = Nexus;
    type UpdateOp = NexusOperation;

    async fn start_update_op(
        &mut self,
        _: &Registry,
        state: &Self::State,
        op: Self::UpdateOp,
    ) -> Result<(), SvcError> {
        match &op {
            NexusOperation::Share(_) if state.share.shared() => Err(SvcError::AlreadyShared {
                kind: ResourceKind::Nexus,
                id: self.uuid_str(),
                share: state.share.to_string(),
            }),
            NexusOperation::Share(_) => Ok(()),
            NexusOperation::Unshare if !state.share.shared() => Err(SvcError::NotShared {
                kind: ResourceKind::Nexus,
                id: self.uuid_str(),
            }),
            NexusOperation::Unshare => Ok(()),
            NexusOperation::AddChild(child) if self.children.contains(child) => {
                Err(SvcError::ChildAlreadyExists {
                    nexus: self.uuid_str(),
                    child: child.to_string(),
                })
            }
            NexusOperation::AddChild(_) => Ok(()),
            NexusOperation::RemoveChild(child)
                if !self.children.contains(child) && !state.contains_child(&child.uri()) =>
            {
                Err(SvcError::ChildNotFound {
                    nexus: self.uuid_str(),
                    child: child.to_string(),
                })
            }
            NexusOperation::RemoveChild(_) => Ok(()),
            _ => unreachable!(),
        }?;
        self.start_op(op);
        Ok(())
    }
    fn start_create_op(&mut self) {
        self.start_op(NexusOperation::Create);
    }
    fn start_destroy_op(&mut self) {
        self.start_op(NexusOperation::Destroy);
    }

    fn dirty(&self) -> bool {
        self.pending_op()
    }
    fn kind(&self) -> ResourceKind {
        ResourceKind::Nexus
    }
    fn uuid_str(&self) -> String {
        self.uuid.to_string()
    }
    fn status(&self) -> SpecStatus<NexusStatus> {
        self.spec_status.clone()
    }
    fn set_status(&mut self, status: SpecStatus<Self::Status>) {
        self.spec_status = status;
    }
    fn owned(&self) -> bool {
        self.owner.is_some()
    }
    fn owners(&self) -> Option<String> {
        self.owner.clone().map(|o| format!("{:?}", o))
    }
    fn disown_all(&mut self) {
        self.owner.take();
    }
    fn operation_result(&self) -> Option<Option<bool>> {
        self.operation.as_ref().map(|r| r.result)
    }
}

/// Implementation of the ResourceSpecs which is retrieved from the ResourceSpecsLocked
/// During these calls, no other thread can add/remove elements from the list
impl ResourceSpecs {
    /// Get all NexusSpec's
    pub fn get_nexuses(&self) -> Vec<NexusSpec> {
        let mut vector = vec![];
        for object in self.nexuses.to_vec() {
            let object = object.lock();
            vector.push(object.clone());
        }
        vector
    }
    /// Get all NexusSpec's which are in a created state
    pub fn get_created_nexuses(&self) -> Vec<NexusSpec> {
        let mut nexuses = vec![];
        for nexus in self.nexuses.to_vec() {
            let nexus = nexus.lock();
            if nexus.spec_status.created() || nexus.spec_status.deleting() {
                nexuses.push(nexus.clone());
            }
        }
        nexuses
    }
}

impl ResourceSpecsLocked {
    /// Get a list of created NexusSpec's
    #[allow(dead_code)]
    pub fn get_created_nexus_specs(&self) -> Vec<NexusSpec> {
        let specs = self.read();
        specs.get_created_nexuses()
    }
    /// Get the protected NexusSpec for the given nexus `id`, if any exists
    pub fn get_nexus(&self, id: &NexusId) -> Option<Arc<Mutex<NexusSpec>>> {
        let specs = self.read();
        specs.nexuses.get(id).cloned()
    }
    /// Get the guarded NexusSpec for the given nexus `id`, if any exists.
    pub(crate) async fn nexus_opt(
        &self,
        nexus: &NexusId,
    ) -> Result<Option<OperationGuardArc<NexusSpec>>, SvcError> {
        Ok(match self.get_nexus(nexus) {
            None => None,
            Some(nexus) => Some(nexus.operation_guard_wait().await?),
        })
    }
    /// Get the guarded NexusSpec for the given nexus `id`, if any exists.
    pub(crate) async fn nexus(
        &self,
        nexus: &NexusId,
    ) -> Result<OperationGuardArc<NexusSpec>, SvcError> {
        match self.get_nexus(nexus) {
            None => Err(SvcError::NexusNotFound {
                nexus_id: nexus.to_string(),
            }),
            Some(nexus) => Ok(nexus.operation_guard_wait().await?),
        }
    }
    /// Get or Create the protected NexusSpec for the given request
    fn get_or_create_nexus(&self, request: &CreateNexus) -> Arc<Mutex<NexusSpec>> {
        let mut specs = self.write();
        if let Some(nexus) = specs.nexuses.get(&request.uuid) {
            nexus.clone()
        } else {
            specs.nexuses.insert(NexusSpec::from(request))
        }
    }

    pub async fn create_nexus(
        &self,
        registry: &Registry,
        request: &CreateNexus,
    ) -> Result<(OperationGuardArc<NexusSpec>, Nexus), SvcError> {
        if registry.node_cordoned(&request.node)? {
            return Err(CordonedNode {
                node_id: request.node.to_string(),
            });
        }

        let node = registry.get_node_wrapper(&request.node).await?;

        let nexus_spec = self.get_or_create_nexus(request);
        let guard = nexus_spec.operation_guard_wait().await?;
        let _ = guard.start_create(registry, request).await?;

        let result = node.create_nexus(request).await;
        self.on_create_set_owners(request, &nexus_spec, &result);

        let nexus_state = guard.complete_create(result, registry).await?;
        Ok((guard, nexus_state))
    }

    fn on_create_set_owners(
        &self,
        request: &CreateNexus,
        spec: &Arc<Mutex<NexusSpec>>,
        result: &Result<Nexus, SvcError>,
    ) {
        if let Ok(nexus) = result {
            if let Some(uuid) = &request.owner {
                let nexus_replicas = spec
                    .lock()
                    .children
                    .iter()
                    .filter_map(|r| r.as_replica())
                    .collect::<Vec<_>>();
                let replicas = self.get_volume_replicas(uuid);
                replicas.into_iter().for_each(|replica_spec| {
                    let mut spec = replica_spec.lock();
                    if nexus_replicas.iter().any(|r| r.uuid() == &spec.uuid) {
                        spec.owners.add_owner(&nexus.uuid);
                    }
                });
            }
        }
    }

    pub async fn destroy_nexus(
        &self,
        nexus: Option<&OperationGuardArc<NexusSpec>>,
        registry: &Registry,
        request: &DestroyNexus,
        delete_owned: bool,
    ) -> Result<(), SvcError> {
        let node = registry.get_node_wrapper(&request.node).await?;

        if let Some(nexus) = nexus {
            nexus.start_destroy(registry, delete_owned).await?;

            let result = node.destroy_nexus(request).await;
            self.on_delete_disown_replicas(nexus);
            nexus.complete_destroy(result, registry).await
        } else {
            node.destroy_nexus(request).await
        }
    }

    fn on_delete_disown_replicas(&self, spec: &Arc<Mutex<NexusSpec>>) {
        let nexus = spec.lock().clone();
        let children = &nexus.children;
        let replicas = children.iter().filter_map(|r| r.as_replica());
        replicas.for_each(|replica| {
            if let Some(replica) = self.get_replica(replica.uuid()) {
                let mut replica = replica.lock();
                replica.disown(&ReplicaOwners::new(None, vec![nexus.uuid.clone()]));
            }
        });
    }

    pub async fn share_nexus(
        &self,
        nexus: Option<&OperationGuardArc<NexusSpec>>,
        registry: &Registry,
        request: &ShareNexus,
    ) -> Result<String, SvcError> {
        let node = registry.get_node_wrapper(&request.node).await?;

        if let Some(nexus) = nexus {
            let status = registry.get_nexus(&request.uuid).await?;
            let spec_clone = nexus
                .start_update(registry, &status, NexusOperation::Share(request.protocol))
                .await?;

            let result = node.share_nexus(request).await;
            nexus.complete_update(registry, result, spec_clone).await
        } else {
            node.share_nexus(request).await
        }
    }

    pub async fn unshare_nexus(
        &self,
        nexus: Option<&OperationGuardArc<NexusSpec>>,
        registry: &Registry,
        request: &UnshareNexus,
    ) -> Result<(), SvcError> {
        let node = registry.get_node_wrapper(&request.node).await?;

        if let Some(nexus) = nexus {
            let status = registry.get_nexus(&request.uuid).await?;
            let spec_clone = nexus
                .start_update(registry, &status, NexusOperation::Unshare)
                .await?;

            let result = node.unshare_nexus(request).await;
            nexus.complete_update(registry, result, spec_clone).await
        } else {
            node.unshare_nexus(request).await
        }
    }

    pub async fn add_nexus_child(
        &self,
        nexus: Option<&OperationGuardArc<NexusSpec>>,
        registry: &Registry,
        request: &AddNexusChild,
    ) -> Result<Child, SvcError> {
        let node = registry.get_node_wrapper(&request.node).await?;

        if let Some(nexus) = nexus {
            let status = registry.get_nexus(&request.nexus).await?;
            let spec_clone = nexus
                .start_update(
                    registry,
                    &status,
                    NexusOperation::AddChild(NexusChild::from(&request.uri)),
                )
                .await?;

            let result = node.add_child(request).await;
            nexus.complete_update(registry, result, spec_clone).await
        } else {
            node.add_child(request).await
        }
    }

    pub async fn add_nexus_replica(
        &self,
        nexus: Option<&OperationGuardArc<NexusSpec>>,
        registry: &Registry,
        request: &AddNexusReplica,
    ) -> Result<Child, SvcError> {
        let node = registry.get_node_wrapper(&request.node).await?;

        if let Some(nexus) = nexus {
            let status = registry.get_nexus(&request.nexus).await?;
            let spec_clone = nexus
                .start_update(
                    registry,
                    &status,
                    NexusOperation::AddChild(NexusChild::from(&request.replica)),
                )
                .await?;

            let result = node.add_child(&request.into()).await;
            self.on_add_own_replica(request, &result);
            nexus.complete_update(registry, result, spec_clone).await
        } else {
            Err(SvcError::NexusNotFound {
                nexus_id: request.nexus.to_string(),
            })
        }
    }

    fn on_add_own_replica(&self, request: &AddNexusReplica, result: &Result<Child, SvcError>) {
        if result.is_ok() {
            if let Some(replica) = self.get_replica(request.replica.uuid()) {
                let mut replica = replica.lock();
                replica.owners.add_owner(&request.nexus);
            }
        }
    }

    #[tracing::instrument(level = "debug", err)]
    pub async fn remove_nexus_child(
        &self,
        nexus: Option<&OperationGuardArc<NexusSpec>>,
        registry: &Registry,
        request: &RemoveNexusChild,
    ) -> Result<(), SvcError> {
        let node = registry.get_node_wrapper(&request.node).await?;

        if let Some(nexus) = nexus {
            let status = registry.get_nexus(&request.nexus).await?;
            let spec_clone = nexus
                .start_update(
                    registry,
                    &status,
                    NexusOperation::RemoveChild(NexusChild::from(&request.uri)),
                )
                .await?;

            let result = node.remove_child(request).await;
            nexus.complete_update(registry, result, spec_clone).await
        } else {
            node.remove_child(request).await
        }
    }

    pub async fn remove_nexus_replica(
        &self,
        nexus: Option<&OperationGuardArc<NexusSpec>>,
        registry: &Registry,
        request: &RemoveNexusReplica,
    ) -> Result<(), SvcError> {
        let node = registry.get_node_wrapper(&request.node).await?;

        if let Some(nexus) = nexus {
            let status = registry.get_nexus(&request.nexus).await?;
            let spec_clone = nexus
                .start_update(
                    registry,
                    &status,
                    NexusOperation::RemoveChild(NexusChild::from(&request.replica)),
                )
                .await?;

            let result = node.remove_child(&request.into()).await;
            self.on_remove_disown_replica(request, &result);

            nexus.complete_update(registry, result, spec_clone).await
        } else {
            Err(SvcError::NexusNotFound {
                nexus_id: request.nexus.to_string(),
            })
        }
    }

    /// Remove a nexus child uri
    /// If it's a replica it also disowns the replica from the volume and attempts to destroy it,
    /// if requested.
    pub async fn remove_nexus_child_by_uri(
        &self,
        registry: &Registry,
        nexus_guard: &OperationGuardArc<NexusSpec>,
        nexus: &Nexus,
        uri: &ChildUri,
        destroy_replica: bool,
    ) -> Result<(), SvcError> {
        let nexus_children = nexus_guard.lock().children.clone();
        match nexus_children.into_iter().find(|c| &c.uri() == uri) {
            Some(NexusChild::Replica(replica)) => {
                let request = RemoveNexusReplica::new(&nexus.node, &nexus.uuid, &replica);
                match self
                    .remove_nexus_replica(Some(nexus_guard), registry, &request)
                    .await
                {
                    Ok(_) if destroy_replica => {
                        let replica_spec =
                            self.get_replica(replica.uuid())
                                .ok_or(SvcError::ReplicaNotFound {
                                    replica_id: replica.uuid().clone(),
                                })?;
                        let pool_id = replica_spec.lock().pool.clone();
                        match Self::get_pool_node(registry, pool_id).await {
                            Some(node) => {
                                if let Err(error) = self
                                    .disown_and_destroy_replica(registry, &node, replica.uuid())
                                    .await
                                {
                                    nexus_guard.lock().clone().error_span(|| {
                                        tracing::error!(
                                            replica.uuid = %replica.uuid(),
                                            error = %error.full_string(),
                                            "Failed to disown and destroy the replica"
                                        )
                                    });
                                }
                            }
                            None => {
                                // The replica node was not found (perhaps because it is offline).
                                // The replica can't be destroyed because the node isn't there.
                                // Instead, disown the replica from the volume and let the garbage
                                // collector destroy it later.
                                nexus_guard.lock().clone().warn_span(|| {
                                    tracing::warn!(
                                        replica.uuid = %replica.uuid(),
                                        "Failed to find the node where the replica is hosted"
                                    )
                                });
                                let _ = self.disown_volume_replica(registry, &replica_spec).await;
                            }
                        }

                        Ok(())
                    }
                    result => result,
                }
            }
            Some(NexusChild::Uri(uri)) => {
                let request = RemoveNexusChild::new(&nexus.node, &nexus.uuid, &uri);
                self.remove_nexus_child(Some(nexus_guard), registry, &request)
                    .await
            }
            None => {
                let request = RemoveNexusChild::new(&nexus.node, &nexus.uuid, uri);
                self.remove_nexus_child(Some(nexus_guard), registry, &request)
                    .await
            }
        }
    }

    fn on_remove_disown_replica(
        &self,
        request: &RemoveNexusReplica,
        result: &Result<(), SvcError>,
    ) {
        if result.is_ok() {
            if let Some(replica) = self.get_replica(request.replica.uuid()) {
                replica
                    .lock()
                    .disown(&ReplicaOwners::new(None, vec![request.nexus.clone()]));
            }
        }
    }

    /// Remove nexus by its `id`
    pub(super) fn remove_nexus(&self, id: &NexusId) {
        let mut specs = self.write();
        specs.nexuses.remove(id);
    }
    /// Get a vector of protected NexusSpec's
    pub fn get_nexuses(&self) -> Vec<Arc<Mutex<NexusSpec>>> {
        let specs = self.read();
        specs.nexuses.to_vec()
    }
    /// Get a list of protected ReplicaSpec's used by the given nexus spec
    pub(crate) fn get_nexus_replicas(&self, nexus: &NexusSpec) -> Vec<Arc<Mutex<ReplicaSpec>>> {
        self.read()
            .replicas
            .values()
            .filter(|r| nexus.contains_replica(&r.lock().uuid))
            .cloned()
            .collect()
    }

    /// Worker that reconciles dirty NexusSpecs's with the persistent store.
    /// This is useful when nexus operations are performed but we fail to
    /// update the spec with the persistent store.
    pub async fn reconcile_dirty_nexuses(&self, registry: &Registry) -> bool {
        let mut pending_ops = false;
        let nexuses = self.get_nexuses();
        for nexus in nexuses {
            if let Ok(guard) = nexus.operation_guard() {
                if !guard.handle_incomplete_ops(registry).await {
                    // Not all pending operations could be handled.
                    pending_ops = true;
                }
            }
        }
        pending_ops
    }
}
