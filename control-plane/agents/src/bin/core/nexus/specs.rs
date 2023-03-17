use crate::controller::{
    registry::Registry,
    resources::{
        operations_helper::{
            GuardedOperationsHelper, OperationSequenceGuard, ResourceSpecs, ResourceSpecsLocked,
            SpecOperationsHelper,
        },
        OperationGuardArc, ResourceMutex,
    },
};
use agents::errors::SvcError;
use stor_port::{
    transport_api::ResourceKind,
    types::v0::{
        store::{
            nexus::{NexusOperation, NexusSpec},
            replica::ReplicaSpec,
            SpecStatus, SpecTransaction,
        },
        transport::{CreateNexus, Nexus, NexusId, NexusOwners, NexusStatus, ReplicaOwners},
    },
};

#[async_trait::async_trait]
impl GuardedOperationsHelper for OperationGuardArc<NexusSpec> {
    type Create = CreateNexus;
    type Owners = NexusOwners;
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
impl SpecOperationsHelper for NexusSpec {
    type Create = CreateNexus;
    type Owners = NexusOwners;
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
            NexusOperation::Share(_, _) if state.share.shared() => Err(SvcError::AlreadyShared {
                kind: ResourceKind::Nexus,
                id: self.uuid_str(),
                share: state.share.to_string(),
            }),
            NexusOperation::Share(_, _) => Ok(()),
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
            NexusOperation::Shutdown => Ok(()),
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
        self.owner.clone().map(|o| format!("{o:?}"))
    }
    fn disown(&mut self, owner: &Self::Owners) {
        if owner.disown_all() || self.owner.as_ref() == owner.volume() {
            let _ = self.owner.take();
        }
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
    pub(crate) fn nexuses(&self) -> Vec<NexusSpec> {
        let mut vector = vec![];
        for object in self.nexuses.to_vec() {
            let object = object.lock();
            vector.push(object.clone());
        }
        vector
    }
    /// Get all NexusSpec's which are in a created state
    pub(crate) fn created_nexuses(&self) -> Vec<NexusSpec> {
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
    pub(crate) fn created_nexus_specs(&self) -> Vec<NexusSpec> {
        let specs = self.read();
        specs.created_nexuses()
    }
    /// Get the resourced NexusSpec for the given nexus `id`, if any exists
    pub(crate) fn nexus_rsc(&self, id: &NexusId) -> Option<ResourceMutex<NexusSpec>> {
        let specs = self.read();
        specs.nexuses.get(id).cloned()
    }
    /// Get the guarded NexusSpec for the given nexus `id`, if any exists.
    pub(crate) async fn nexus_opt(
        &self,
        nexus: &NexusId,
    ) -> Result<Option<OperationGuardArc<NexusSpec>>, SvcError> {
        Ok(match self.nexus_rsc(nexus) {
            None => None,
            Some(nexus) => Some(nexus.operation_guard_wait().await?),
        })
    }
    /// Get the guarded NexusSpec for the given nexus `id`, if any exists.
    pub(crate) async fn nexus(
        &self,
        nexus: &NexusId,
    ) -> Result<OperationGuardArc<NexusSpec>, SvcError> {
        match self.nexus_rsc(nexus) {
            None => Err(SvcError::NexusNotFound {
                nexus_id: nexus.to_string(),
            }),
            Some(nexus) => Ok(nexus.operation_guard_wait().await?),
        }
    }
    /// Get or Create the resourced NexusSpec for the given request
    pub(crate) fn get_or_create_nexus(&self, request: &CreateNexus) -> ResourceMutex<NexusSpec> {
        let mut specs = self.write();
        if let Some(nexus) = specs.nexuses.get(&request.uuid) {
            nexus.clone()
        } else {
            specs.nexuses.insert(NexusSpec::from(request))
        }
    }

    pub(crate) fn on_create_set_owners(
        &self,
        request: &CreateNexus,
        spec: &ResourceMutex<NexusSpec>,
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
                let replicas = self.volume_replicas(uuid);
                replicas.into_iter().for_each(|replica_spec| {
                    let mut spec = replica_spec.lock();
                    if nexus_replicas.iter().any(|r| r.uuid() == &spec.uuid) {
                        spec.owners.add_owner(&nexus.uuid);
                    }
                });
            }
        }
    }

    pub(crate) fn on_delete_disown_replicas(&self, spec: &ResourceMutex<NexusSpec>) {
        let nexus = spec.lock().clone();
        let children = &nexus.children;
        let replicas = children.iter().filter_map(|r| r.as_replica());
        replicas.for_each(|replica| {
            if let Some(replica) = self.replica_rsc(replica.uuid()) {
                let mut replica = replica.lock();
                replica.disown(&ReplicaOwners::new(None, vec![nexus.uuid.clone()]));
            }
        });
    }

    /// Remove nexus by its `id`
    pub(super) fn remove_nexus(&self, id: &NexusId) {
        let mut specs = self.write();
        specs.nexuses.remove(id);
    }
    /// Get a vector of resourced NexusSpec's
    pub(crate) fn nexuses(&self) -> Vec<ResourceMutex<NexusSpec>> {
        let specs = self.read();
        specs.nexuses.to_vec()
    }
    /// Get a list of resourced ReplicaSpec's used by the given nexus spec
    pub(crate) fn nexus_replicas(&self, nexus: &NexusSpec) -> Vec<ResourceMutex<ReplicaSpec>> {
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
    pub(crate) async fn reconcile_dirty_nexuses(&self, registry: &Registry) -> bool {
        let mut pending_ops = false;
        let nexuses = self.nexuses();
        for nexus in nexuses {
            if let Ok(mut guard) = nexus.operation_guard() {
                if !guard.handle_incomplete_ops(registry).await {
                    // Not all pending operations could be handled.
                    pending_ops = true;
                }
            }
        }
        pending_ops
    }
}
