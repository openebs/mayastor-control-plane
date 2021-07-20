use parking_lot::Mutex;
use std::sync::Arc;

use snafu::OptionExt;

use crate::core::{
    registry::Registry,
    specs::{ResourceSpecs, ResourceSpecsLocked, SpecOperations},
    wrapper::ClientOps,
};
use common::errors::{NodeNotFound, SvcError};
use common_lib::{
    mbus_api::ResourceKind,
    types::v0::{
        message_bus::{
            AddNexusChild, Child, CreateNexus, DestroyNexus, Nexus, NexusId, NexusState,
            RemoveNexusChild, ShareNexus, UnshareNexus,
        },
        store::{
            nexus::{NexusOperation, NexusSpec},
            nexus_child::NexusChild,
            SpecState, SpecTransaction,
        },
    },
};

#[async_trait::async_trait]
impl SpecOperations for NexusSpec {
    type Create = CreateNexus;
    type State = NexusState;
    type Status = Nexus;
    type UpdateOp = NexusOperation;

    fn start_update_op(
        &mut self,
        status: &Self::Status,
        op: Self::UpdateOp,
    ) -> Result<(), SvcError> {
        match &op {
            NexusOperation::Share(_) if status.share.shared() => Err(SvcError::AlreadyShared {
                kind: ResourceKind::Nexus,
                id: self.uuid(),
                share: status.share.to_string(),
            }),
            NexusOperation::Share(_) => Ok(()),
            NexusOperation::Unshare if !status.share.shared() => Err(SvcError::NotShared {
                kind: ResourceKind::Nexus,
                id: self.uuid(),
            }),
            NexusOperation::Unshare => Ok(()),
            NexusOperation::AddChild(child) if self.children.contains(child) => {
                Err(SvcError::ChildAlreadyExists {
                    nexus: self.uuid(),
                    child: child.to_string(),
                })
            }
            NexusOperation::AddChild(_) => Ok(()),
            NexusOperation::RemoveChild(child) if !self.children.contains(child) => {
                Err(SvcError::ChildNotFound {
                    nexus: self.uuid(),
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
    fn remove_spec(locked_spec: &Arc<Mutex<Self>>, registry: &Registry) {
        let uuid = locked_spec.lock().uuid.clone();
        registry.specs.remove_nexus(&uuid);
    }
    fn set_updating(&mut self, updating: bool) {
        self.updating = updating;
    }
    fn updating(&self) -> bool {
        self.updating
    }
    fn dirty(&self) -> bool {
        self.pending_op()
    }
    fn kind(&self) -> ResourceKind {
        ResourceKind::Nexus
    }
    fn uuid(&self) -> String {
        self.uuid.to_string()
    }
    fn state(&self) -> SpecState<NexusState> {
        self.state.clone()
    }
    fn set_state(&mut self, state: SpecState<Self::State>) {
        self.state = state;
    }
    fn owned(&self) -> bool {
        self.owner.is_some()
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
            if nexus.state.created() || nexus.state.deleting() {
                nexuses.push(nexus.clone());
            }
        }
        nexuses
    }
}

impl ResourceSpecsLocked {
    /// Get a list of created NexusSpec's
    pub fn get_created_nexus_specs(&self) -> Vec<NexusSpec> {
        let specs = self.read();
        specs.get_created_nexuses()
    }
    /// Get the protected NexusSpec for the given nexus `id`, if any exists
    fn get_nexus(&self, id: &NexusId) -> Option<Arc<Mutex<NexusSpec>>> {
        let specs = self.read();
        specs.nexuses.get(id).cloned()
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
    ) -> Result<Nexus, SvcError> {
        let node = registry
            .get_node_wrapper(&request.node)
            .await
            .context(NodeNotFound {
                node_id: request.node.clone(),
            })?;

        let nexus_spec = self.get_or_create_nexus(request);
        SpecOperations::start_create(&nexus_spec, registry, request).await?;

        let result = node.create_nexus(request).await;
        SpecOperations::complete_create(result, &nexus_spec, registry).await
    }

    pub async fn destroy_nexus(
        &self,
        registry: &Registry,
        request: &DestroyNexus,
        delete_owned: bool,
    ) -> Result<(), SvcError> {
        let node = registry
            .get_node_wrapper(&request.node)
            .await
            .context(NodeNotFound {
                node_id: request.node.clone(),
            })?;

        if let Some(nexus) = self.get_nexus(&request.uuid) {
            SpecOperations::start_destroy(&nexus, registry, delete_owned).await?;

            let result = node.destroy_nexus(request).await;
            SpecOperations::complete_destroy(result, &nexus, registry).await
        } else {
            node.destroy_nexus(request).await
        }
    }

    pub async fn share_nexus(
        &self,
        registry: &Registry,
        request: &ShareNexus,
    ) -> Result<String, SvcError> {
        let node = registry
            .get_node_wrapper(&request.node)
            .await
            .context(NodeNotFound {
                node_id: request.node.clone(),
            })?;

        if let Some(nexus_spec) = self.get_nexus(&request.uuid) {
            let status = registry.get_nexus(&request.uuid).await?;
            let spec_clone = SpecOperations::start_update(
                registry,
                &nexus_spec,
                &status,
                NexusOperation::Share(request.protocol),
            )
            .await?;

            let result = node.share_nexus(request).await;
            SpecOperations::complete_update(registry, result, nexus_spec, spec_clone).await
        } else {
            node.share_nexus(request).await
        }
    }

    pub async fn unshare_nexus(
        &self,
        registry: &Registry,
        request: &UnshareNexus,
    ) -> Result<(), SvcError> {
        let node = registry
            .get_node_wrapper(&request.node)
            .await
            .context(NodeNotFound {
                node_id: request.node.clone(),
            })?;

        if let Some(nexus_spec) = self.get_nexus(&request.uuid) {
            let status = registry.get_nexus(&request.uuid).await?;
            let spec_clone = SpecOperations::start_update(
                registry,
                &nexus_spec,
                &status,
                NexusOperation::Unshare,
            )
            .await?;

            let result = node.unshare_nexus(request).await;
            SpecOperations::complete_update(registry, result, nexus_spec, spec_clone).await
        } else {
            node.unshare_nexus(request).await
        }
    }

    pub async fn add_nexus_child(
        &self,
        registry: &Registry,
        request: &AddNexusChild,
    ) -> Result<Child, SvcError> {
        let node = registry
            .get_node_wrapper(&request.node)
            .await
            .context(NodeNotFound {
                node_id: request.node.clone(),
            })?;

        if let Some(nexus_spec) = self.get_nexus(&request.nexus) {
            let status = registry.get_nexus(&request.nexus).await?;
            let spec_clone = SpecOperations::start_update(
                registry,
                &nexus_spec,
                &status,
                NexusOperation::AddChild(NexusChild::from(&request.uri)),
            )
            .await?;

            let result = node.add_child(request).await;
            SpecOperations::complete_update(registry, result, nexus_spec, spec_clone).await
        } else {
            node.add_child(request).await
        }
    }

    pub async fn remove_nexus_child(
        &self,
        registry: &Registry,
        request: &RemoveNexusChild,
    ) -> Result<(), SvcError> {
        let node = registry
            .get_node_wrapper(&request.node)
            .await
            .context(NodeNotFound {
                node_id: request.node.clone(),
            })?;

        if let Some(nexus_spec) = self.get_nexus(&request.nexus) {
            let status = registry.get_nexus(&request.nexus).await?;
            let spec_clone = SpecOperations::start_update(
                registry,
                &nexus_spec,
                &status,
                NexusOperation::RemoveChild(NexusChild::from(&request.uri)),
            )
            .await?;

            let result = node.remove_child(request).await;
            SpecOperations::complete_update(registry, result, nexus_spec, spec_clone).await
        } else {
            node.remove_child(request).await
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

    /// Worker that reconciles dirty NexusSpecs's with the persistent store.
    /// This is useful when nexus operations are performed but we fail to
    /// update the spec with the persistent store.
    pub async fn reconcile_dirty_nexuses(&self, registry: &Registry) -> bool {
        if registry.store_online().await {
            let mut pending_count = 0;

            let nexuses = self.get_nexuses();
            for nexus_spec in nexuses {
                let mut nexus_clone = {
                    let mut nexus = nexus_spec.lock();
                    if nexus.updating || !nexus.state.created() {
                        continue;
                    }
                    nexus.updating = true;
                    nexus.clone()
                };

                if let Some(op) = nexus_clone.operation.clone() {
                    let fail = !match op.result {
                        Some(true) => {
                            nexus_clone.commit_op();
                            let result = registry.store_obj(&nexus_clone).await;
                            if result.is_ok() {
                                let mut nexus = nexus_spec.lock();
                                nexus.commit_op();
                            }
                            result.is_ok()
                        }
                        Some(false) => {
                            nexus_clone.clear_op();
                            let result = registry.store_obj(&nexus_clone).await;
                            if result.is_ok() {
                                let mut nexus = nexus_spec.lock();
                                nexus.clear_op();
                            }
                            result.is_ok()
                        }
                        None => {
                            // we must have crashed... we could check the node to see what the
                            // current state is but for now assume failure
                            nexus_clone.clear_op();
                            let result = registry.store_obj(&nexus_clone).await;
                            if result.is_ok() {
                                let mut nexus = nexus_spec.lock();
                                nexus.clear_op();
                            }
                            result.is_ok()
                        }
                    };
                    if fail {
                        pending_count += 1;
                    }
                } else {
                    // No operation to reconcile.
                    let mut spec = nexus_spec.lock();
                    spec.updating = false;
                }
            }
            pending_count > 0
        } else {
            true
        }
    }
}
