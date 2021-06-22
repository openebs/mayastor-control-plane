use std::{ops::Deref, sync::Arc};

use snafu::OptionExt;
use tokio::sync::Mutex;

use crate::core::{
    registry::Registry,
    specs::{ResourceSpecs, ResourceSpecsLocked},
    wrapper::ClientOps,
};
use common::errors::{NodeNotFound, SvcError};
use mbus_api::ResourceKind;
use types::v0::{
    message_bus::mbus::{
        AddNexusChild, Child, CreateNexus, DestroyNexus, Nexus, NexusId, NexusState,
        RemoveNexusChild, ShareNexus, UnshareNexus,
    },
    store::{
        definitions::{ObjectKey, Store, StoreError},
        nexus::{NexusOperation, NexusSpec, NexusSpecKey, NexusSpecState},
        SpecTransaction,
    },
};

impl ResourceSpecs {
    fn get_nexus(&self, id: &NexusId) -> Option<Arc<Mutex<NexusSpec>>> {
        self.nexuses.get(id).cloned()
    }
}

/// Implementation of the ResourceSpecs which is retrieved from the ResourceSpecsLocked
/// During these calls, no other thread can add/remove elements from the list
impl ResourceSpecs {
    /// Get all NexusSpec's
    pub async fn get_nexuses(&self) -> Vec<NexusSpec> {
        let mut vector = vec![];
        for object in self.nexuses.values() {
            let object = object.lock().await;
            vector.push(object.clone());
        }
        vector
    }
    /// Get all NexusSpec's which are in a created state
    pub async fn get_created_nexuses(&self) -> Vec<NexusSpec> {
        let mut nexuses = vec![];
        for nexus in self.nexuses.values() {
            let nexus = nexus.lock().await;
            if nexus.state.created() || nexus.state.deleting() {
                nexuses.push(nexus.clone());
            }
        }
        nexuses
    }
}

impl ResourceSpecsLocked {
    /// Get a list of created NexusSpec's
    pub async fn get_created_nexus_specs(&self) -> Vec<NexusSpec> {
        let specs = self.read().await;
        specs.get_created_nexuses().await
    }
    /// Get the protected NexusSpec for the given nexus `id`, if any exists
    async fn get_nexus(&self, id: &NexusId) -> Option<Arc<Mutex<NexusSpec>>> {
        let specs = self.read().await;
        specs.nexuses.get(id).cloned()
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

        let nexus_spec = {
            let mut specs = self.write().await;
            if let Some(spec) = specs.get_nexus(&request.uuid) {
                {
                    let mut nexus_spec = spec.lock().await;
                    if nexus_spec.updating {
                        // already being created
                        return Err(SvcError::Conflict {});
                    } else if nexus_spec.state.creating() {
                        // this might be a retry, check if the params are the
                        // same and if so, let's retry!
                        if nexus_spec.ne(request) {
                            // if not then we can't proceed
                            return Err(SvcError::Conflict {});
                        }
                    } else {
                        return Err(SvcError::AlreadyExists {
                            kind: ResourceKind::Nexus,
                            id: request.uuid.to_string(),
                        });
                    }

                    nexus_spec.updating = true;
                }
                spec
            } else {
                let spec = NexusSpec::from(request);
                // write the spec to the persistent store
                {
                    let mut store = registry.store.lock().await;
                    store.put_obj(&spec).await?;
                }
                // add spec to the internal spec registry
                let spec = Arc::new(Mutex::new(spec));
                specs.nexuses.insert(request.uuid.clone(), spec.clone());
                spec
            }
        };

        let result = node.create_nexus(request).await;
        if result.is_ok() {
            let mut nexus_spec = nexus_spec.lock().await;
            nexus_spec.state = NexusSpecState::Created(NexusState::Online);
            nexus_spec.updating = false;
            let mut store = registry.store.lock().await;
            store.put_obj(nexus_spec.deref()).await?;
        }

        result
    }

    pub async fn destroy_nexus(
        &self,
        registry: &Registry,
        request: &DestroyNexus,
        force: bool,
    ) -> Result<(), SvcError> {
        let node = registry
            .get_node_wrapper(&request.node)
            .await
            .context(NodeNotFound {
                node_id: request.node.clone(),
            })?;

        let nexus = self.get_nexus(&request.uuid).await;
        if let Some(nexus) = &nexus {
            let mut nexus = nexus.lock().await;
            let destroy_nexus = force || nexus.owner.is_none();

            if nexus.updating {
                return Err(SvcError::Conflict {});
            } else if nexus.state.deleted() {
                return Ok(());
            }
            if !destroy_nexus {
                return Err(SvcError::Conflict {});
            }
            if !nexus.state.deleting() {
                nexus.state = NexusSpecState::Deleting;
                // write it to the store
                let mut store = registry.store.lock().await;
                store.put_obj(nexus.deref()).await?;
            }
            nexus.updating = true;
        }

        if let Some(nexus) = nexus {
            let result = node.destroy_nexus(request).await;
            match &result {
                Ok(_) => {
                    let mut nexus = nexus.lock().await;
                    nexus.updating = false;
                    {
                        // remove the spec from the persistent store
                        // if it fails, then fail the request and let the op
                        // retry
                        let mut store = registry.store.lock().await;
                        if let Err(error) = store
                            .delete_kv(&NexusSpecKey::from(&request.uuid).key())
                            .await
                        {
                            if !matches!(error, StoreError::MissingEntry { .. }) {
                                return Err(error.into());
                            }
                        }
                    }
                    nexus.state = NexusSpecState::Deleted;
                    drop(nexus);
                    // now remove the spec from our list
                    self.del_nexus(&request.uuid).await;
                }
                Err(_error) => {
                    let mut nexus = nexus.lock().await;
                    nexus.updating = false;
                }
            }
            result
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

        if let Some(nexus_spec) = self.get_nexus(&request.uuid).await {
            let spec_clone = {
                let status = registry.get_nexus(&request.uuid).await?;
                let mut spec = nexus_spec.lock().await;
                if spec.pending_op() {
                    return Err(SvcError::StoreSave {
                        kind: ResourceKind::Nexus,
                        id: request.uuid.to_string(),
                    });
                } else if spec.updating {
                    return Err(SvcError::Conflict {});
                } else if !spec.state.created() {
                    return Err(SvcError::NexusNotFound {
                        nexus_id: request.uuid.to_string(),
                    });
                } else {
                    // validate the operation itself against the spec and status
                    if spec.share != status.share {
                        return Err(SvcError::NotReady {
                            kind: ResourceKind::Nexus,
                            id: request.uuid.to_string(),
                        });
                    } else if spec.share.shared() {
                        return Err(SvcError::AlreadyShared {
                            kind: ResourceKind::Nexus,
                            id: spec.uuid.to_string(),
                            share: spec.share.to_string(),
                        });
                    }
                }

                spec.start_op(NexusOperation::Share(request.protocol));
                spec.clone()
            };

            if let Err(error) = registry.store_obj(&spec_clone).await {
                let mut spec = nexus_spec.lock().await;
                spec.clear_op();
                return Err(error);
            }

            let result = node.share_nexus(request).await;
            Self::spec_complete_op(registry, result, nexus_spec, spec_clone).await
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

        if let Some(nexus_spec) = self.get_nexus(&request.uuid).await {
            let spec_clone = {
                let status = registry.get_nexus(&request.uuid).await?;
                let mut spec = nexus_spec.lock().await;
                if spec.pending_op() {
                    return Err(SvcError::StoreSave {
                        kind: ResourceKind::Nexus,
                        id: request.uuid.to_string(),
                    });
                } else if spec.updating {
                    return Err(SvcError::Conflict {});
                } else if !spec.state.created() {
                    return Err(SvcError::NexusNotFound {
                        nexus_id: request.uuid.to_string(),
                    });
                } else {
                    // validate the operation itself against the spec and status
                    if spec.share != status.share {
                        return Err(SvcError::NotReady {
                            kind: ResourceKind::Nexus,
                            id: request.uuid.to_string(),
                        });
                    } else if !spec.share.shared() {
                        return Err(SvcError::NotShared {
                            kind: ResourceKind::Nexus,
                            id: spec.uuid.to_string(),
                        });
                    }
                }

                spec.start_op(NexusOperation::Unshare);
                spec.clone()
            };

            if let Err(error) = registry.store_obj(&spec_clone).await {
                let mut spec = nexus_spec.lock().await;
                spec.clear_op();
                return Err(error);
            }

            let result = node.unshare_nexus(request).await;
            Self::spec_complete_op(registry, result, nexus_spec, spec_clone).await
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

        if let Some(nexus_spec) = self.get_nexus(&request.nexus).await {
            let spec_clone = {
                let status = registry.get_nexus(&request.nexus).await?;
                let mut spec = nexus_spec.lock().await;
                if spec.pending_op() {
                    return Err(SvcError::StoreSave {
                        kind: ResourceKind::Nexus,
                        id: request.nexus.to_string(),
                    });
                } else if spec.updating {
                    return Err(SvcError::Conflict {});
                } else if !spec.state.created() {
                    return Err(SvcError::NexusNotFound {
                        nexus_id: request.nexus.to_string(),
                    });
                } else if spec.children.contains(&request.uri)
                    && status.children.iter().any(|c| c.uri == request.uri)
                {
                    return Err(SvcError::ChildAlreadyExists {
                        nexus: request.nexus.to_string(),
                        child: request.uri.to_string(),
                    });
                }

                spec.start_op(NexusOperation::AddChild(request.uri.clone()));
                spec.clone()
            };

            if let Err(error) = registry.store_obj(&spec_clone).await {
                let mut spec = nexus_spec.lock().await;
                spec.clear_op();
                return Err(error);
            }

            let result = node.add_child(request).await;
            Self::spec_complete_op(registry, result, nexus_spec, spec_clone).await
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

        if let Some(nexus_spec) = self.get_nexus(&request.nexus).await {
            let spec_clone = {
                let status = registry.get_nexus(&request.nexus).await?;
                let mut spec = nexus_spec.lock().await;
                if spec.pending_op() {
                    return Err(SvcError::StoreSave {
                        kind: ResourceKind::Nexus,
                        id: request.nexus.to_string(),
                    });
                } else if spec.updating {
                    return Err(SvcError::Conflict {});
                } else if !spec.state.created() {
                    return Err(SvcError::NexusNotFound {
                        nexus_id: request.nexus.to_string(),
                    });
                } else if !spec.children.contains(&request.uri)
                    && !status.children.iter().any(|c| c.uri == request.uri)
                {
                    return Err(SvcError::ChildNotFound {
                        nexus: request.nexus.to_string(),
                        child: request.uri.to_string(),
                    });
                }

                spec.start_op(NexusOperation::RemoveChild(request.uri.clone()));
                spec.clone()
            };

            if let Err(error) = registry.store_obj(&spec_clone).await {
                let mut spec = nexus_spec.lock().await;
                spec.clear_op();
                return Err(error);
            }

            let result = node.remove_child(request).await;
            Self::spec_complete_op(registry, result, nexus_spec, spec_clone).await
        } else {
            node.remove_child(request).await
        }
    }

    /// Delete nexus by its `id`
    async fn del_nexus(&self, id: &NexusId) {
        let mut specs = self.write().await;
        specs.nexuses.remove(id);
    }
    /// Get a vector of protected NexusSpec's
    pub async fn get_nexuses(&self) -> Vec<Arc<Mutex<NexusSpec>>> {
        let specs = self.read().await;
        specs.nexuses.values().cloned().collect()
    }

    /// Worker that reconciles dirty NexusSpecs's with the persistent store.
    /// This is useful when nexus operations are performed but we fail to
    /// update the spec with the persistent store.
    pub async fn reconcile_dirty_nexuses(&self, registry: &Registry) -> bool {
        if registry.store_online().await {
            let mut pending_count = 0;

            let nexuses = self.get_nexuses().await;
            for nexus_spec in nexuses {
                let mut nexus = nexus_spec.lock().await;
                if nexus.updating || !nexus.state.created() {
                    continue;
                }
                if let Some(op) = nexus.operation.clone() {
                    let mut nexus_clone = nexus.clone();

                    let fail = !match op.result {
                        Some(true) => {
                            nexus_clone.commit_op();
                            let result = registry.store_obj(&nexus_clone).await;
                            if result.is_ok() {
                                nexus.commit_op();
                            }
                            result.is_ok()
                        }
                        Some(false) => {
                            nexus_clone.clear_op();
                            let result = registry.store_obj(&nexus_clone).await;
                            if result.is_ok() {
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
                                nexus.clear_op();
                            }
                            result.is_ok()
                        }
                    };
                    if fail {
                        pending_count += 1;
                    }
                }
            }
            pending_count > 0
        } else {
            true
        }
    }
}
