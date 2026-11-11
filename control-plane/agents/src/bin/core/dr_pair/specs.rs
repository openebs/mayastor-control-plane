use crate::controller::{
    registry::Registry,
    resources::operations_helper::{ResourceSpecs, ResourceSpecsLocked},
};
use agents::errors::SvcError;
use stor_port::{
    pstor::ObjectKey,
    types::v0::{
        store::dr_pair::{DrClusterPairSpec, DrClusterPairSpecKey},
        transport::{CreateDrClusterPair, DrPairId},
    },
};

impl ResourceSpecs {
    /// Get a copy of all dr cluster pair specs.
    pub(crate) fn dr_pair_specs(&self) -> Vec<DrClusterPairSpec> {
        self.dr_pairs.values().map(|v| v.lock().clone()).collect()
    }
}

impl ResourceSpecsLocked {
    /// Get a copy of all dr cluster pair specs.
    pub(crate) fn dr_pair_specs(&self) -> Vec<DrClusterPairSpec> {
        self.read().dr_pair_specs()
    }

    /// Get a dr cluster pair spec by id.
    pub(crate) fn dr_pair_spec(&self, id: &DrPairId) -> Option<DrClusterPairSpec> {
        self.read().dr_pairs.get(id).map(|spec| spec.lock().clone())
    }

    /// Create a dr cluster pair, or adopt one that already exists.
    ///
    /// Repeating the call is not an error: an operator reconciling the same desired state
    /// converges rather than failing on the second pass. Terms that differ from the stored
    /// ones are applied, so the record follows the declared intent.
    pub(crate) async fn create_dr_pair(
        &self,
        registry: &Registry,
        req: &CreateDrClusterPair,
    ) -> Result<DrClusterPairSpec, SvcError> {
        let (changed, spec) = {
            let mut specs = self.write();
            match specs.dr_pairs.get(&req.id) {
                Some(existing) => {
                    let mut pair = existing.lock();
                    let changed = pair.cluster != req.cluster
                        || pair.peer != req.peer
                        || pair.transport != req.transport;
                    pair.cluster = req.cluster.clone();
                    pair.peer = req.peer.clone();
                    pair.transport = req.transport.clone();
                    (changed, pair.clone())
                }
                None => {
                    // The uid is core's own: it is the identity the control plane
                    // already keys its persistent store on, so it is read here rather
                    // than asked of the caller, who has no way to know it.
                    let pair = DrClusterPairSpec::new(
                        req.id.clone(),
                        req.cluster.clone(),
                        stor_port::platform::platform_info().uid().into(),
                        req.peer.clone(),
                        req.transport.clone(),
                    );
                    specs.dr_pairs.insert(pair.clone());
                    (true, pair)
                }
            }
        };
        if changed {
            registry.store_obj(&spec).await?;
        }
        Ok(spec)
    }

    /// Remove a dr cluster pair.
    pub(crate) async fn destroy_dr_pair(
        &self,
        registry: &Registry,
        id: &DrPairId,
    ) -> Result<(), SvcError> {
        // Destroying what is not there is an error, as it is for every other resource -
        // the plugin's `--ignore-not-found` is what turns it back into a no-op.
        if self.dr_pair_spec(id).is_none() {
            return Err(SvcError::DrClusterPairNotFound { id: id.to_string() });
        }
        registry
            .delete_kv(&DrClusterPairSpecKey::from(id).key())
            .await?;
        self.write().dr_pairs.remove(id);
        Ok(())
    }
}
