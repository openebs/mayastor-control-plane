use crate::store::PairStore;
use agents::errors::SvcError;
use grpc::{context::Context, operations::dr::traits::DrOperations};
use std::{collections::HashMap, sync::Arc};
use stor_port::{
    transport_api::ReplyError,
    types::v0::{
        store::{definitions::StoreError, dr::DrClusterPairSpec},
        transport::{
            CreateDrClusterPair, DestroyDrClusterPair, DrClusterPair, DrClusterPairs, DrPairId,
            GetDrClusterPair,
        },
    },
};
use tokio::sync::RwLock;

/// The disaster recovery agent's service.
///
/// It answers the liveness probe and holds the cluster pairs.
///
/// The pairs are held in memory and written through to the persistent store, and read back
/// from it at startup. A cluster has a handful of pairs, so this is a map rather than the
/// core agent's resource registry: there is no reconciler, nothing to lock against a
/// background task, and no pagination to serve.
#[derive(Debug, Clone)]
pub(crate) struct Service {
    pairs: Arc<RwLock<HashMap<DrPairId, DrClusterPairSpec>>>,
    store: PairStore,
}

/// A name the store layout and the persistent store key are both built from.
fn segment(what: &str, name: &str) -> Result<(), SvcError> {
    if name.is_empty() || name.contains('/') || name == "." || name == ".." {
        return Err(SvcError::InvalidDrClusterPair {
            error: format!("a pair's {what} must be a single path segment, got '{name}'"),
        });
    }
    Ok(())
}

/// A side's name: a segment, and dot-free, since a chunk reference joins side, uid and
/// sequence with dots and the collector parses them back apart.
fn side(what: &str, name: &str) -> Result<(), SvcError> {
    segment(what, name)?;
    if name.contains('.') {
        return Err(SvcError::InvalidDrClusterPair {
            error: format!("a pair's {what} must not carry a '.', got '{name}'"),
        });
    }
    Ok(())
}

/// The stored spec, as the transport sees it.
fn as_transport(spec: DrClusterPairSpec) -> DrClusterPair {
    DrClusterPair {
        id: spec.id,
        cluster: spec.cluster,
        cluster_uid: spec.cluster_uid,
        peer: spec.peer,
        transport: spec.transport,
    }
}

impl Service {
    /// Creates a new dr cluster pair service from what the store already holds.
    pub(crate) fn new(store: PairStore, pairs: Vec<DrClusterPairSpec>) -> Self {
        Self {
            pairs: Arc::new(RwLock::new(
                pairs.into_iter().map(|p| (p.id.clone(), p)).collect(),
            )),
            store,
        }
    }

    async fn create_pair(&self, req: &CreateDrClusterPair) -> Result<DrClusterPair, SvcError> {
        segment("id", req.id.as_str())?;
        side("cluster", req.cluster.as_str())?;
        side("peer", req.peer.as_str())?;
        // A pair whose two ends carry the same name cannot be told apart in the store, so
        // every key this side wrote would also be the key it expects its peer to have
        // written.
        if req.cluster == req.peer {
            return Err(SvcError::InvalidDrClusterPair {
                error: format!(
                    "a pair's cluster and peer names must differ, both are '{}'",
                    req.cluster
                ),
            });
        }
        let mut pairs = self.pairs.write().await;
        let spec = match pairs.get(&req.id) {
            // The two names say which relationship this is, so they are not something a
            // put amends: everything already written under the old ones would be orphaned
            // while the volumes naming this pair followed it elsewhere.
            Some(existing) if existing.cluster != req.cluster || existing.peer != req.peer => {
                return Err(SvcError::InvalidDrClusterPair {
                    error: format!(
                        "pair '{}' is between '{}' and '{}'; destroy it to pair other ends",
                        req.id, existing.cluster, existing.peer
                    ),
                })
            }
            // Repeating the call is not an error: an operator reconciling the same desired
            // state converges rather than failing on the second pass. Where the store is,
            // and the credentials for it, do move.
            Some(existing) => {
                let mut spec = existing.clone();
                spec.transport = req.transport.clone();
                spec
            }
            // The uid is not the caller's to choose: it is the identity the control plane
            // already keys its own persistent store on, so it is read here.
            None => DrClusterPairSpec::new(
                req.id.clone(),
                req.cluster.clone(),
                stor_port::platform::platform_info().uid().into(),
                req.peer.clone(),
                req.transport.clone(),
            ),
        };
        // Written before it is published, so a pair this answers for is one a restart
        // still knows about.
        self.store.put(&spec).await?;
        pairs.insert(spec.id.clone(), spec.clone());
        Ok(as_transport(spec))
    }

    async fn get_pair(&self, req: &GetDrClusterPair) -> Result<DrClusterPair, SvcError> {
        let pairs = self.pairs.read().await.get(&req.id).cloned();
        pairs
            .map(as_transport)
            .ok_or_else(|| SvcError::DrClusterPairNotFound {
                id: req.id.to_string(),
            })
    }

    async fn list_pairs(&self) -> Result<DrClusterPairs, SvcError> {
        let pairs = self.pairs.read().await;
        let entries = pairs.values().cloned().map(as_transport);
        Ok(DrClusterPairs {
            entries: entries.collect(),
        })
    }

    async fn destroy_pair(&self, req: &DestroyDrClusterPair) -> Result<(), SvcError> {
        let mut pairs = self.pairs.write().await;
        // Destroying what is not there is an error, as it is for every other resource -
        // the plugin's `--ignore-not-found` is what turns it back into a no-op.
        let spec = pairs.get(&req.id).cloned();
        let spec = spec.ok_or_else(|| SvcError::DrClusterPairNotFound {
            id: req.id.to_string(),
        })?;
        self.store.delete(&spec).await?;
        pairs.remove(&req.id);
        Ok(())
    }
}

#[tonic::async_trait]
impl DrOperations for Service {
    async fn probe(&self, _ctx: Option<Context>) -> Result<bool, ReplyError> {
        Ok(true)
    }

    async fn create_pair(
        &self,
        req: &CreateDrClusterPair,
        _ctx: Option<Context>,
    ) -> Result<DrClusterPair, ReplyError> {
        Ok(self.create_pair(req).await?)
    }

    async fn get_pair(
        &self,
        req: &GetDrClusterPair,
        _ctx: Option<Context>,
    ) -> Result<DrClusterPair, ReplyError> {
        Ok(self.get_pair(req).await?)
    }

    async fn list_pairs(&self, _ctx: Option<Context>) -> Result<DrClusterPairs, ReplyError> {
        Ok(self.list_pairs().await?)
    }

    async fn destroy_pair(
        &self,
        req: &DestroyDrClusterPair,
        _ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        Ok(self.destroy_pair(req).await?)
    }
}

/// Connect to the persistent store, read back the pairs already there, and serve them.
pub(crate) async fn new(
    store: &http::Uri,
    timeout: std::time::Duration,
) -> Result<Service, StoreError> {
    let store = PairStore::new(store, timeout).await?;
    let pairs = store.list().await?;
    tracing::info!(count = pairs.len(), "Loaded dr cluster pairs");
    Ok(Service::new(store, pairs))
}
