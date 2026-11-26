use crate::{bucket::Bucket, store::Store};
use agents::errors::SvcError;
use grpc::{
    client::CoreClient,
    context::Context,
    operations::{dr::traits::DrOperations, volume::traits::VolumeOperations},
};
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use stor_port::{
    pstor::StorableObjectType,
    transport_api::{ReplyError, ResourceKind},
    types::v0::{
        store::{
            definitions::StoreError,
            dr::{DrClusterPairSpec, DrVolume, DrVolumeSpec, DrVolumeSpecStatus},
        },
        transport::{
            CreateDrClusterPair, DestroyDrClusterPair, DisableDrVolume, DrClusterPair,
            DrClusterPairs, DrPairId, DrPolicy, DrVolume as DrVolumeEntry, DrVolumes,
            EnableDrVolume, Filter, GetDrClusterPair, GetDrVolume, VolumeId,
        },
    },
};
use tokio::sync::RwLock;

/// The disaster recovery agent's service.
///
/// It answers the liveness probe, and holds the cluster pairs and the volume entries
/// replicated over them.
///
/// Both are held in memory and written through to the persistent store, and read back
/// from it at startup. What the peer needs of them is written to the pair's bucket first:
/// a pair puts its cluster's `cluster.cfg` there, and enabling a volume its side's
/// `dr.cfg` and `state.json`. A request the bucket refuses is not persisted, so nothing
/// this agent answers for is missing from the bucket. This is a pair of maps rather than the core agent's resource
/// registry: there is no reconciler yet, nothing to lock against a background task, and
/// no pagination to serve.
///
/// Where both maps are locked, the pairs are locked first: a volume names a pair, and a
/// pair is not destroyed while one does.
pub(crate) struct Service {
    pairs: Arc<RwLock<HashMap<DrPairId, DrClusterPairSpec>>>,
    volumes: Arc<RwLock<HashMap<VolumeId, DrVolume>>>,
    store: Store,
    core: CoreClient,
    /// Where named credentials are mounted, one directory per name.
    credentials: Option<PathBuf>,
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

/// A volume entry's policy: every interval in it has to be one.
fn policy(policy: &DrPolicy) -> Result<(), SvcError> {
    let intervals = [
        ("schedule", Some(policy.schedule)),
        ("ttl", Some(policy.ttl)),
        ("maxLag", policy.max_lag),
    ];
    match intervals
        .into_iter()
        .find(|(_, interval)| interval.is_some_and(|i| i.is_zero()))
    {
        Some((what, _)) => Err(SvcError::InvalidDrVolume {
            error: format!("a volume's {what} must not be zero"),
        }),
        None => Ok(()),
    }
}

impl Service {
    /// Creates a new dr service from what the store already holds.
    pub(crate) fn new(
        store: Store,
        core: CoreClient,
        credentials: Option<PathBuf>,
        pairs: Vec<DrClusterPairSpec>,
        volumes: Vec<DrVolume>,
    ) -> Self {
        Self {
            pairs: Arc::new(RwLock::new(
                pairs.into_iter().map(|p| (p.id.clone(), p)).collect(),
            )),
            volumes: Arc::new(RwLock::new(
                volumes
                    .into_iter()
                    .map(|v| (v.volume().clone(), v))
                    .collect(),
            )),
            store,
            core,
            credentials,
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
        // Decided before the bucket is written and again after: the bucket is not written
        // under the lock, since a store that is slow to answer should not hold up every
        // other request, and a concurrent put may have moved the pair meanwhile.
        let spec = Self::pair_spec(req, &*self.pairs.read().await)?;
        // The bucket first: a pair whose store refuses it is not a pair.
        Bucket::open(&spec, self.credentials.as_deref())?
            .put_cluster(&spec)
            .await?;
        let mut pairs = self.pairs.write().await;
        let spec = Self::pair_spec(req, &pairs)?;
        // Written before it is published, so a pair this answers for is one a restart
        // still knows about.
        self.store.put(&spec).await?;
        pairs.insert(spec.id.clone(), spec.clone());
        Ok(as_transport(spec))
    }

    /// The pair a create asks for, against the pairs there are.
    fn pair_spec(
        req: &CreateDrClusterPair,
        pairs: &HashMap<DrPairId, DrClusterPairSpec>,
    ) -> Result<DrClusterPairSpec, SvcError> {
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
        Ok(spec)
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

    /// Destroying a pair leaves its `cluster.cfg` in the bucket: it is keyed on the cluster,
    /// not the pair, and another pair under the same name over the same bucket publishes the
    /// same one.
    async fn destroy_pair(&self, req: &DestroyDrClusterPair) -> Result<(), SvcError> {
        let mut pairs = self.pairs.write().await;
        // Destroying what is not there is an error, as it is for every other resource -
        // the plugin's `--ignore-not-found` is what turns it back into a no-op.
        let spec = pairs.get(&req.id).cloned();
        let spec = spec.ok_or_else(|| SvcError::DrClusterPairNotFound {
            id: req.id.to_string(),
        })?;
        // A volume replicating over the pair would lose the store it writes to, and so
        // would one still waiting to be purged from it.
        let volumes = self.volumes.read().await;
        if volumes.values().any(|volume| volume.spec.pair == req.id) {
            return Err(SvcError::InUse {
                kind: ResourceKind::DrClusterPair,
                id: req.id.to_string(),
            });
        }
        self.store.delete(&spec).await?;
        pairs.remove(&req.id);
        Ok(())
    }

    async fn enable_volume(&self, req: &EnableDrVolume) -> Result<DrVolumeEntry, ReplyError> {
        policy(&req.policy)?;
        // Before any lock: core answers for the volume, and a slow answer should not hold
        // up every other request. Its geometry is what a matching volume is built from.
        let volumes = self
            .core
            .volume()
            .get(Filter::Volume(req.volume.clone()), false, None, None)
            .await?;
        let cluster_size = match volumes.entries.first() {
            Some(volume) => volume.spec_ref().cluster_size as u64,
            None => {
                return Err(SvcError::VolumeNotFound {
                    vol_id: req.volume.to_string(),
                }
                .into())
            }
        };

        // Decided before the bucket is written and again after, as for a pair: the
        // bucket is not written under the locks.
        let (pair, entry) = {
            let pairs = self.pairs.read().await;
            let volumes = self.volumes.read().await;
            Self::volume_entry(req, &pairs, &volumes)?
        };
        // The bucket first, as for a pair: an entry whose side is not published is not
        // one the peer can find.
        Bucket::open(&pair, self.credentials.as_deref())?
            .put_side(&pair, &entry.spec, cluster_size)
            .await?;

        let pairs = self.pairs.read().await;
        let mut volumes = self.volumes.write().await;
        let (_, entry) = Self::volume_entry(req, &pairs, &volumes)?;
        // Written before it is published, so an entry this answers for is one a restart
        // still knows about.
        self.store.put(&entry).await.map_err(SvcError::from)?;
        volumes.insert(req.volume.clone(), entry.clone());
        Ok(entry.into())
    }

    /// The entry an enable asks for, and the pair it is over, against the pairs and the
    /// entries there are.
    fn volume_entry(
        req: &EnableDrVolume,
        pairs: &HashMap<DrPairId, DrClusterPairSpec>,
        volumes: &HashMap<VolumeId, DrVolume>,
    ) -> Result<(DrClusterPairSpec, DrVolume), ReplyError> {
        let Some(pair) = pairs.get(&req.pair) else {
            return Err(SvcError::DrClusterPairNotFound {
                id: req.pair.to_string(),
            }
            .into());
        };
        let asked = DrVolumeSpec::new(req.volume.clone(), req.pair.clone(), req.role, req.policy);
        let entry = match volumes.get(&req.volume) {
            None => DrVolume { spec: asked },
            // The relationship is still in the store until both ends have let it go, so
            // enabling it again now would resume one the peer is tearing down.
            Some(existing) if !matches!(existing.spec.status, DrVolumeSpecStatus::Created(_)) => {
                return Err(SvcError::Deleting {
                    kind: ResourceKind::DrVolume,
                }
                .into())
            }
            // The pair and the role say which relationship this is; amending them would be
            // a different one, and everything written under this one would be orphaned.
            Some(existing)
                if existing.spec.pair != asked.pair || existing.spec.role != asked.role =>
            {
                return Err(SvcError::InvalidDrVolume {
                    error: format!(
                        "volume '{}' is the {:?} of relationship '{}' over pair '{}'; disable \
                         replication to enable a different one",
                        req.volume, existing.spec.role, existing.spec.dr, existing.spec.pair
                    ),
                }
                .into())
            }
            // Repeating the call converges, as for a pair: the policy is what moves.
            Some(existing) => {
                let mut entry = existing.clone();
                entry.spec.policy = req.policy;
                entry
            }
        };
        Ok((pair.clone(), entry))
    }

    async fn get_volume(&self, req: &GetDrVolume) -> Result<DrVolumeEntry, SvcError> {
        let volume = self.volumes.read().await.get(&req.volume).cloned();
        volume
            .map(Into::into)
            .ok_or_else(|| SvcError::DrVolumeNotFound {
                vol_id: req.volume.to_string(),
            })
    }

    async fn list_volumes(&self) -> Result<DrVolumes, SvcError> {
        let volumes = self.volumes.read().await;
        let entries = volumes.values().cloned().map(Into::into);
        Ok(DrVolumes {
            entries: entries.collect(),
        })
    }

    /// Disabling replication marks the entry rather than removing it: the relationship's
    /// objects are shared with the peer, so they are purged only once both ends have let
    /// go of it, and the entry is what says this end has.
    async fn disable_volume(&self, req: &DisableDrVolume) -> Result<(), SvcError> {
        let mut volumes = self.volumes.write().await;
        let Some(existing) = volumes.get(&req.volume) else {
            return Err(SvcError::DrVolumeNotFound {
                vol_id: req.volume.to_string(),
            });
        };
        // Already let go of: the request is in effect, so repeating it converges.
        if !matches!(existing.spec.status, DrVolumeSpecStatus::Created(_)) {
            return Ok(());
        }
        let mut entry = existing.clone();
        entry.spec.status = DrVolumeSpecStatus::Deleting;
        self.store.put(&entry).await?;
        volumes.insert(req.volume.clone(), entry);
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

    async fn enable_volume(
        &self,
        req: &EnableDrVolume,
        _ctx: Option<Context>,
    ) -> Result<DrVolumeEntry, ReplyError> {
        self.enable_volume(req).await
    }

    async fn get_volume(
        &self,
        req: &GetDrVolume,
        _ctx: Option<Context>,
    ) -> Result<DrVolumeEntry, ReplyError> {
        Ok(self.get_volume(req).await?)
    }

    async fn list_volumes(&self, _ctx: Option<Context>) -> Result<DrVolumes, ReplyError> {
        Ok(self.list_volumes().await?)
    }

    async fn disable_volume(
        &self,
        req: &DisableDrVolume,
        _ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        Ok(self.disable_volume(req).await?)
    }
}

/// Connect to the persistent store, read back the pairs and volumes already there, and
/// serve them.
pub(crate) async fn new(
    store: &http::Uri,
    timeout: std::time::Duration,
    core: CoreClient,
    credentials: Option<PathBuf>,
) -> Result<Service, StoreError> {
    let store = Store::new(store, timeout).await?;
    let pairs = store.list(StorableObjectType::DrClusterPairSpec).await?;
    tracing::info!(count = pairs.len(), "Loaded dr cluster pairs");
    let volumes = store.list(StorableObjectType::DrVolume).await?;
    tracing::info!(count = volumes.len(), "Loaded dr volumes");
    Ok(Service::new(store, core, credentials, pairs, volumes))
}
