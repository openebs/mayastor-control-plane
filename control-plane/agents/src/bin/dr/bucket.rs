//! The store a pair's relationships live in, as this agent reaches it.

use agents::errors::SvcError;
use dr_store::{
    ClusterConfig, ClusterState, Clusters, DrConfig, HashAlgorithm, Prefix, Role, S3Config, S3Keys,
    Side, Sides, Store, UploadFormat, VERSION,
};
use std::{path::Path, time::Duration};
use stor_port::types::v0::{
    store::dr::{DrClusterPairSpec, DrVolumeSpec},
    transport::{DrRole, DrS3Credentials, DrTransport},
};

/// The region a signature is scoped to when the pair names none. Stores that do not
/// shard by region still require one, and this is the one they default to.
const DEFAULT_REGION: &str = "us-east-1";

/// How long a request answers within, all of the store's own retries included.
///
/// The store retries for minutes before it gives up, which is right for a cycle and
/// wrong for a request somebody is waiting on: a store that cannot be reached should be
/// an answer, not a hang.
const TIMEOUT: Duration = Duration::from_secs(30);

/// In a named credential's directory: the access key id.
pub(crate) const ACCESS_KEY: &str = "accessKey";
/// In a named credential's directory: the secret access key.
pub(crate) const SECRET_KEY: &str = "secretKey";
/// In a named credential's directory: the store's CA certificates, PEM. Named as a
/// Kubernetes tls secret names its CA, so one can be mounted as it is.
pub(crate) const CA: &str = "ca.crt";

/// What a pair's named credentials resolve to.
struct Named {
    keys: Option<S3Keys>,
    ca: Option<String>,
}

impl Named {
    /// `name`, as the directory `credentials` holds it: a Kubernetes secret, mounted.
    ///
    /// Each file is one of the secret's keys. The keys are both or neither - neither is the
    /// agent's own environment, with only a CA from here - and the CA is optional.
    ///
    /// Read when the store is opened, so per request: a secret changed under a running
    /// agent is what it uses next, as far as the mount has caught up with it.
    fn read(credentials: Option<&Path>, pair: &str, name: &str) -> Result<Self, SvcError> {
        if name.is_empty() || name.contains('/') || name.starts_with('.') {
            return Err(SvcError::InvalidDrClusterPair {
                error: format!(
                    "pair '{pair}' names credentials '{name}', which is not a single path \
                     segment, or starts with a '.'"
                ),
            });
        }
        let refused = |error: String| SvcError::DrStore {
            pair: pair.to_string(),
            transient: false,
            error: format!("credentials '{name}': {error}"),
        };
        let Some(credentials) = credentials else {
            return Err(refused(
                "this agent has no credentials directory to find them in".to_string(),
            ));
        };
        let dir = credentials.join(name);
        if !dir.is_dir() {
            return Err(refused(format!("no directory {}", dir.display())));
        }
        let read = |file: &str| -> Result<Option<String>, SvcError> {
            match std::fs::read_to_string(dir.join(file)) {
                Ok(content) => Ok(Some(content)),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(error) => Err(refused(format!("reading {file}: {error}"))),
            }
        };
        // A key written to a file usually ends in a newline, and no key has whitespace.
        let key = |file: &str| Ok::<_, SvcError>(read(file)?.map(|key| key.trim().to_string()));
        let keys = match (key(ACCESS_KEY)?, key(SECRET_KEY)?) {
            (Some(access_key_id), Some(secret_access_key)) => Some(S3Keys {
                access_key_id,
                secret_access_key,
            }),
            (None, None) => None,
            _ => {
                return Err(refused(format!(
                    "{ACCESS_KEY} and {SECRET_KEY} are both or neither"
                )))
            }
        };
        Ok(Self {
            keys,
            ca: read(CA)?,
        })
    }
}

/// A pair's store, and where its keys start.
///
/// Opened per request rather than held: it is a client, not a connection, so building
/// one costs nothing on the wire, and a pair amended to another store is then simply
/// the next one opened.
pub(crate) struct Bucket {
    pair: String,
    store: Store,
    prefix: Prefix,
}

impl Bucket {
    /// The store `pair` names, with its named credentials found under `credentials`.
    pub(crate) fn open(
        pair: &DrClusterPairSpec,
        credentials: Option<&Path>,
    ) -> Result<Self, SvcError> {
        let DrTransport::S3(s3) = &pair.transport;
        let (keys, ca) = match &s3.credentials {
            Some(DrS3Credentials::Keys(keys)) => (
                Some(S3Keys {
                    access_key_id: keys.access_key.clone(),
                    secret_access_key: keys.secret_key.clone(),
                }),
                None,
            ),
            Some(DrS3Credentials::Named(name)) => {
                let named = Named::read(credentials, pair.id.as_str(), name)?;
                (named.keys, named.ca)
            }
            None => (None, None),
        };
        let config = S3Config {
            endpoint: s3.endpoint.clone(),
            region: s3
                .region
                .clone()
                .unwrap_or_else(|| DEFAULT_REGION.to_string()),
            bucket: s3.bucket.clone(),
            keys,
            ca,
        };
        let store = Store::s3(&config).map_err(|error| store_error(&pair.id, error))?;
        Ok(Self {
            pair: pair.id.to_string(),
            store,
            prefix: Prefix::new(s3.prefix.as_deref().unwrap_or_default()),
        })
    }

    /// Publish this cluster's `cluster.cfg`: what a pair puts in the bucket.
    pub(crate) async fn put_cluster(&self, pair: &DrClusterPairSpec) -> Result<(), SvcError> {
        let cfg = ClusterConfig::new(pair.cluster.as_str(), pair.cluster_uid.as_str());
        self.timed(self.store.put_cluster_cfg(&self.prefix, &cfg))
            .await
    }

    /// Publish this side of a relationship: its `dr.cfg`, and its `state.json` unless
    /// it has one.
    ///
    /// The config is rewritten every time, which is what repairs a side whose entry was
    /// written and whose documents were not. The state is not: it is the side's progress,
    /// and enabling a relationship again must not wind it back.
    pub(crate) async fn put_side(
        &self,
        pair: &DrClusterPairSpec,
        volume: &DrVolumeSpec,
        cluster_size: u64,
    ) -> Result<(), SvcError> {
        let layout = self.prefix.relationship(volume.dr.as_str());
        let side = Side::new(pair.cluster.as_str(), pair.cluster_uid.as_str());
        let role = role(volume.role);
        let cfg = DrConfig {
            version: VERSION,
            dr: *volume.dr.uuid(),
            cluster: side.cluster.clone(),
            uid: side.uid.clone(),
            peer: pair.peer.to_string(),
            volume: *volume.volume.uuid(),
            role,
            cluster_size,
            // Derived from the cluster size once the changed-chunk set exists to say how;
            // until then they are the same.
            chunk_size: cluster_size,
            // The phase 1 format.
            upload_format: UploadFormat::SyntheticFull,
            dedup: true,
            hash: HashAlgorithm::Blake3,
            disabled: None,
        };
        self.timed(self.store.put_cfg(&layout, &cfg)).await?;
        if self
            .timed(self.store.get_state(&layout, &side))
            .await?
            .is_none()
        {
            let state = ClusterState::new(&side.cluster, cfg.dr, cfg.volume, role);
            self.timed(self.store.put_state(&layout, &side, &state))
                .await?;
        }
        Ok(())
    }

    async fn timed<T>(
        &self,
        op: impl std::future::Future<Output = dr_store::Result<T>>,
    ) -> Result<T, SvcError> {
        match tokio::time::timeout(TIMEOUT, op).await {
            Ok(result) => result.map_err(|error| store_error(&self.pair, error)),
            Err(_) => Err(SvcError::DrStore {
                pair: self.pair.clone(),
                transient: true,
                error: format!("no answer within {TIMEOUT:?}"),
            }),
        }
    }
}

/// The role as the store spells it.
fn role(role: DrRole) -> Role {
    match role {
        DrRole::Source => Role::Source,
        DrRole::Destination => Role::Destination,
    }
}

/// What the store said, as the agent answers it: worth asking again, or not until
/// something about the pair changes.
fn store_error(pair: &impl ToString, error: dr_store::Error) -> SvcError {
    SvcError::DrStore {
        pair: pair.to_string(),
        transient: error.transient(),
        error: error.to_string(),
    }
}
