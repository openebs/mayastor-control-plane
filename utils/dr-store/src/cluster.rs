//! What a cluster publishes about itself, outside any relationship.

use crate::{
    error::{Error, Result},
    side::{encode, read},
    DrStore, Prefix, VERSION,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// `<prefix>/clusters/<cluster>/<cluster-uid>/cluster.cfg`: a cluster's account of
/// itself, written when it is paired.
///
/// The pair is per cluster, not per volume, and this is what it puts in the bucket: that
/// a cluster under this name and uid is party to relationships here, before any volume
/// is. Written only by the cluster it describes.
///
/// It names no peer. The key is the cluster's name and uid, and one cluster can be
/// paired under the same name with more than one peer over the same bucket, so a peer
/// written here would be whichever pair wrote last.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// The schema version of the agent that wrote this.
    pub version: u32,
    /// The name this cluster holds its end of a relationship under.
    pub cluster: String,
    /// This cluster's own identity.
    pub uid: String,
    /// When this was written.
    pub observed_at: DateTime<Utc>,
}

impl ClusterConfig {
    /// This cluster, as of now.
    pub fn new(cluster: impl Into<String>, uid: impl Into<String>) -> Self {
        Self {
            version: VERSION,
            cluster: cluster.into(),
            uid: uid.into(),
            observed_at: Utc::now(),
        }
    }
}

/// Publishing and reading a cluster's own document, over any store.
#[async_trait::async_trait]
pub trait Clusters: DrStore {
    /// Publish this cluster's config.
    async fn put_cluster_cfg(&self, prefix: &Prefix, cfg: &ClusterConfig) -> Result<()> {
        let key = prefix.cluster_cfg(&cfg.cluster, &cfg.uid);
        self.put(&key, encode(cfg)?).await.map(|_| ())
    }

    /// A cluster's config, if it has published one. As for a side's documents, one that
    /// is there and cannot be read, or that describes another cluster, is an error.
    async fn get_cluster_cfg(
        &self,
        prefix: &Prefix,
        cluster: &str,
        uid: &str,
    ) -> Result<Option<ClusterConfig>> {
        let key = prefix.cluster_cfg(cluster, uid);
        let Some(cfg) = read::<ClusterConfig>(self, &key).await? else {
            return Ok(None);
        };
        if cfg.cluster != cluster || cfg.uid != uid {
            return Err(Error::Malformed {
                key,
                reason: format!("it describes cluster '{}/{}'", cfg.cluster, cfg.uid),
            });
        }
        Ok(Some(cfg))
    }
}

impl<S: DrStore + ?Sized> Clusters for S {}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn round_trips() {
        let cfg = ClusterConfig::new("east", "uid-1");
        let read: ClusterConfig = serde_json::from_slice(&encode(&cfg).unwrap()).unwrap();
        assert_eq!(read, cfg);
        let value = serde_json::to_value(&cfg).unwrap();
        assert_eq!(value["version"], json!(VERSION));
        assert_eq!(value["cluster"], json!("east"));
    }
}
