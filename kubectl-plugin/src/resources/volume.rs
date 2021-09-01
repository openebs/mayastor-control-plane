use crate::{
    operations::{Get, List, Scale},
    resources::{ReplicaCount, VolumeId},
    rest_client::RestClient,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use reqwest::{self, Response};
use structopt::StructOpt;

/// Volumes resource.
#[derive(StructOpt, Debug)]
pub(crate) struct Volumes {}

#[async_trait]
impl List for Volumes {
    /// Support listing volumes.
    async fn list(&self) -> Result<Response> {
        let url = RestClient::http_base_url().join("volumes")?;
        RestClient::get_request(&url)
            .await
            .map_err(|e| anyhow!("Failed to list volumes. Error {}", e))
    }
}

/// Volume resource.
#[derive(StructOpt, Debug)]
pub(crate) struct Volume {
    /// ID of the volume.
    id: VolumeId,
    /// Number of replicas.
    replica_count: Option<ReplicaCount>,
}

impl Volume {
    /// Construct a new Volume instance.
    pub fn new(id: &str, replica_count: Option<ReplicaCount>) -> Self {
        Self {
            id: id.into(),
            replica_count,
        }
    }
}

#[async_trait]
impl Get for Volume {
    /// Support getting a volume using its ID.
    async fn get(&self) -> Result<Response> {
        let url = RestClient::http_base_url().join(&format!("volumes/{}", self.id))?;
        RestClient::get_request(&url)
            .await
            .map_err(|e| anyhow!("Failed to get volume {}. Error {}", self.id, e))
    }
}

#[async_trait]
impl Scale for Volume {
    /// Support increasing/decreasing the replica count of a volume.
    async fn scale(&self) -> Result<Response> {
        assert!(self.replica_count.is_some());
        let replica_count = self.replica_count.unwrap();
        let url = RestClient::http_base_url().join(&format!(
            "volumes/{}/replica_count/{}",
            self.id, replica_count
        ))?;
        RestClient::put_request(&url).await.map_err(|e| {
            anyhow!(
                "Failed to scale volume {} to {} replica(s). Error {}",
                self.id,
                replica_count,
                e
            )
        })
    }
}
