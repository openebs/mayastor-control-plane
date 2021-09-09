use crate::{
    operations::{Get, List, Scale},
    resources::{ReplicaCount, VolumeId},
    rest_wrapper::RestClient,
};
use anyhow::Result;
use async_trait::async_trait;
use structopt::StructOpt;

/// Volumes resource.
#[derive(StructOpt, Debug)]
pub(crate) struct Volumes {}

#[async_trait(?Send)]
impl List for Volumes {
    async fn list() -> Result<()> {
        let volumes = RestClient::client()
            .volumes_api()
            .get_volumes()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get volumes. Error {}", e))?;
        println!("{:?}", volumes);
        Ok(())
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

#[async_trait(?Send)]
impl Get for Volume {
    type ID = VolumeId;
    async fn get(id: &Self::ID) -> Result<()> {
        let volume = RestClient::client()
            .volumes_api()
            .get_volume(id)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get volume {}. Error {}", id, e))?;
        println!("{:?}", volume);
        Ok(())
    }
}

#[async_trait(?Send)]
impl Scale for Volume {
    type ID = VolumeId;

    async fn scale(id: &Self::ID, replica_count: u8) -> Result<()> {
        let volume = RestClient::client()
            .volumes_api()
            .put_volume_replica_count(id, replica_count)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to scale volume {}. Error {}", id, e))?;
        println!("{:?}", volume);
        Ok(())
    }
}
