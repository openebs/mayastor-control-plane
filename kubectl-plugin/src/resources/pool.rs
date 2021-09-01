use crate::{
    operations::{Get, List},
    resources::PoolId,
    rest_client::RestClient,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use reqwest::Response;
use structopt::StructOpt;

/// Pools resource.
#[derive(StructOpt, Debug)]
pub struct Pools {}

#[async_trait]
impl List for Pools {
    /// Support listing pools.
    async fn list(&self) -> Result<Response> {
        let url = RestClient::http_base_url().join("pools")?;
        RestClient::get_request(&url)
            .await
            .map_err(|e| anyhow!("Failed to list pools. Error {}", e))
    }
}

/// Pool resource.
#[derive(StructOpt, Debug)]
pub(crate) struct Pool {
    /// ID of the pool.
    id: PoolId,
}

impl Pool {
    /// Construct a new Pool instance.
    pub fn new(id: &str) -> Self {
        Self { id: id.into() }
    }
}

#[async_trait]
impl Get for Pool {
    /// Support getting a pool using its ID.
    async fn get(&self) -> Result<Response> {
        let url = RestClient::http_base_url().join(&format!("pools/{}", self.id))?;
        RestClient::get_request(&url)
            .await
            .map_err(|e| anyhow!("Failed to get pool {}. Error {}", self.id, e))
    }
}
