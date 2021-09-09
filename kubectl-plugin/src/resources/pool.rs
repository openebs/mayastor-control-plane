use crate::{
    operations::{Get, List},
    resources::PoolId,
    rest_wrapper::RestClient,
};
use anyhow::Result;
use async_trait::async_trait;
use structopt::StructOpt;

/// Pools resource.
#[derive(StructOpt, Debug)]
pub struct Pools {}

#[async_trait(?Send)]
impl List for Pools {
    async fn list() -> Result<()> {
        let pools = RestClient::client()
            .pools_api()
            .get_pools()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get pools. Error {}", e))?;
        println!("{:?}", pools);
        Ok(())
    }
}

/// Pool resource.
#[derive(StructOpt, Debug)]
pub(crate) struct Pool {
    /// ID of the pool.
    id: PoolId,
}

#[async_trait(?Send)]
impl Get for Pool {
    type ID = PoolId;
    async fn get(id: &Self::ID) -> Result<()> {
        let pool = RestClient::client()
            .pools_api()
            .get_pool(id)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get pool {}. Error {}", id, e))?;
        println!("{:?}", pool);
        Ok(())
    }
}
