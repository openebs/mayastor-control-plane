use crate::{
    operations::{Get, List},
    resources::{utils, utils::CreateRows, PoolId},
    rest_wrapper::RestClient,
};
use async_trait::async_trait;
use prettytable::Row;
use structopt::StructOpt;

/// Pools resource.
#[derive(StructOpt, Debug)]
pub struct Pools {
    output: String,
}

impl CreateRows for Vec<openapi::models::Pool> {
    fn create_rows(&self) -> Vec<Row> {
        let mut rows: Vec<Row> = Vec::new();
        for pool in self {
            let state = pool.state.as_ref().unwrap();
            // The disks are joined by a comma and shown in the table, ex /dev/vda, /dev/vdb
            let disks = state.disks.join(", ");
            rows.push(row![
                state.id,
                state.capacity,
                state.used,
                disks,
                state.node,
                state.status
            ]);
        }
        rows
    }
}

#[async_trait(?Send)]
impl List for Pools {
    async fn list(output: utils::OutputFormat) {
        match RestClient::client().pools_api().get_pools().await {
            Ok(pools) => {
                utils::print_table::<openapi::models::Pool>(output, pools);
            }
            Err(e) => {
                println!("Failed to list pools. Error {}", e)
            }
        }
    }
}

/// Pool resource.
#[derive(StructOpt, Debug)]
pub(crate) struct Pool {
    /// ID of the pool.
    id: PoolId,
    output: String,
}

#[async_trait(?Send)]
impl Get for Pool {
    type ID = PoolId;
    async fn get(id: &Self::ID, output: utils::OutputFormat) {
        match RestClient::client().pools_api().get_pool(id).await {
            Ok(pool) => {
                let pool_to_vector: Vec<openapi::models::Pool> = vec![pool];
                utils::print_table::<openapi::models::Pool>(output, pool_to_vector);
            }
            Err(e) => {
                println!("Failed to get pool {}. Error {}", id, e)
            }
        }
    }
}
