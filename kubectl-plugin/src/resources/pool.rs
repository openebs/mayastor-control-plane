use crate::{
    operations::{Get, List},
    resources::{
        utils,
        utils::{CreateRows, GetHeaderRow},
        PoolId,
    },
    rest_wrapper::RestClient,
};
use async_trait::async_trait;
use prettytable::Row;
use structopt::StructOpt;

/// Pools resource.
#[derive(StructOpt, Debug)]
pub struct Pools {}

// CreateRows being trait for Vec<Pool> would create the rows from the list of
// Pools returned from REST call.
impl CreateRows for Vec<openapi::models::Pool> {
    fn create_rows(&self) -> Vec<Row> {
        let mut rows: Vec<Row> = Vec::new();
        for pool in self {
            let mut managed = true;
            if pool.spec.is_none() {
                managed = false;
            }
            // The spec would be empty if it was not created using
            // control plane.
            let spec = pool.spec.clone().unwrap_or_default();
            // In case the state is not coming as filled, either due to pool, node lost, fill in
            // spec data and mark the status as Unknown.
            let state = pool.state.clone().unwrap_or(openapi::models::PoolState {
                capacity: 0,
                disks: spec.disks,
                id: spec.id,
                node: spec.node,
                status: openapi::models::PoolStatus::Unknown,
                used: 0,
            });
            let disks = state.disks.join(", ");
            rows.push(row![
                pool.id,
                state.capacity,
                state.used,
                disks,
                state.node,
                state.status,
                managed
            ]);
        }
        rows
    }
}

// GetHeaderRow being trait for Pool would return the Header Row for
// Pool.
impl GetHeaderRow for Vec<openapi::models::Pool> {
    fn get_header_row(&self) -> Row {
        (&*utils::POOLS_HEADERS).clone()
    }
}

#[async_trait(?Send)]
impl List for Pools {
    async fn list(output: &utils::OutputFormat) {
        match RestClient::client().pools_api().get_pools().await {
            Ok(pools) => {
                // Print table, json or yaml based on output format.
                utils::print_table::<openapi::models::Pool>(output, pools, "list");
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
}

#[async_trait(?Send)]
impl Get for Pool {
    type ID = PoolId;
    async fn get(id: &Self::ID, output: &utils::OutputFormat) {
        match RestClient::client().pools_api().get_pool(id).await {
            Ok(pool) => {
                // Print table, json or yaml based on output format.
                utils::print_table::<openapi::models::Pool>(output, vec![pool], "get");
            }
            Err(e) => {
                println!("Failed to get pool {}. Error {}", id, e)
            }
        }
    }
}
