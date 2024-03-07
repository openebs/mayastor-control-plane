use crate::{
    operations::{Get, List, PluginResult},
    resources::{
        error::Error,
        utils,
        utils::{CreateRow, GetHeaderRow},
        GetResources, PoolId,
    },
    rest_wrapper::RestClient,
};
use async_trait::async_trait;
use prettytable::Row;

#[derive(Debug, Clone, clap::Args)]
/// Arguments used when getting a pool.
pub struct GetPoolArgs {
    /// Id of the pool to get.
    pool_id: PoolId,
}

impl GetPoolArgs {
    /// Return the pool ID.
    pub fn pool_id(&self) -> PoolId {
        self.pool_id.clone()
    }
}

/// Pools resource.
#[derive(clap::Args, Debug)]
pub struct Pools {}

impl CreateRow for openapi::models::Pool {
    fn row(&self) -> Row {
        // The spec would be empty if it was not created using
        // control plane.
        let managed = self.spec.is_some();
        let spec = self.spec.clone().unwrap_or_default();
        // In case the state is not coming as filled, either due to pool, node lost, fill in
        // spec data and mark the status as Unknown.
        let state = self.state.clone().unwrap_or(openapi::models::PoolState {
            capacity: 0,
            disks: spec.disks,
            id: spec.id,
            node: spec.node,
            status: openapi::models::PoolStatus::Unknown,
            used: 0,
            committed: None,
        });
        let free = if state.capacity > state.used {
            state.capacity - state.used
        } else {
            0
        };
        let disks = state.disks.join(", ");
        row![
            self.id,
            disks,
            managed,
            state.node,
            state.status,
            ::utils::bytes::into_human(state.capacity),
            ::utils::bytes::into_human(state.used),
            ::utils::bytes::into_human(free),
            utils::optional_cell(state.committed.map(::utils::bytes::into_human)),
        ]
    }
}

// GetHeaderRow being trait for Pool would return the Header Row for
// Pool.
impl GetHeaderRow for openapi::models::Pool {
    fn get_header_row(&self) -> Row {
        (*utils::POOLS_HEADERS).clone()
    }
}

#[async_trait(?Send)]
impl List for Pools {
    async fn list(output: &utils::OutputFormat) -> PluginResult {
        match RestClient::client().pools_api().get_pools().await {
            Ok(pools) => {
                // Print table, json or yaml based on output format.
                utils::print_table(output, pools.into_body());
            }
            Err(e) => {
                return Err(Error::ListPoolsError { source: e });
            }
        }
        Ok(())
    }
}

/// Pool resource.
#[derive(clap::Args, Debug)]
pub struct Pool {}

#[async_trait(?Send)]
impl Get for Pool {
    type ID = PoolId;
    async fn get(
        id: &Self::ID,
        _get_resource: GetResources,
        output: &utils::OutputFormat,
    ) -> PluginResult {
        match RestClient::client().pools_api().get_pool(id).await {
            Ok(pool) => {
                // Print table, json or yaml based on output format.
                utils::print_table(output, pool.into_body());
            }
            Err(e) => {
                return Err(Error::GetPoolError {
                    id: id.to_string(),
                    source: e,
                });
            }
        }
        Ok(())
    }
}
