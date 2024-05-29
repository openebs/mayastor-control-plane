use crate::{
    operations::{Get, ListWithArgs, PluginResult},
    resources::{
        error::Error,
        utils,
        utils::{CreateRow, GetHeaderRow},
        NodeId, PoolId,
    },
    rest_wrapper::RestClient,
};
use async_trait::async_trait;
use prettytable::Row;
use std::collections::HashMap;

use super::VolumeId;

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

/// Arguments used when getting pools.
#[derive(Debug, Clone, clap::Args)]
pub struct GetPoolsArgs {
    /// Gets Pools from this node only.
    #[clap(long)]
    node: Option<NodeId>,

    /// Gets Pools for the given volume.
    #[clap(long)]
    volume: Option<VolumeId>,

    /// Selector (label query) to filter on, supports '=' only.
    /// (e.g. -l key1=value1,key2=value2).
    /// Pools must satisfy all of the specified label constraints.
    #[clap(short = 'l', long)]
    selector: Option<String>,
}

impl GetPoolsArgs {
    /// Return the node ID.
    pub fn node(&self) -> &Option<NodeId> {
        &self.node
    }

    /// Return the volume ID.
    pub fn volume(&self) -> &Option<VolumeId> {
        &self.volume
    }

    /// Select the pools based on labels.
    pub fn selector(&self) -> &Option<String> {
        &self.selector
    }
}

#[async_trait(?Send)]
impl ListWithArgs for Pools {
    type Args = GetPoolsArgs;
    async fn list(args: &Self::Args, output: &utils::OutputFormat) -> PluginResult {
        let mut pools = match args.node() {
            Some(node_id) => RestClient::client()
                .pools_api()
                .get_node_pools(node_id)
                .await
                .map(|pools| pools.into_body())
                .map_err(|e| Error::ListPoolsError { source: e }),
            None => RestClient::client()
                .pools_api()
                .get_pools(args.volume().as_ref())
                .await
                .map(|pools| pools.into_body())
                .map_err(|e| Error::ListPoolsError { source: e }),
        }?;

        pools.retain(|pool| match &pool.spec {
            Some(spec) => match &spec.labels {
                Some(pool_labels) => {
                    let pool_label_match =
                        labels_matched(pool_labels, args.selector()).unwrap_or(false);
                    pool_label_match
                }
                None => true,
            },
            None => true,
        });
        utils::print_table(output, pools);
        Ok(())
    }
}

/// Pool resource.
#[derive(clap::Args, Debug)]
pub struct Pool {}

#[async_trait(?Send)]
impl Get for Pool {
    type ID = PoolId;
    async fn get(id: &Self::ID, output: &utils::OutputFormat) -> PluginResult {
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

/// Check if the labels match the pool labels.
pub(crate) fn labels_matched(
    pool_labels: &HashMap<String, String>,
    labels: &Option<String>,
) -> Result<bool, Error> {
    match labels {
        Some(filter_labels) => {
            for label in filter_labels.split(',') {
                let [key, value] = label.split('=').collect::<Vec<_>>()[..] else {
                    return Err(Error::LabelNodeFilter {
                        labels: filter_labels.to_string(),
                    });
                };
                if pool_labels.get(key) != Some(&value.to_string()) {
                    return Ok(false);
                }
            }
        }
        None => return Ok(true),
    }
    Ok(true)
}
