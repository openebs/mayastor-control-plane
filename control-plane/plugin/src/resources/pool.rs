extern crate utils as external_utils;
use super::VolumeId;
use crate::{
    operations::{Cordoning, GetWithArgs, Label, ListWithArgs, PluginResult},
    resources::{
        error::{Error, LabelAssignSnafu, OpError, TopologyError},
        utils,
        utils::{
            optional_cell, print_table, validate_topology_key, validate_topology_value, CreateRow,
            CreateRows, GetHeaderRow, OutputFormat,
        },
        NodeId, PoolId,
    },
    rest_wrapper::RestClient,
};

use async_trait::async_trait;
use openapi::{apis::StatusCode, models, models::PoolCordonDrain};
use prettytable::{Cell, Row};
use serde::Serialize;
use snafu::ResultExt;
use std::collections::HashMap;
use strum_macros::{AsRefStr, Display, EnumString};

/// Pools resource.
#[derive(clap::Args, Debug)]
pub struct Pools {}

#[derive(AsRefStr, EnumString, Display)]
enum PoolCordonDrainState {
    Cordoned,
}

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
            encrypted: spec.encryption.is_some(),
        });
        let free = if state.capacity > state.used {
            state.capacity - state.used
        } else {
            0
        };
        let disks = state.disks.join(", ");
        let statuses = match spec.cordon_drain {
            None => format!("{:?}", state.status),
            Some(_) => {
                format!("{:?}, {}", state.status, PoolCordonDrainState::Cordoned)
            }
        };
        row![
            self.id,
            disks,
            managed,
            state.node,
            statuses,
            ::utils::bytes::into_human(state.capacity),
            ::utils::bytes::into_human(state.used),
            ::utils::bytes::into_human(free),
            optional_cell(state.committed.map(::utils::bytes::into_human)),
            state.encrypted
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

/// Arguments used when getting a pool.
#[derive(Debug, Clone, clap::Args)]
pub struct GetPoolArgs {
    /// Id of the pool.
    pool_id: PoolId,
    /// Show the labels of the pool.
    #[clap(long, default_value = "false")]
    show_labels: bool,
    /// Show the cordoned resources.
    #[clap(long)]
    show_cordons: bool,
}

impl GetPoolArgs {
    /// Return the pool ID.
    pub fn pool_id(&self) -> PoolId {
        self.pool_id.clone()
    }
    /// Return whether to show the labels of the pool.
    pub fn show_labels(&self) -> bool {
        self.show_labels
    }
    /// Return whether to show the cordoned resources of the pool.
    pub fn show_cordons(&self) -> bool {
        self.show_cordons
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

    /// Show the labels of the pool.
    #[clap(long, default_value = "false")]
    show_labels: bool,

    /// Show the cordoned resources.
    #[clap(long)]
    show_cordons: bool,
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

    /// Return whether to show the labels of the pool.
    pub fn show_labels(&self) -> bool {
        self.show_labels
    }

    /// Return whether to show the cordoned resources of the pool.
    pub fn show_cordons(&self) -> bool {
        self.show_cordons
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

        let pools_display =
            PoolDisplay::new_pools(pools.clone(), args.show_labels, args.show_cordons);
        match output {
            OutputFormat::Yaml | OutputFormat::Json => {
                print_table(output, pools_display.inner);
            }
            OutputFormat::None => {
                print_table(output, pools_display);
            }
        }

        Ok(())
    }
}

/// Pool resource.
#[derive(clap::Args, Debug)]
pub struct Pool {}

#[async_trait(?Send)]
impl GetWithArgs for Pool {
    type ID = PoolId;
    type Args = GetPoolArgs;
    async fn get(id: &Self::ID, args: &Self::Args, output: &utils::OutputFormat) -> PluginResult {
        match RestClient::client().pools_api().get_pool(id).await {
            Ok(pool) => match output {
                OutputFormat::Yaml | OutputFormat::Json => {
                    print_table(output, pool.clone().into_body());
                }
                OutputFormat::None => {
                    print_table(
                        output,
                        PoolDisplay::new(pool.into_body(), args.show_labels, args.show_cordons),
                    );
                }
            },
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

#[async_trait(?Send)]
impl Label for Pool {
    type ID = PoolId;
    async fn label(
        id: &Self::ID,
        label: String,
        overwrite: bool,
        output: &utils::OutputFormat,
    ) -> PluginResult {
        let result = if label.contains('=') {
            let [key, value] = label.split('=').collect::<Vec<_>>()[..] else {
                return Err(TopologyError::LabelMultiAssign {}.into());
            };

            validate_topology_key(key).context(super::error::PoolLabelFormatSnafu)?;
            validate_topology_value(value).context(super::error::PoolLabelFormatSnafu)?;
            match RestClient::client()
                .pools_api()
                .put_pool_label(id, key, value, Some(overwrite))
                .await
            {
                Err(source) => match source.status() {
                    Some(StatusCode::UNPROCESSABLE_ENTITY) if output.none() => {
                        Err(OpError::LabelExists {
                            resource: "Pool".to_string(),
                            id: id.to_string(),
                        })
                    }
                    Some(StatusCode::PRECONDITION_FAILED) if output.none() => {
                        Err(OpError::LabelConflict {
                            resource: "Pool".to_string(),
                            id: id.to_string(),
                        })
                    }
                    Some(StatusCode::NOT_FOUND) if output.none() => {
                        Err(OpError::ResourceNotFound {
                            resource: "Pool".to_string(),
                            id: id.to_string(),
                        })
                    }
                    _ => Err(OpError::Generic {
                        resource: "Pool".to_string(),
                        id: id.to_string(),
                        source,
                    }),
                },
                Ok(pool) => Ok(pool),
            }
        } else {
            snafu::ensure!(label.len() >= 2 && label.ends_with('-'), LabelAssignSnafu);
            let key = &label[..label.len() - 1];
            validate_topology_key(key)?;
            match RestClient::client()
                .pools_api()
                .del_pool_label(id, key)
                .await
            {
                Err(source) => match source.status() {
                    Some(StatusCode::PRECONDITION_FAILED) if output.none() => {
                        Err(OpError::LabelNotFound {
                            resource: "Pool".to_string(),
                            id: id.to_string(),
                        })
                    }
                    Some(StatusCode::NOT_FOUND) if output.none() => {
                        Err(OpError::ResourceNotFound {
                            resource: "Pool".to_string(),
                            id: id.to_string(),
                        })
                    }
                    _ => Err(OpError::Generic {
                        resource: "Pool".to_string(),
                        id: id.to_string(),
                        source,
                    }),
                },
                Ok(pool) => Ok(pool),
            }
        }?;
        let pool = result.into_body();
        match output {
            OutputFormat::Yaml | OutputFormat::Json => {
                // Print json or yaml based on output format.
                print_table(output, pool);
            }
            OutputFormat::None => {
                // In case the output format is not specified, show a success message.
                let mut labels = pool.spec.unwrap().labels.unwrap_or_default();
                let internal_label = external_utils::dsp_created_by_key();
                labels.remove(&internal_label);
                println!("Pool {id} labelled successfully. Current labels: {labels:?}");
            }
        }
        Ok(())
    }
}

/// The PoolDisplay structure is responsible for controlling the display formatting of Pool
/// objects. `#[serde(flatten)]` and `#[serde(skip)]` attributes are used to ensure that when the
/// object is serialised, only the `inner` object is represented.
#[derive(Serialize, Debug)]
pub struct PoolDisplay {
    #[serde(flatten)]
    pub inner: Vec<openapi::models::Pool>,
    #[serde(skip)]
    show_labels: bool,
    #[serde(skip)]
    show_cordons: bool,
}

impl PoolDisplay {
    /// Create a new `PoolDisplay` instance.
    pub(crate) fn new(pool: openapi::models::Pool, show_labels: bool, show_cordons: bool) -> Self {
        let vec: Vec<openapi::models::Pool> = vec![pool];
        Self {
            inner: vec,
            show_labels,
            show_cordons,
        }
    }
    /// Create a new `PoolDisplay` instance from a vector of pools.
    pub(crate) fn new_pools(
        pools: Vec<openapi::models::Pool>,
        show_labels: bool,
        show_cordons: bool,
    ) -> Self {
        Self {
            inner: pools,
            show_labels,
            show_cordons,
        }
    }

    /// Get a list of pool labels.
    pub(crate) fn pool_label_list(pool: &openapi::models::Pool) -> Vec<String> {
        let mut pools_labels: Vec<String> = vec![];
        let internal_label = external_utils::dsp_created_by_key();

        if let Some(spec) = &pool.spec {
            if let Some(ds) = &spec.labels {
                pools_labels = ds
                    .iter()
                    // Don't return the created_by_dsp label for the gets
                    .filter(|(key, _)| *key != &internal_label)
                    .map(|(key, value)| format!("{}={}", key, value))
                    .collect();
            }
        }
        pools_labels
    }
}

// Create the header for a `PoolDisplay` object.
impl GetHeaderRow for PoolDisplay {
    fn get_header_row(&self) -> Row {
        let mut header = (*utils::POOLS_HEADERS).clone();
        if self.show_labels {
            header.extend(vec!["LABELS"]);
        }
        if self.show_cordons {
            header.extend(vec!["CORDONS"]);
        }
        header
    }
}

impl CreateRows for PoolDisplay {
    fn create_rows(&self) -> Vec<Row> {
        let mut rows = vec![];
        for pool in self.inner.iter() {
            let mut row = pool.row();
            if self.show_labels {
                let labelstring = PoolDisplay::pool_label_list(pool).join(", ");
                // Add the pool labels to each row.
                row.add_cell(Cell::new(&labelstring));
            }
            if self.show_cordons {
                row.add_cell(Cell::new(&pool_cordon_resources(pool)));
            }
            rows.push(row);
        }
        rows
    }
}

#[derive(Debug, Clone, clap::Args)]
pub struct CordonReq {
    /// No new replicas can be created on the pool.
    #[clap(long)]
    pub replicas: bool,
    /// No new snapshots can be created on the pool.
    #[clap(long)]
    pub snapshots: bool,
    /// No new restores can be created on the pool.
    #[clap(long)]
    pub restores: bool,
}

#[derive(Debug, Clone, clap::Args)]
pub struct UncordonReq {
    /// New replicas can be created on the pool.
    #[clap(long)]
    pub replicas: bool,
    /// New snapshots can be created on the pool.
    #[clap(long)]
    pub snapshots: bool,
    /// New restores can be created on the pool.
    #[clap(long)]
    pub restores: bool,
}

#[async_trait(?Send)]
impl Cordoning for Pool {
    type ID = PoolId;
    type CREQ = Option<CordonReq>;
    type UREQ = Option<UncordonReq>;

    async fn cordon(id: &Self::ID, resources: &Self::CREQ, output: &OutputFormat) -> PluginResult {
        let resources = resources.clone().unwrap_or(CordonReq {
            replicas: true,
            snapshots: false,
            restores: true,
        });
        let body = models::PoolCordonReq::new_all(
            resources.replicas,
            resources.snapshots,
            resources.restores,
        );
        match RestClient::client()
            .pools_api()
            .put_pool_cordon(id, body)
            .await
        {
            Ok(node) => match output {
                OutputFormat::Yaml | OutputFormat::Json => {
                    // Print json or yaml based on output format.
                    utils::print_table(output, node.into_body());
                }
                OutputFormat::None => {
                    // In case the output format is not specified, show a success message.
                    println!("Pool {id} cordoned successfully")
                }
            },
            Err(source) => {
                if source.error_body().map(|b| b.kind)
                    == Some(models::rest_json_error::Kind::AlreadyExists)
                {
                    println!("Pool {id} already cordoned");
                } else {
                    return Err(Error::PoolCordonError {
                        id: id.to_string(),
                        source,
                    });
                }
            }
        }
        Ok(())
    }

    async fn uncordon(
        id: &Self::ID,
        resources: &Self::UREQ,
        output: &OutputFormat,
    ) -> PluginResult {
        let resources = resources.clone().unwrap_or(UncordonReq {
            replicas: true,
            snapshots: true,
            restores: true,
        });
        let body = models::PoolCordonReq::new_all(
            resources.replicas,
            resources.snapshots,
            resources.restores,
        );
        let (failed, pool) = match RestClient::client()
            .pools_api()
            .del_pool_cordon(id, body)
            .await
        {
            Ok(pool) => Ok((false, pool)),
            Err(source)
                if source.error_body().map(|b| b.kind)
                    == Some(models::rest_json_error::Kind::AlreadyExists) =>
            {
                RestClient::client()
                    .pools_api()
                    .get_pool(id)
                    .await
                    .map_err(|source| Error::GetPoolError {
                        id: id.to_string(),
                        source,
                    })
                    .map(|p| (true, p))
            }
            Err(source) => Err(Error::PoolUncordonError {
                id: id.to_string(),
                source,
            }),
        }?;
        match output {
            OutputFormat::Yaml | OutputFormat::Json => {
                // Print json or yaml based on output format.
                utils::print_table(output, pool.into_body());
            }
            OutputFormat::None => {
                // In case the output format is not specified, show a success message.
                let resources = pool_cordon_resources(&pool.into_body());
                if failed {
                    if resources.is_empty() {
                        println!("Pool {id} already uncordoned");
                    } else {
                        println!("Pool {id} already partially uncordoned. Remaining cordoned resources: {resources:?}");
                    }
                } else if resources.is_empty() {
                    println!("Pool {id} successfully uncordoned");
                } else {
                    println!("Pool {id} partially uncordoned. Remaining cordoned resources: {resources:?}");
                }
            }
        }
        Ok(())
    }
}

fn resource(yes: bool, name: &str) -> &str {
    if yes {
        name
    } else {
        ""
    }
}
fn cordon_resources(rsc: &models::PoolCordon) -> String {
    [
        resource(rsc.replicas, "replicas"),
        resource(rsc.snapshots, "snapshots"),
        resource(rsc.restores, "restores"),
    ]
    .into_iter()
    .filter(|s| !s.is_empty())
    .collect::<Vec<&str>>()
    .join(",")
}
fn pool_cordon_resources(pool: &models::Pool) -> String {
    match &pool.spec {
        Some(spec) => match &spec.cordon_drain {
            Some(cds) => match cds {
                PoolCordonDrain::cordoned(rsc) => cordon_resources(rsc),
            },
            None => String::new(),
        },
        None => String::new(),
    }
}
