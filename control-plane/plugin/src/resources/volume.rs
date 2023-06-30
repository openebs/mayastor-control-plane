use crate::{
    operations::{Get, List, Scale},
    resources::{utils, VolumeId},
    rest_wrapper::RestClient,
};
use async_trait::async_trait;

use crate::{
    operations::{RebuildHistory, ReplicaTopology},
    resources::utils::{optional_cell, CreateRow, CreateRows, GetHeaderRow, OutputFormat},
};
use openapi::tower::client::Url;
use prettytable::Row;
use std::{collections::HashMap, str::FromStr};

/// Volumes resource.
#[derive(clap::Args, Debug)]
pub struct Volumes {}

impl CreateRow for openapi::models::Volume {
    fn row(&self) -> Row {
        let state = &self.state;
        row![
            state.uuid,
            self.spec.num_replicas,
            optional_cell(state.target.clone().map(|t| t.node)),
            optional_cell(state.target.as_ref().and_then(target_protocol)),
            state.status.clone(),
            ::utils::bytes::into_human(state.size),
            match self.spec.thin {
                true => "true",
                false if self.spec.as_thin == Some(true) => "true (snapped)",
                false => "false",
            },
            optional_cell(
                state
                    .usage
                    .as_ref()
                    .map(|u| ::utils::bytes::into_human(u.allocated))
            )
        ]
    }
}

/// Retrieve the protocol from a volume target and return it as an option
fn target_protocol(target: &openapi::models::Nexus) -> Option<openapi::models::Protocol> {
    match &target.protocol {
        openapi::models::Protocol::None => None,
        protocol => Some(*protocol),
    }
}

impl GetHeaderRow for openapi::models::Volume {
    fn get_header_row(&self) -> Row {
        (*utils::VOLUME_HEADERS).clone()
    }
}

#[async_trait(?Send)]
impl List for Volumes {
    async fn list(output: &utils::OutputFormat) {
        if let Some(volumes) = get_paginated_volumes().await {
            // Print table, json or yaml based on output format.
            utils::print_table(output, volumes);
        }
    }
}

/// Get the list of volumes over multiple paginated requests if necessary.
/// If any `get_volumes` request fails, `None` will be returned. This prevents the user from getting
/// a partial list when they expect a complete list.
async fn get_paginated_volumes() -> Option<Vec<openapi::models::Volume>> {
    // The number of volumes to get per request.
    let max_entries = 200;
    let mut starting_token = Some(0);
    let mut volumes = Vec::with_capacity(max_entries as usize);

    // The last paginated request will set the `starting_token` to `None`.
    while starting_token.is_some() {
        match RestClient::client()
            .volumes_api()
            .get_volumes(max_entries, None, starting_token)
            .await
        {
            Ok(vols) => {
                let v = vols.into_body();
                volumes.extend(v.entries);
                starting_token = v.next_token;
            }
            Err(e) => {
                println!("Failed to list volumes. Error {e}");
                return None;
            }
        }
    }

    Some(volumes)
}

/// Volume resource.
#[derive(clap::Args, Debug)]
pub struct Volume {}

#[async_trait(?Send)]
impl Get for Volume {
    type ID = VolumeId;
    async fn get(id: &Self::ID, output: &utils::OutputFormat) {
        match RestClient::client().volumes_api().get_volume(id).await {
            Ok(volume) => {
                // Print table, json or yaml based on output format.
                utils::print_table(output, volume.into_body());
            }
            Err(e) => {
                println!("Failed to get volume {id}. Error {e}")
            }
        }
    }
}

#[async_trait(?Send)]
impl Scale for Volume {
    type ID = VolumeId;
    async fn scale(id: &Self::ID, replica_count: u8, output: &utils::OutputFormat) {
        match RestClient::client()
            .volumes_api()
            .put_volume_replica_count(id, replica_count)
            .await
        {
            Ok(volume) => match output {
                OutputFormat::Yaml | OutputFormat::Json => {
                    // Print json or yaml based on output format.
                    utils::print_table(output, volume.into_body());
                }
                OutputFormat::None => {
                    // In case the output format is not specified, show a success message.
                    println!("Volume {id} scaled successfully 🚀")
                }
            },
            Err(e) => {
                println!("Failed to scale volume {id}. Error {e}")
            }
        }
    }
}

#[async_trait(?Send)]
impl ReplicaTopology for Volume {
    type ID = VolumeId;
    async fn topologies(output: &OutputFormat) {
        let volumes = VolumeTopologies(get_paginated_volumes().await.unwrap_or_default());
        utils::print_table(output, volumes);
    }
    async fn topology(id: &Self::ID, output: &OutputFormat) {
        match RestClient::client().volumes_api().get_volume(id).await {
            Ok(volume) => {
                // Print table, json or yaml based on output format.
                utils::print_table(output, volume.into_body().state.replica_topology);
            }
            Err(e) => {
                println!("Failed to get volume {id}. Error {e}")
            }
        }
    }
}

#[async_trait(?Send)]
impl RebuildHistory for Volume {
    type ID = VolumeId;
    async fn rebuild_history(id: &Self::ID, output: &OutputFormat) {
        match RestClient::client()
            .volumes_api()
            .get_rebuild_history(id)
            .await
        {
            Ok(history) => {
                utils::print_table(output, history.into_body());
            }
            Err(e) => {
                println!("Failed to get rebuild history for volume {id}. Error {e}")
            }
        }
    }
}

#[derive(Default, Debug, serde::Serialize, serde::Deserialize)]
struct VolumeTopologies(Vec<openapi::models::Volume>);

impl GetHeaderRow for VolumeTopologies {
    fn get_header_row(&self) -> Row {
        utils::REPLICA_TOPOLOGIES_PREFIX
            .iter()
            .chain(utils::REPLICA_TOPOLOGY_HEADERS.iter())
            .cloned()
            .collect()
    }
}
impl CreateRows for VolumeTopologies {
    fn create_rows(&self) -> Vec<Row> {
        self.0
            .iter()
            .flat_map(|volume| {
                let mut rows = Vec::new();
                volume
                    .state
                    .replica_topology
                    .create_rows()
                    .into_iter()
                    .enumerate()
                    .for_each(|(i, mut r)| {
                        let mut row = if i == 0 {
                            row![volume.spec.uuid]
                        } else if i < volume.state.replica_topology.len() - 1 {
                            row!["├─"]
                        } else {
                            row!["└─"]
                        };
                        for cell in r.iter_mut() {
                            row.add_cell(cell.clone());
                        }
                        rows.push(row);
                    });
                rows
            })
            .collect()
    }
}

impl GetHeaderRow for HashMap<String, openapi::models::ReplicaTopology> {
    fn get_header_row(&self) -> Row {
        (*utils::REPLICA_TOPOLOGY_HEADERS).clone()
    }
}

impl CreateRows for HashMap<String, openapi::models::ReplicaTopology> {
    fn create_rows(&self) -> Vec<Row> {
        self.iter()
            .map(|(id, topology)| {
                row![
                    id,
                    optional_cell(topology.node.as_ref()),
                    optional_cell(topology.pool.as_ref()),
                    topology.state,
                    optional_cell(
                        topology
                            .usage
                            .as_ref()
                            .map(|u| ::utils::bytes::into_human(u.capacity))
                    ),
                    optional_cell(
                        topology
                            .usage
                            .as_ref()
                            .map(|u| ::utils::bytes::into_human(u.allocated))
                    ),
                    optional_cell(topology.child_status.as_ref().map(|s| s.to_string())),
                    optional_cell(topology.child_status_reason.as_ref().map(|s| s.to_string())),
                    optional_cell(topology.rebuild_progress.map(|p| format!("{p}%"))),
                ]
            })
            .collect()
    }
}

impl GetHeaderRow for openapi::models::RebuildHistory {
    fn get_header_row(&self) -> Row {
        (*utils::REBUILD_HISTORY_HEADER).clone()
    }
}

impl CreateRows for openapi::models::RebuildHistory {
    fn create_rows(&self) -> Vec<Row> {
        self.records
            .iter()
            .map(|rec| {
                row![
                    child_uuid(rec.child_uri.as_str()),
                    child_uuid(rec.src_uri.as_str()),
                    rec.rebuild_job_state,
                    to_human_readable(rec.blocks_total),
                    to_human_readable(rec.blocks_recovered),
                    to_human_readable(rec.blocks_transferred),
                    to_human_readable(rec.block_size),
                    rec.is_partial,
                    rec.start_time,
                    rec.end_time,
                ]
            })
            .collect()
    }
}

fn child_uuid(uri: &str) -> String {
    let Ok(uri) = Url::from_str(uri) else { return "".into();};
    match uri.query_pairs().find(|(q, _)| q == "uuid") {
        Some((_, uuid)) => uuid.to_string(),
        _ => "".into(),
    }
}

fn to_human_readable(val: isize) -> String {
    ::utils::bytes::into_human(val as u64)
}
