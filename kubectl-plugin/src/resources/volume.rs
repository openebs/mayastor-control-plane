use crate::{
    operations::{Get, List, Scale},
    resources::{utils, ReplicaCount, VolumeId},
    rest_wrapper::RestClient,
};
use async_trait::async_trait;
use structopt::StructOpt;

use crate::{
    operations::ReplicaTopology,
    resources::utils::{optional_cell, CreateRows, GetHeaderRow, OutputFormat},
};
use prettytable::Row;
use std::collections::HashMap;

/// Volumes resource.
#[derive(StructOpt, Debug)]
pub(crate) struct Volumes {}

// CreateRows being trait for Vec<Volume> would create the rows from the list of
// Volumes returned from REST call.
impl CreateRows for openapi::models::Volume {
    fn create_rows(&self) -> Vec<Row> {
        let state = self.state.clone();
        let rows = vec![row![
            state.uuid,
            self.spec.num_replicas,
            optional_cell(state.target.clone().map(|t| t.node)),
            optional_cell(state.target.as_ref().map(|t| target_protocol(t)).flatten()),
            state.status,
            state.size
        ]];
        rows
    }
}

/// Retrieve the protocol from a volume target and return it as an option
fn target_protocol(target: &openapi::models::Nexus) -> Option<openapi::models::Protocol> {
    match &target.protocol {
        openapi::models::Protocol::None => None,
        protocol => Some(*protocol),
    }
}

// GetHeaderRow being trait for Volume would return the Header Row for
// Volume.
impl GetHeaderRow for openapi::models::Volume {
    fn get_header_row(&self) -> Row {
        (&*utils::VOLUME_HEADERS).clone()
    }
}

#[async_trait(?Send)]
impl List for Volumes {
    async fn list(output: &utils::OutputFormat) {
        match RestClient::client().volumes_api().get_volumes().await {
            Ok(volumes) => {
                // Print table, json or yaml based on output format.
                utils::print_table(output, volumes);
            }
            Err(e) => {
                println!("Failed to list volumes. Error {}", e)
            }
        }
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
    async fn get(id: &Self::ID, output: &utils::OutputFormat) {
        match RestClient::client().volumes_api().get_volume(id).await {
            Ok(volume) => {
                // Print table, json or yaml based on output format.
                utils::print_table(output, volume);
            }
            Err(e) => {
                println!("Failed to get volume {}. Error {}", id, e)
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
                    utils::print_table(output, volume);
                }
                OutputFormat::NoFormat => {
                    // Incase the output format is not specified, show a success message.
                    println!("Volume {} Scaled Successfully 🚀", id)
                }
            },
            Err(e) => {
                println!("Failed to scale volume {}. Error {}", id, e)
            }
        }
    }
}

#[async_trait(?Send)]
impl ReplicaTopology for Volume {
    type ID = VolumeId;
    async fn topology(id: &Self::ID, output: &OutputFormat) {
        match RestClient::client().volumes_api().get_volume(id).await {
            Ok(volume) => {
                // Print table, json or yaml based on output format.
                utils::print_table(output, volume.state.replica_topology);
            }
            Err(e) => {
                println!("Failed to get volume {}. Error {}", id, e)
            }
        }
    }
}

impl GetHeaderRow for HashMap<String, openapi::models::ReplicaTopology> {
    fn get_header_row(&self) -> Row {
        (&*utils::REPLICA_TOPOLOGY_HEADERS).clone()
    }
}

impl CreateRows for HashMap<String, openapi::models::ReplicaTopology> {
    fn create_rows(&self) -> Vec<Row> {
        let mut rows = vec![];
        self.iter().for_each(|(id, topology)| {
            rows.push(row![
                id,
                optional_cell(topology.node.as_ref()),
                optional_cell(topology.pool.as_ref()),
                topology.state,
            ])
        });
        rows
    }
}
