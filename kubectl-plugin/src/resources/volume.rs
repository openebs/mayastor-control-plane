use crate::{
    operations::{Get, List, Scale},
    resources::{utils, ReplicaCount, VolumeId},
    rest_wrapper::RestClient,
};
use async_trait::async_trait;
use structopt::StructOpt;

use crate::resources::utils::{CreateRows, GetHeaderRow, OutputFormat};
use prettytable::Row;

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
            state.status,
            state.size
        ]];
        rows
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
