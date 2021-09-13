use crate::{
    operations::{Get, List, Scale},
    resources::{utils, ReplicaCount, VolumeId},
    rest_wrapper::RestClient,
};
use async_trait::async_trait;
use structopt::StructOpt;

use crate::resources::utils::{CreateRows, OutputFormat};
use prettytable::Row;

/// Volumes resource.
#[derive(StructOpt, Debug)]
pub(crate) struct Volumes {
    output: String,
}

impl CreateRows for Vec<openapi::models::Volume> {
    fn create_rows(&self) -> Vec<Row> {
        let mut rows: Vec<Row> = Vec::new();
        for volume in self {
            let state = volume.state.as_ref().unwrap();
            rows.push(row![
                state.uuid,
                volume.spec.num_paths,
                volume.spec.num_replicas,
                state.protocol,
                state.status,
                state.size
            ]);
        }
        rows
    }
}

#[async_trait(?Send)]
impl List for Volumes {
    async fn list(output: utils::OutputFormat) {
        match RestClient::client().volumes_api().get_volumes().await {
            Ok(volumes) => {
                utils::print_table::<openapi::models::Volume>(output, volumes);
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
    output: String,
}

#[async_trait(?Send)]
impl Get for Volume {
    type ID = VolumeId;
    async fn get(id: &Self::ID, output: utils::OutputFormat) {
        match RestClient::client().volumes_api().get_volume(id).await {
            Ok(volume) => {
                let volume_to_vector: Vec<openapi::models::Volume> = vec![volume];
                utils::print_table::<openapi::models::Volume>(output, volume_to_vector);
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
    async fn scale(id: &Self::ID, replica_count: u8, output: utils::OutputFormat) {
        match RestClient::client()
            .volumes_api()
            .put_volume_replica_count(id, replica_count)
            .await
        {
            Ok(volume) => match output {
                OutputFormat::Yaml | OutputFormat::Json => {
                    let volume_to_vector: Vec<openapi::models::Volume> = vec![volume];
                    utils::print_table::<openapi::models::Volume>(output, volume_to_vector);
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
