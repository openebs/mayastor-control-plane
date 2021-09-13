use crate::{
    operations::{Get, List, Scale},
    resources::{utils, OutputFormat, ReplicaCount, VolumeId},
    rest_wrapper::RestClient,
};
use async_trait::async_trait;
use structopt::StructOpt;

use prettytable::Row;

/// Volumes resource.
#[derive(StructOpt, Debug)]
pub(crate) struct Volumes {
    output: OutputFormat,
}

#[async_trait(?Send)]
impl List for Volumes {
    type Format = OutputFormat;
    async fn list(output: &Self::Format) {
        match RestClient::client().volumes_api().get_volumes().await {
            Ok(volumes) => match output.to_lowercase().trim() {
                utils::YAML_FORMAT => {
                    // Show the YAML form output if output format is YAML.
                    let s = serde_yaml::to_string(&volumes).unwrap();
                    println!("{}", s);
                }
                utils::JSON_FORMAT => {
                    // Show the JSON form output if output format is JSON.
                    let s = serde_json::to_string(&volumes).unwrap();
                    println!("{}", s);
                }
                _ => {
                    // Show the tabular form if output format is not specified.
                    let rows: Vec<Row> = utils::create_volume_rows(volumes);
                    utils::table_printer((&*utils::VOLUME_HEADERS).clone(), rows);
                }
            },
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
    output: OutputFormat,
}

#[async_trait(?Send)]
impl Get for Volume {
    type ID = VolumeId;
    type Format = OutputFormat;
    async fn get(id: &Self::ID, output: &Self::Format) {
        match RestClient::client().volumes_api().get_volume(id).await {
            Ok(volume) => match output.to_lowercase().trim() {
                utils::YAML_FORMAT => {
                    // Show the YAML form output if output format is YAML.
                    let s = serde_yaml::to_string(&volume).unwrap();
                    println!("{}", s);
                }
                utils::JSON_FORMAT => {
                    // Show the JSON form output if output format is JSON.
                    let s = serde_json::to_string(&volume).unwrap();
                    println!("{}", s);
                }
                _ => {
                    // Show the tabular form if output format is not specified.
                    // Convert the output to a vector to be used in the method.
                    let volume_to_vector: Vec<openapi::models::Volume> = vec![volume];
                    let rows: Vec<Row> = utils::create_volume_rows(volume_to_vector);
                    utils::table_printer((&*utils::VOLUME_HEADERS).clone(), rows);
                }
            },
            Err(e) => {
                println!("Failed to get volume {}. Error {}", id, e)
            }
        }
    }
}

#[async_trait(?Send)]
impl Scale for Volume {
    type ID = VolumeId;
    type Format = OutputFormat;
    async fn scale(id: &Self::ID, replica_count: u8, output: &Self::Format) {
        match RestClient::client()
            .volumes_api()
            .put_volume_replica_count(id, replica_count)
            .await
        {
            Ok(volume) => match output.to_lowercase().trim() {
                utils::YAML_FORMAT => {
                    // Show the YAML form output if output format is YAML.
                    let s = serde_yaml::to_string(&volume).unwrap();
                    println!("{}", s);
                }
                utils::JSON_FORMAT => {
                    // Show the JSON form output if output format is JSON.
                    let s = serde_json::to_string_pretty(&volume).unwrap();
                    println!("{}", s);
                }
                _ => {
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
