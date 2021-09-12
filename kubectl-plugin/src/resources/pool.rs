use crate::{
    operations::{Get, List},
    resources::{utils, OutputFormat, PoolId},
    rest_wrapper::RestClient,
};
use async_trait::async_trait;
use prettytable::Row;
use structopt::StructOpt;
/// Pools resource.
#[derive(StructOpt, Debug)]
pub struct Pools {
    output: OutputFormat,
}

#[async_trait(?Send)]
impl List for Pools {
    type Format = OutputFormat;
    async fn list(output: &Self::Format) {
        match RestClient::client().pools_api().get_pools().await {
            Ok(pools) => match output.to_lowercase().trim() {
                "" => {
                    // Show the tabular form if output format is not specified.
                    let rows: Vec<Row> = utils::create_pool_rows(pools);
                    utils::table_printer((&*utils::POOLS_HEADERS).clone(), rows);
                }
                utils::YAML_FORMAT => {
                    // Show the YAML form output if output format is YAML.
                    let s = serde_yaml::to_string(&pools).unwrap();
                    println!("{}", s);
                }
                utils::JSON_FORMAT => {
                    // Show the JSON form output if output format is JSON.
                    let s = serde_json::to_string(&pools).unwrap();
                    println!("{}", s);
                }
                _ => {
                    // Incase of invalid output format, show error and gracefully end.
                    println!("Output not supported for {} format", output);
                }
            },
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
    output: OutputFormat,
}

#[async_trait(?Send)]
impl Get for Pool {
    type ID = PoolId;
    type Format = OutputFormat;
    async fn get(id: &Self::ID, output: &Self::Format) {
        match RestClient::client().pools_api().get_pool(id).await {
            Ok(pool) => match output.to_lowercase().trim() {
                "" => {
                    // Show the tabular form if outpur format is not specified.
                    // Convert the output to a vector to be used in the method.
                    let pool_to_vector: Vec<openapi::models::Pool> = vec![pool];
                    let rows: Vec<Row> = utils::create_pool_rows(pool_to_vector);
                    utils::table_printer((&*utils::POOLS_HEADERS).clone(), rows);
                }
                utils::YAML_FORMAT => {
                    // Show the YAML form output if output format is YAML.
                    let s = serde_yaml::to_string(&pool).unwrap();
                    println!("{}", s);
                }
                utils::JSON_FORMAT => {
                    // Show the JSON form output if output format is JSON.
                    let s = serde_json::to_string(&pool).unwrap();
                    println!("{}", s);
                }
                _ => {
                    // Incase of invalid output format, show error and gracefully end.
                    println!("Output not supported for {} format", output);
                }
            },
            Err(e) => {
                println!("Failed to get pool {}. Error {}", id, e)
            }
        }
    }
}
