#![feature(once_cell)]

mod operations;
mod resources;
mod rest_client;

use crate::resources::ResourceFactory;
use anyhow::Result;
use operations::Operations;
use reqwest::{Response, Url};
use rest_client::RestClient;
use serde_json::Value;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
struct CliArgs {
    /// Rest endpoint.
    #[structopt(long, short)]
    rest: Url,
    /// The operation to be performed.
    #[structopt(subcommand)]
    operations: Operations,
}

#[tokio::main]
async fn main() {
    let cli_args = &CliArgs::from_args();

    // Initialise the REST client.
    RestClient::init(&cli_args.rest);

    // Perform the requested operation.
    let response = match &cli_args.operations {
        Operations::List(resource) => resource.instance().list().await,
        Operations::Get(resource) => resource.instance().get().await,
        Operations::Scale(resource) => resource.instance().scale().await,
    };

    // TODO: Tabulate output by default. Output as JSON if an output argument is supplied.
    match response {
        Ok(r) => {
            if let Err(e) = pretty_print(r).await {
                println!("Failed to pretty print response. Error {}", e);
            }
        }
        Err(e) => {
            println!("Operation failed with error {}", e);
        }
    };
}

/// Print the response as pretty JSON.
/// Printing the response is the last thing we do, so if there is an error just output an
/// appropriate message as there isn't anything else to do.
async fn pretty_print(response: Response) -> Result<()> {
    let response_text = response.text().await?;
    let response_value: Value = serde_json::from_str(&response_text)?;
    let pretty_string = serde_json::to_string_pretty(&response_value)?;
    println!("{}", pretty_string);
    Ok(())
}
