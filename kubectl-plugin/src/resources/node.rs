use crate::{
    operations::{Get, List},
    resources::{
        utils,
        utils::{CreateRows, GetHeaderRow},
        NodeId,
    },
    rest_wrapper::RestClient,
};
use async_trait::async_trait;
use prettytable::Row;
use structopt::StructOpt;

/// Nodes resource.
#[derive(StructOpt, Debug)]
pub struct Nodes {}

// CreateRows being trait for Node would create the rows from the list of
// Nodes returned from REST call.
impl CreateRows for openapi::models::Node {
    fn create_rows(&self) -> Vec<Row> {
        let spec = self.spec.clone().unwrap_or_default();
        // In case the state is not coming as filled, either due to node offline, fill in
        // spec data and mark the status as Unknown.
        let state = self.state.clone().unwrap_or(openapi::models::NodeState {
            id: spec.id,
            grpc_endpoint: spec.grpc_endpoint,
            status: openapi::models::NodeStatus::Unknown,
        });
        let rows = vec![row![self.id, state.grpc_endpoint, state.status,]];
        rows
    }
}

// GetHeaderRow being trait for Node would return the Header Row for
// Node.
impl GetHeaderRow for openapi::models::Node {
    fn get_header_row(&self) -> Row {
        (&*utils::NODE_HEADERS).clone()
    }
}

#[async_trait(?Send)]
impl List for Nodes {
    async fn list(output: &utils::OutputFormat) {
        match RestClient::client().nodes_api().get_nodes().await {
            Ok(nodes) => {
                // Print table, json or yaml based on output format.
                utils::print_table(output, nodes.into_body());
            }
            Err(e) => {
                println!("Failed to list nodes. Error {}", e)
            }
        }
    }
}

/// Node resource.
#[derive(StructOpt, Debug)]
pub(crate) struct Node {
    /// ID of the node.
    id: NodeId,
}

#[async_trait(?Send)]
impl Get for Node {
    type ID = NodeId;
    async fn get(id: &Self::ID, output: &utils::OutputFormat) {
        match RestClient::client().nodes_api().get_node(id).await {
            Ok(node) => {
                // Print table, json or yaml based on output format.
                utils::print_table(output, node.into_body());
            }
            Err(e) => {
                println!("Failed to get node {}. Error {}", id, e)
            }
        }
    }
}
