pub struct NodeDrain {}
pub struct NodeDrains {}

use async_trait::async_trait;

use crate::{
    operations::{Get, List},
    resources::{
        node::{node_display_print, node_display_print_one, NodeDisplayFormat},
        utils::OutputFormat,
        NodeId,
    },
    rest_wrapper::RestClient,
};

#[async_trait(?Send)]
impl Get for NodeDrain {
    type ID = NodeId;
    async fn get(id: &Self::ID, output: &OutputFormat) {
        match RestClient::client().nodes_api().get_node(id).await {
            Ok(node) => node_display_print_one(node.into_body(), output, NodeDisplayFormat::Drain),
            Err(e) => {
                println!("Failed to get node {}. Error {}", id, e)
            }
        }
    }
}

#[async_trait(?Send)]
impl List for NodeDrains {
    async fn list(output: &OutputFormat) {
        match RestClient::client().nodes_api().get_nodes().await {
            Ok(nodes) => {
                // iterate through the nodes and filter for only those that have drain labels
                // then print with the format NodeDisplayFormat::Drain
                let nodelist = nodes.into_body();
                let mut filteredlist = nodelist;
                // remove nodes with no drain labels
                filteredlist.retain(|i| {
                    i.spec.is_some() && !i.spec.as_ref().unwrap().drain_labels.is_empty()
                });
                node_display_print(filteredlist, output, NodeDisplayFormat::Drain);
            }
            Err(e) => {
                println!("Failed to list nodes. Error {}", e)
            }
        }
    }
}
