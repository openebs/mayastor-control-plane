pub struct NodeLabel {}
pub struct NodeLabels {}

use async_trait::async_trait;

use crate::{
    operations::{Get, List, PluginResult},
    resources::{
        error::Error,
        node::{node_display_print, node_display_print_one, NodeDisplayFormat},
        utils::OutputFormat,
        NodeId,
    },
    rest_wrapper::RestClient,
};

#[async_trait(?Send)]
impl Get for NodeLabel {
    type ID = NodeId;
    async fn get(id: &Self::ID, output: &OutputFormat) -> PluginResult {
        match RestClient::client().nodes_api().get_node(id).await {
            Ok(node) => node_display_print_one(node.into_body(), output, NodeDisplayFormat::Labels),
            Err(e) => {
                return Err(Error::GetNodeError {
                    id: id.to_string(),
                    source: e,
                });
            }
        }
        Ok(())
    }
}

#[async_trait(?Send)]
impl List for NodeLabels {
    async fn list(output: &OutputFormat) -> PluginResult {
        match RestClient::client().nodes_api().get_nodes(None).await {
            Ok(nodes) => {
                // iterate through the nodes and filter for only those that have labels
                let nodelist = nodes.into_body();
                let mut filteredlist = nodelist;
                // remove nodes with labels
                filteredlist
                    .retain(|i| i.spec.is_some() && i.spec.as_ref().unwrap().labels.is_some());
                node_display_print(filteredlist, output, NodeDisplayFormat::Labels)
            }
            Err(e) => {
                return Err(Error::ListNodesError { source: e });
            }
        }
        Ok(())
    }
}
