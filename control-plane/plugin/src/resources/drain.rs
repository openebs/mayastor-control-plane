pub struct NodeDrain {}
pub struct NodeDrains {}

use async_trait::async_trait;
use openapi::models::CordonDrainState;

use crate::{
    operations::{Get, List, PluginResult},
    resources::{
        error::Error,
        node::{node_display_print, node_display_print_one, NodeDisplayFormat},
        utils::OutputFormat,
        GetResources, NodeId,
    },
    rest_wrapper::RestClient,
};

#[async_trait(?Send)]
impl Get for NodeDrain {
    type ID = NodeId;
    async fn get(
        id: &Self::ID,
        _get_resource: GetResources,
        output: &OutputFormat,
    ) -> PluginResult {
        match RestClient::client().nodes_api().get_node(id).await {
            Ok(node) => node_display_print_one(node.into_body(), output, NodeDisplayFormat::Drain),
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
impl List for NodeDrains {
    async fn list(output: &OutputFormat) -> PluginResult {
        match RestClient::client().nodes_api().get_nodes(None).await {
            Ok(nodes) => {
                // iterate through the nodes and filter for only those that have drain labels
                // then print with the format NodeDisplayFormat::Drain
                let nodelist = nodes.into_body();
                let mut filteredlist = nodelist;
                // remove nodes with no drain labels
                filteredlist.retain(|i| {
                    i.spec.is_some()
                        && match &i.spec.as_ref().unwrap().cordondrainstate {
                            Some(ds) => match ds {
                                CordonDrainState::cordonedstate(_) => false,
                                CordonDrainState::drainingstate(_) => true,
                                CordonDrainState::drainedstate(_) => true,
                            },
                            None => false,
                        }
                });
                node_display_print(filteredlist, output, NodeDisplayFormat::Drain);
            }
            Err(e) => {
                return Err(Error::ListNodesError { source: e });
            }
        }
        Ok(())
    }
}
