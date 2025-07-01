pub struct NodeCordon {}
pub struct PoolCordon {}

use async_trait::async_trait;
use openapi::models::CordonDrainState;

use crate::{
    operations::{Get, List, PluginResult},
    resources::{
        error::Error,
        node::{node_display_print, node_display_print_one, NodeDisplayFormat},
        pool::PoolDisplay,
        utils::{print_table, OutputFormat},
        NodeId, PoolId,
    },
    rest_wrapper::RestClient,
};

#[async_trait(?Send)]
impl Get for NodeCordon {
    type ID = NodeId;
    async fn get(id: &Self::ID, output: &OutputFormat) -> PluginResult {
        match RestClient::client().nodes_api().get_node(id).await {
            Ok(node) => {
                node_display_print_one(node.into_body(), output, NodeDisplayFormat::CordonLabels)
            }
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
impl List for NodeCordon {
    async fn list(output: &OutputFormat) -> PluginResult {
        match RestClient::client().nodes_api().get_nodes(None).await {
            Ok(nodes) => {
                // iterate through the nodes and filter for only those that have cordon or drain
                // labels
                let nodelist = nodes.into_body();
                let mut filteredlist = nodelist;
                // remove nodes with no cordon or drain labels
                filteredlist.retain(|i| {
                    i.spec.is_some()
                        && match &i.spec.as_ref().unwrap().cordondrainstate {
                            Some(ds) => match ds {
                                CordonDrainState::cordonedstate(_) => true,
                                CordonDrainState::drainingstate(_) => true,
                                CordonDrainState::drainedstate(_) => true,
                            },
                            None => false,
                        }
                });
                node_display_print(filteredlist, output, NodeDisplayFormat::CordonLabels)
            }
            Err(e) => {
                return Err(Error::ListNodesError { source: e });
            }
        }
        Ok(())
    }
}

#[async_trait(?Send)]
impl Get for PoolCordon {
    type ID = PoolId;
    async fn get(id: &Self::ID, output: &OutputFormat) -> PluginResult {
        match RestClient::client().pools_api().get_pool(id).await {
            Ok(pool) => {
                let display = PoolDisplay::new(pool.into_body(), false, true);
                match output {
                    OutputFormat::Yaml | OutputFormat::Json => {
                        print_table(output, display.inner);
                    }
                    OutputFormat::None => {
                        print_table(output, display);
                    }
                }
            }
            Err(e) => {
                return Err(Error::GetPoolError {
                    id: id.to_string(),
                    source: e,
                });
            }
        }
        Ok(())
    }
}

#[async_trait(?Send)]
impl List for PoolCordon {
    async fn list(output: &OutputFormat) -> PluginResult {
        match RestClient::client().pools_api().get_pools(None).await {
            Ok(pools) => {
                // iterate through the nodes and filter for only those that have cordon or drain
                // labels
                let mut pools = pools.into_body();
                // remove nodes with no cordon or drain labels
                pools.retain(|i| {
                    i.spec
                        .as_ref()
                        .map(|spec| spec.cordon_drain.is_some())
                        .unwrap_or_default()
                });
                let display = PoolDisplay::new_pools(pools, false, true);
                match output {
                    OutputFormat::Yaml | OutputFormat::Json => {
                        print_table(output, display.inner);
                    }
                    OutputFormat::None => {
                        print_table(output, display);
                    }
                }
            }
            Err(e) => {
                return Err(Error::ListPoolsError { source: e });
            }
        }
        Ok(())
    }
}
