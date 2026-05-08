use crate::{
    operations::{Get, ListWithArgs, PluginResult},
    resources::{
        utils,
        utils::{optional_cell, CreateRow, GetHeaderRow},
        Error, NodeId,
    },
    rest_wrapper::RestClient,
};
use async_trait::async_trait;
use itertools::Itertools;
use prettytable::Row;

/// App-Nodes resource.
#[derive(clap::Args, Debug)]
pub struct AppNodes {}

/// Arguments used when getting an application/csi node.
#[derive(Debug, Clone, clap::Args)]
pub struct GetAppNodeArgs {
    /// Id of the application/csi node.
    pub(crate) node_id: NodeId,
}

/// Arguments used when getting all application/csi node.
#[derive(Debug, Clone, clap::Args)]
pub struct GetAppNodesArgs {
    /// Id of the application/csi node.
    node_id: Option<NodeId>,
}

// CreateRows being trait for Node would create the rows from the list of
// Nodes returned from REST call.
impl CreateRow for openapi::models::AppNode {
    fn row(&self) -> Row {
        let labels = self.spec.labels.as_ref().map(|l| {
            l.iter()
                .map(|(key, value)| format!("{key}={value}"))
                .join(",")
        });
        let state = self.state.as_ref();
        row![
            self.id,
            state.map(|s| &s.endpoint).unwrap_or(&self.spec.endpoint),
            state.map(|s| s.status).unwrap_or_default(),
            optional_cell(labels),
        ]
    }
}

impl GetHeaderRow for openapi::models::AppNode {
    fn get_header_row(&self) -> Row {
        utils::APP_NODE_HEADERS.clone()
    }
}

#[async_trait(?Send)]
impl ListWithArgs for AppNodes {
    type Args = GetAppNodesArgs;
    async fn list(args: &GetAppNodesArgs, output: &utils::OutputFormat) -> PluginResult {
        if let Some(node_id) = &args.node_id {
            return AppNode::get(node_id, output).await;
        }

        let max_entries = 1024;
        let mut app_nodes = Vec::with_capacity(max_entries as usize);
        let client = RestClient::client().app_nodes_api();
        let mut starting_token = Some(0);
        while starting_token.is_some() {
            match client.get_app_nodes(max_entries, starting_token).await {
                Ok(nodes) => {
                    let nodes = nodes.into_body();
                    app_nodes.extend(nodes.entries);
                    starting_token = nodes.next_token;
                }
                Err(source) => {
                    return Err(Error::ListNodesError { source });
                }
            }
        }

        utils::print_table(output, app_nodes);
        Ok(())
    }
}

/// Node resource.
#[derive(clap::Args, Debug)]
pub struct AppNode {}

#[async_trait(?Send)]
impl Get for AppNode {
    type ID = NodeId;
    async fn get(id: &Self::ID, output: &utils::OutputFormat) -> PluginResult {
        match RestClient::client().app_nodes_api().get_app_node(id).await {
            Ok(node) => {
                utils::print_table(output, node.into_body());
                Ok(())
            }
            Err(source) => Err(Error::GetNodeError {
                id: id.to_owned(),
                source,
            }),
        }
    }
}
