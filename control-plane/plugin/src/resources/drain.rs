pub struct NodeDrain {}
pub struct NodeDrains {}
pub struct PoolDrain {}

use async_trait::async_trait;
use openapi::models::{CordonDrainState, PoolDrainRecordExt, PoolDrainUsage};
use prettytable::Row;
use serde::Serialize;

use crate::{
    operations::{Get, List, PluginResult},
    resources::{
        error::Error,
        node::{node_display_print, node_display_print_one, NodeDisplayFormat},
        utils::{optional_cell, print_table, CreateRows, GetHeaderRow, OutputFormat},
        NodeId, PoolId,
    },
    rest_wrapper::RestClient,
};

#[async_trait(?Send)]
impl Get for NodeDrain {
    type ID = NodeId;
    async fn get(id: &Self::ID, output: &OutputFormat) -> PluginResult {
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

/// Pool drain progress, displayed as a single row of columns.
/// The full record, including the moving replicas, is available in the json/yaml output.
#[derive(Serialize)]
struct PoolDrainDisplay {
    #[serde(skip)]
    id: PoolId,
    #[serde(flatten)]
    record: PoolDrainRecordExt,
}

impl GetHeaderRow for PoolDrainDisplay {
    fn get_header_row(&self) -> Row {
        row![
            "POOL",
            "PHASE",
            "REASON",
            "REPLICAS (INITIAL/CURRENT)",
            "SNAPSHOTS (INITIAL/CURRENT)",
            "USED (INITIAL/CURRENT)"
        ]
    }
}

impl CreateRows for PoolDrainDisplay {
    fn create_rows(&self) -> Vec<Row> {
        let initial = self.record.initial.as_ref();
        let current = self.record.current.as_ref();
        let pair = |value: fn(&PoolDrainUsage) -> String| {
            format!(
                "{}/{}",
                optional_cell(initial.map(value)),
                optional_cell(current.map(value))
            )
        };
        vec![row![
            self.id,
            self.record.phase,
            optional_cell(self.record.reason),
            pair(|u| u.replica_count.to_string()),
            pair(|u| u.snapshot_count.to_string()),
            pair(|u| ::utils::bytes::into_human(u.used)),
        ]]
    }
}

#[async_trait(?Send)]
impl Get for PoolDrain {
    type ID = PoolId;
    async fn get(id: &Self::ID, output: &OutputFormat) -> PluginResult {
        let record = RestClient::client()
            .pools_api()
            .get_pool_drain(id)
            .await
            .map_err(|source| Error::GetPoolDrainError {
                id: id.to_string(),
                source,
            })?
            .into_body();
        print_table(
            output,
            PoolDrainDisplay {
                id: id.clone(),
                record,
            },
        );
        Ok(())
    }
}
