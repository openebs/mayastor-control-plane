use crate::{
    operations::{Cordoning, Drain, DrainList, Get, List},
    resources::{
        utils,
        utils::{print_table, CreateRows, GetHeaderRow, OutputFormat},
        NodeId,
    },
    rest_wrapper::RestClient,
};
use async_trait::async_trait;
use openapi::models::NodeSpec;
use prettytable::{Cell, Row};
use serde::Serialize;
use std::time;
use tokio::time::Duration;

#[derive(Debug, Clone, clap::Args)]
/// Arguments used when getting a node.
pub struct GetNodeArgs {
    /// Id of the node
    node_id: NodeId,
    #[clap(long)]
    /// Shows the cordon labels associated with the node
    show_cordon_labels: bool,
    #[clap(long)]
    /// Shows the drain information and filter for draining / drained nodes
    show_drain: bool,
}

impl GetNodeArgs {
    /// Return the node ID.
    pub fn node_id(&self) -> NodeId {
        self.node_id.clone()
    }

    /// Return whether or not we should show the cordon labels.
    pub fn show_cordon_labels(&self) -> bool {
        self.show_cordon_labels
    }

    /// Return whether or not we should show the drain labels.
    pub fn show_drain(&self) -> bool {
        self.show_drain
    }
}

#[derive(Debug, Clone, clap::Args)]
/// Arguments used when getting a node.
pub struct GetNodesArgs {
    #[clap(long)]
    /// Shows the drain information and filter for draining / drained nodes
    show_drain: bool,
}

impl GetNodesArgs {
    /// Return whether or not we should show the drain labels and filter based on drain labels
    pub fn show_drain(&self) -> bool {
        self.show_drain
    }
}

/// Nodes resource.
#[derive(clap::Args, Debug)]
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
            drain_status: openapi::models::DrainStatus::NotDraining,
        });
        let rows = vec![row![
            self.id,
            state.grpc_endpoint,
            state.status,
            !(spec.cordon_labels.is_empty() && spec.drain_labels.is_empty()),
        ]];
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
#[derive(clap::Args, Debug)]
pub struct Node {}

#[async_trait(?Send)]
impl Get for Node {
    type ID = NodeId;
    async fn get(id: &Self::ID, output: &utils::OutputFormat) {
        match RestClient::client().nodes_api().get_node(id).await {
            Ok(node) => {
                // Print table, json or yaml based on output format.
                let node_display = NodeDisplay::new(node.into_body(), NodeDisplayFormat::Default);
                print_table(output, node_display);
            }
            Err(e) => {
                println!("Failed to get node {}. Error {}", id, e)
            }
        }
    }
}

#[async_trait(?Send)]
impl Cordoning for Node {
    type ID = NodeId;
    async fn cordon(id: &Self::ID, label: &str, output: &OutputFormat) {
        match RestClient::client()
            .nodes_api()
            .put_node_cordon(id, label)
            .await
        {
            Ok(node) => match output {
                OutputFormat::Yaml | OutputFormat::Json => {
                    // Print json or yaml based on output format.
                    utils::print_table(output, node.into_body());
                }
                OutputFormat::None => {
                    // In case the output format is not specified, show a success message.
                    println!("Node {} cordoned successfully", id)
                }
            },
            Err(e) => {
                println!("Failed to cordon node {}. Error {}", id, e)
            }
        }
    }

    async fn uncordon(id: &Self::ID, label: &str, output: &OutputFormat) {
        match RestClient::client()
            .nodes_api()
            .delete_node_cordon(id, label)
            .await
        {
            Ok(node) => match output {
                OutputFormat::Yaml | OutputFormat::Json => {
                    // Print json or yaml based on output format.
                    utils::print_table(output, node.into_body());
                }
                OutputFormat::None => {
                    // In case the output format is not specified, show a success message.
                    let cordon_labels = node
                        .clone()
                        .into_body()
                        .spec
                        .map(|node_spec| node_spec.cordon_labels)
                        .unwrap_or_default();

                    let drain_labels = node
                        .into_body()
                        .spec
                        .map(|node_spec| node_spec.drain_labels)
                        .unwrap_or_default();
                    let labels = [cordon_labels, drain_labels].concat();
                    if labels.is_empty() {
                        println!("Node {} successfully uncordoned", id);
                    } else {
                        println!(
                            "Cordon label successfully removed. Remaining cordon labels {:?}",
                            labels,
                        );
                    }
                }
            },
            Err(e) => {
                println!("Failed to uncordon node {}. Error {}", id, e)
            }
        }
    }

    async fn get_node_with_cordon_labels(id: &Self::ID, output: &OutputFormat) {
        match RestClient::client().nodes_api().get_node(id).await {
            Ok(node) => {
                let node_display =
                    NodeDisplay::new(node.into_body(), NodeDisplayFormat::CordonLabels);
                print_table(output, node_display);
            }
            Err(e) => {
                println!("Failed to get node {}. Error {}", id, e)
            }
        }
    }
}

/// Display format options for a `Node` object.
#[derive(Debug)]
enum NodeDisplayFormat {
    Default,
    CordonLabels,
    Drain,
}

/// The NodeDisply structure is responsible for controlling the display formatting of Node objects.
/// `#[serde(flatten)]` and `#[serde(skip)]` attributes are used to ensure that when the object is
/// serialised, only the `inner` object is represented.
#[derive(Serialize, Debug)]
struct NodeDisplay {
    #[serde(flatten)]
    inner: Vec<openapi::models::Node>,
    #[serde(skip)]
    format: NodeDisplayFormat,
}

impl NodeDisplay {
    /// Create a new `NodeDisplay` instance.
    pub(crate) fn new(node: openapi::models::Node, format: NodeDisplayFormat) -> Self {
        let vec: Vec<openapi::models::Node> = vec![node];
        Self { inner: vec, format }
    }
    pub(crate) fn new_nodes(nodes: Vec<openapi::models::Node>, format: NodeDisplayFormat) -> Self {
        Self {
            inner: nodes,
            format,
        }
    }
}

// Create the rows required for a `NodeDisplay` object. Nodes returned from REST call.
impl CreateRows for NodeDisplay {
    fn create_rows(&self) -> Vec<Row> {
        match self.format {
            NodeDisplayFormat::Default => self.inner.create_rows(),
            NodeDisplayFormat::CordonLabels => {
                let mut rows = vec![];
                for node in self.inner.iter() {
                    let mut row = node.create_rows();
                    let mut cordon_labels_string = node
                        .spec
                        .as_ref()
                        .unwrap_or(&NodeSpec::default())
                        .cordon_labels
                        .join(", ");
                    let drain_labels_string = node
                        .spec
                        .as_ref()
                        .unwrap_or(&NodeSpec::default())
                        .drain_labels
                        .join(", ");
                    if !cordon_labels_string.is_empty() && !drain_labels_string.is_empty() {
                        cordon_labels_string += ", ";
                    }
                    cordon_labels_string += &drain_labels_string;
                    // Add the cordon labels to each row.
                    row[0].add_cell(Cell::new(&cordon_labels_string));
                    rows.push(row[0].clone());
                }
                rows
            }
            NodeDisplayFormat::Drain => {
                let mut rows = vec![];
                for node in self.inner.iter() {
                    let mut row = node.create_rows();
                    let drain_status_string = match node.state.as_ref().unwrap().drain_status {
                        openapi::models::DrainStatus::NotDraining => "Not Draining",
                        openapi::models::DrainStatus::Draining => "Draining",
                        openapi::models::DrainStatus::Drained => "Drained",
                    };

                    let drain_labels_string = node
                        .spec
                        .as_ref()
                        .unwrap_or(&NodeSpec::default())
                        .drain_labels
                        .join(", ");
                    // Add the drain labels to each row.
                    row[0].add_cell(Cell::new(drain_status_string));
                    row[0].add_cell(Cell::new(&drain_labels_string));
                    rows.push(row[0].clone());
                }
                rows
            }
        }
    }
}

// Create the header for a `NodeDisplay` object.
impl GetHeaderRow for NodeDisplay {
    fn get_header_row(&self) -> Row {
        let mut header = (&*utils::NODE_HEADERS).clone();
        match self.format {
            NodeDisplayFormat::Default => header,
            NodeDisplayFormat::CordonLabels => {
                header.extend(vec!["CORDON LABELS"]);
                header
            }
            NodeDisplayFormat::Drain => {
                header.extend(vec!["DRAIN STATE"]);
                header.extend(vec!["DRAIN LABELS"]);
                header
            }
        }
    }
}

#[derive(Debug, Clone, clap::Args)]
pub struct DrainNodeArgs {
    /// Id of the node
    node_id: NodeId,
    /// Label of the drain
    label: String,
    #[clap(long)]
    /// Timeout for the drain operation
    drain_timeout: Option<humantime::Duration>,
}

impl DrainNodeArgs {
    /// Return the node ID.
    pub fn node_id(&self) -> NodeId {
        self.node_id.clone()
    }
    /// Return the drain label.
    pub fn label(&self) -> String {
        self.label.clone()
    }
    /// Return whether or not we should show the drain labels.
    pub fn drain_timeout(&self) -> Option<humantime::Duration> {
        self.drain_timeout
    }
}

#[async_trait(?Send)]
impl Drain for Node {
    type ID = NodeId;
    async fn drain(
        id: &Self::ID,
        label: String,
        drain_timeout: Option<humantime::Duration>,
        output: &utils::OutputFormat,
    ) {
        let mut timeout_instant: Option<time::Instant> = None;
        if let Some(dt) = drain_timeout {
            //let duration: std::time::Duration = drain_timeout.unwrap().into();
            timeout_instant = time::Instant::now().checked_add(dt.into());
        }
        match RestClient::client()
            .nodes_api()
            .put_node_drain(id, &label)
            .await
        {
            Ok(_node) => {
                // loop this call until no longer draining
                loop {
                    match RestClient::client().nodes_api().get_node(id).await {
                        Ok(node) => {
                            let node_body = &node.clone().into_body();
                            let state = node_body.state.as_ref().unwrap().drain_status;
                            match state {
                                openapi::models::DrainStatus::NotDraining
                                | openapi::models::DrainStatus::Drained => {
                                    match output {
                                        OutputFormat::None => match state {
                                            openapi::models::DrainStatus::NotDraining => {
                                                println!("Drain has been cancelled");
                                            }
                                            openapi::models::DrainStatus::Drained => {
                                                println!("Drain completed");
                                            }
                                            _ => {} // not possible
                                        },
                                        _ => {
                                            // json or yaml
                                            print_table(output, node.into_body());
                                        }
                                    }
                                    break;
                                }
                                openapi::models::DrainStatus::Draining => {}
                            }
                        }
                        Err(e) => {
                            println!("Failed to get node {}. Error {}", id, e);
                            break;
                        }
                    }
                    if timeout_instant.is_some() && time::Instant::now() > timeout_instant.unwrap()
                    {
                        println!("Drain command timed out");
                        break;
                    }
                    let sleep = Duration::from_secs(1);
                    tokio::time::sleep(sleep).await;
                }
            }
            Err(e) => {
                println!("Failed to get node {}. Error {}", id, e)
            }
        }
    }

    async fn get_node_with_drain_info(id: &Self::ID, output: &OutputFormat) {
        match RestClient::client().nodes_api().get_node(id).await {
            Ok(node) => {
                let node_display = NodeDisplay::new(node.into_body(), NodeDisplayFormat::Drain);
                print_table(output, node_display);
            }
            Err(e) => {
                println!("Failed to get node {}. Error {}", id, e)
            }
        }
    }
}

#[async_trait(?Send)]
impl DrainList for Nodes {
    async fn list_nodes_with_drain_info(output: &utils::OutputFormat) {
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
                let node_display = NodeDisplay::new_nodes(filteredlist, NodeDisplayFormat::Drain);
                utils::print_table(output, node_display);
            }
            Err(e) => {
                println!("Failed to list nodes. Error {}", e)
            }
        }
    }
}
