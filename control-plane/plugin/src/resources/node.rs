use crate::{
    operations::{Cordoning, Drain, Get, List},
    resources::{
        utils,
        utils::{print_table, CreateRows, GetHeaderRow, OutputFormat},
        NodeId,
    },
    rest_wrapper::RestClient,
};
use async_trait::async_trait;
use openapi::models::CordonDrainState;
use prettytable::{Cell, Row};
use serde::Serialize;
use std::time;
use tokio::time::Duration;

#[derive(Debug, Clone, clap::Args)]
/// Arguments used when getting a node.
pub struct GetNodeArgs {
    /// Id of the node
    node_id: NodeId,
}

impl GetNodeArgs {
    /// Return the node ID.
    pub fn node_id(&self) -> NodeId {
        self.node_id.clone()
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
            node_nqn: spec.node_nqn,
        });
        let rows = vec![row![
            self.id,
            state.grpc_endpoint,
            state.status,
            spec.cordondrainstate.is_some(),
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
                utils::print_table(output, node.into_body());
            }
            Err(e) => {
                println!("Failed to get node {}. Error {}", id, e)
            }
        }
    }
}

/// Get the cordon labels from whichever state.
fn cordon_labels_from_state(ds: &CordonDrainState) -> Vec<String> {
    match ds {
        CordonDrainState::cordonedstate(state) => state.cordonlabels.clone(),
        CordonDrainState::drainingstate(state) => state.cordonlabels.clone(),
        CordonDrainState::drainedstate(state) => state.cordonlabels.clone(),
    }
}

fn drain_labels_from_state(ds: &CordonDrainState) -> Vec<String> {
    match ds {
        CordonDrainState::cordonedstate(_) => Vec::<String>::new(),
        CordonDrainState::drainingstate(state) => state.drainlabels.clone(),
        CordonDrainState::drainedstate(state) => state.drainlabels.clone(),
    }
}

#[async_trait(?Send)]
impl Cordoning for Node {
    type ID = NodeId;
    async fn cordon(id: &Self::ID, label: &str, output: &OutputFormat) {
        // is node already cordoned with the label?
        let already_has_cordon_label: bool =
            match RestClient::client().nodes_api().get_node(id).await {
                Ok(node) => {
                    let node_body = &node.into_body();
                    match &node_body.spec {
                        Some(spec) => match &spec.cordondrainstate {
                            Some(ds) => cordon_labels_from_state(ds).contains(&label.to_string()),
                            None => false,
                        },
                        None => {
                            println!("Node {} is not registered", id);
                            return;
                        }
                    }
                }
                Err(e) => {
                    println!("Failed to get node {}. Error {}", id, e);
                    return;
                }
            };
        let result = match already_has_cordon_label {
            false => {
                RestClient::client()
                    .nodes_api()
                    .put_node_cordon(id, label)
                    .await
            }
            true => RestClient::client().nodes_api().get_node(id).await,
        };
        match result {
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
                    let mut cordon_labels: Vec<String> = vec![];
                    let mut drain_labels: Vec<String> = vec![];
                    match node.into_body().spec {
                        Some(spec) => match spec.cordondrainstate {
                            Some(cds) => {
                                cordon_labels = cordon_labels_from_state(&cds);
                                drain_labels = drain_labels_from_state(&cds);
                            }
                            None => {}
                        },
                        None => println!("Error: Node {} has no spec", id), /* shouldn't happen */
                    }
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
}

/// Display format options for a `Node` object.
#[derive(Debug)]
pub enum NodeDisplayFormat {
    Default,
    CordonLabels,
    Drain,
}

/// The NodeDisply structure is responsible for controlling the display formatting of Node objects.
/// `#[serde(flatten)]` and `#[serde(skip)]` attributes are used to ensure that when the object is
/// serialised, only the `inner` object is represented.
#[derive(Serialize, Debug)]
pub struct NodeDisplay {
    #[serde(flatten)]
    pub inner: Vec<openapi::models::Node>,
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
    pub(crate) fn get_label_list(node: &openapi::models::Node) -> Vec<String> {
        let mut cordon_labels: Vec<String> = vec![];
        let mut drain_labels: Vec<String> = vec![];

        match &node.spec {
            Some(ns) => match &ns.cordondrainstate {
                Some(ds) => {
                    cordon_labels = cordon_labels_from_state(ds);
                    drain_labels = drain_labels_from_state(ds);
                }
                None => {}
            },
            None => {}
        }
        [cordon_labels, drain_labels].concat()
    }
    pub(crate) fn get_drain_label_list(node: &openapi::models::Node) -> Vec<String> {
        let mut drain_labels: Vec<String> = vec![];
        match &node.spec {
            Some(ns) => match &ns.cordondrainstate {
                Some(ds) => {
                    drain_labels = drain_labels_from_state(ds);
                }
                None => {}
            },
            None => {}
        }
        drain_labels
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
                    let labelstring = NodeDisplay::get_label_list(node).join(", ");
                    // Add the cordon labels to each row.
                    row[0].add_cell(Cell::new(&labelstring));
                    rows.push(row[0].clone());
                }
                rows
            }
            NodeDisplayFormat::Drain => {
                let mut rows = vec![];
                for node in self.inner.iter() {
                    let mut row = node.create_rows();

                    let drain_status_string = match &node.spec.as_ref().unwrap().cordondrainstate {
                        Some(ds) => match ds {
                            CordonDrainState::cordonedstate(_) => "Not draining",
                            CordonDrainState::drainingstate(_) => "Draining",
                            CordonDrainState::drainedstate(_) => "Drained",
                        },
                        None => "Not draining",
                    };

                    let labelstring = NodeDisplay::get_drain_label_list(node).join(", ");
                    // Add the drain labels to each row.
                    row[0].add_cell(Cell::new(drain_status_string));
                    row[0].add_cell(Cell::new(&labelstring));
                    rows.push(row[0].clone());
                }
                rows
            }
        }
    }
}

/// Print the given vector of nodes in the specified output format.
pub(crate) fn node_display_print(
    nodes: Vec<openapi::models::Node>,
    output: &OutputFormat,
    format: NodeDisplayFormat,
) {
    let node_display = NodeDisplay::new_nodes(nodes, format);
    match output {
        OutputFormat::Yaml | OutputFormat::Json => {
            print_table(output, node_display.inner);
        }
        OutputFormat::None => {
            print_table(output, node_display);
        }
    }
}

/// Print the given node in the specified output format.
pub(crate) fn node_display_print_one(
    nodes: openapi::models::Node,
    output: &OutputFormat,
    format: NodeDisplayFormat,
) {
    let node_display = NodeDisplay::new(nodes, format);
    match output {
        OutputFormat::Yaml | OutputFormat::Json => {
            print_table(output, node_display.inner);
        }
        OutputFormat::None => {
            print_table(output, node_display);
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
    /// Id of the node.
    node_id: NodeId,
    /// Label of the drain.
    label: String,
    #[clap(long)]
    /// Timeout for the drain operation.
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
    /// Return the timeout for the drain operation.
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
            timeout_instant = time::Instant::now().checked_add(dt.into());
        }
        let already_has_drain_label: bool =
            match RestClient::client().nodes_api().get_node(id).await {
                Ok(node) => {
                    let node_body = &node.into_body();
                    match &node_body.spec {
                        Some(spec) => match &spec.cordondrainstate {
                            Some(ds) => drain_labels_from_state(ds).contains(&label),
                            None => false,
                        },
                        None => {
                            println!("Node {} is not registered", id);
                            return;
                        }
                    }
                }
                Err(e) => {
                    println!("Failed to get node {}. Error {}", id, e);
                    return;
                }
            };
        if !already_has_drain_label {
            if let Err(error) = RestClient::client()
                .nodes_api()
                .put_node_drain(id, &label)
                .await
            {
                println!("Failed to put node drain {}. Error {}", id, error);
                return;
            }
        }
        // loop this call until no longer draining
        loop {
            match RestClient::client().nodes_api().get_node(id).await {
                Ok(node) => {
                    let node_body = &node.clone().into_body();
                    match &node_body.spec {
                        Some(spec) => {
                            match &spec.cordondrainstate {
                                Some(ds) => match ds {
                                    CordonDrainState::cordonedstate(_) => {
                                        match output {
                                            OutputFormat::None => {
                                                println!("Node {} drain has been cancelled", id);
                                            }
                                            _ => {
                                                // json or yaml
                                                print_table(output, node.into_body());
                                            }
                                        }
                                        break;
                                    }
                                    CordonDrainState::drainingstate(_) => {}
                                    CordonDrainState::drainedstate(_) => {
                                        match output {
                                            OutputFormat::None => {
                                                println!("Node {} successfully drained", id);
                                            }
                                            _ => {
                                                // json or yaml
                                                print_table(output, node.into_body());
                                            }
                                        }
                                        break;
                                    }
                                },
                                None => {
                                    match output {
                                        OutputFormat::None => {
                                            println!("Node {} drain has been cancelled", id);
                                        }
                                        _ => {
                                            // json or yaml
                                            print_table(output, node.into_body());
                                        }
                                    }
                                    break;
                                }
                            }
                        }
                        None => {
                            println!("Node {} is not registered", id);
                            break;
                        }
                    }
                }
                Err(e) => {
                    println!("Failed to get node {}. Error {}", id, e);
                    break;
                }
            }
            if timeout_instant.is_some() && time::Instant::now() > timeout_instant.unwrap() {
                println!("Node {} drain command timed out", id);
                break;
            }
            let sleep = Duration::from_secs(2);
            tokio::time::sleep(sleep).await;
        }
    }
}
