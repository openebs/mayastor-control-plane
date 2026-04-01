use crate::{
    operations::{Cordoning, Delete, Drain, GetWithArgs, Label, ListWithArgs, PluginResult},
    resources::{
        error::{Error, LabelAssignSnafu, OpError, TopologyError},
        utils::{
            self, optional_cell, print_table, validate_topology_key, validate_topology_value,
            CreateRow, CreateRows, GetHeaderRow, OutputFormat,
        },
        NodeId,
    },
    rest_wrapper::RestClient,
};
use async_trait::async_trait;
use openapi::{apis::StatusCode, models, models::CordonDrainState};
use prettytable::{Cell, Row};
use serde::Serialize;
use snafu::ResultExt;
use std::time;
use strum_macros::{AsRefStr, Display, EnumString};
use tokio::time::Duration;

#[derive(AsRefStr, EnumString, Display)]
enum NodeCordonDrainState {
    Cordoned,
    Draining,
    Drained,
}

#[derive(Debug, Clone, clap::Args)]
/// Arguments used when getting a node.
pub struct GetNodeArgs {
    /// Id of the node
    node_id: NodeId,
    /// Show the labels of the node
    #[clap(long, default_value = "false")]
    show_labels: bool,
}

impl GetNodeArgs {
    /// Return the node ID.
    pub fn node_id(&self) -> NodeId {
        self.node_id.clone()
    }
    /// Return whether to show the labels of the node.
    pub fn show_labels(&self) -> bool {
        self.show_labels
    }
}

#[derive(Debug, Clone, clap::Args)]
/// Arguments used when getting nodes.
pub struct GetNodesArgs {
    /// Show the labels of the nodes
    #[clap(long, default_value = "false")]
    show_labels: bool,
}

impl GetNodesArgs {
    /// Return whether to show the labels of the nodes.
    pub fn show_labels(&self) -> bool {
        self.show_labels
    }
}

/// Nodes resource.
#[derive(clap::Args, Debug)]
pub struct Nodes {}

// CreateRows being trait for Node would create the rows from the list of
// Nodes returned from REST call.
impl CreateRow for openapi::models::Node {
    fn row(&self) -> Row {
        let spec = self.spec.clone().unwrap_or_default();
        // In case the state is not coming as filled, either due to node offline, fill in
        // spec data and mark the status as Unknown.
        let state = self.state.clone().unwrap_or(openapi::models::NodeState {
            id: spec.id,
            grpc_endpoint: spec.grpc_endpoint,
            status: self.status.unwrap_or_default(),
            node_nqn: spec.node_nqn,
            version: spec.version,
        });
        let statuses = match spec.cordondrainstate {
            None => format!("{:?}", state.status),
            Some(CordonDrainState::cordonedstate(_)) => {
                format!("{:?}, {}", state.status, NodeCordonDrainState::Cordoned)
            }
            Some(CordonDrainState::drainingstate(_)) => {
                format!(
                    "{:?}, {}, {}",
                    state.status,
                    NodeCordonDrainState::Cordoned,
                    NodeCordonDrainState::Draining
                )
            }
            Some(CordonDrainState::drainedstate(_)) => {
                format!(
                    "{:?}, {}, {}",
                    state.status,
                    NodeCordonDrainState::Cordoned,
                    NodeCordonDrainState::Drained
                )
            }
        };
        row![
            self.id,
            state.grpc_endpoint,
            statuses,
            optional_cell(state.version)
        ]
    }
}

// GetHeaderRow being trait for Node would return the Header Row for
// Node.
impl GetHeaderRow for openapi::models::Node {
    fn get_header_row(&self) -> Row {
        (*utils::NODE_HEADERS).clone()
    }
}

#[async_trait(?Send)]
impl ListWithArgs for Nodes {
    type Args = GetNodesArgs;
    async fn list(args: &Self::Args, output: &utils::OutputFormat) -> PluginResult {
        match RestClient::client().nodes_api().get_nodes(None).await {
            Ok(nodes) => {
                let node_display =
                    NodeDisplayLabels::new_nodes(nodes.clone().into_body(), args.show_labels());
                match output {
                    OutputFormat::Yaml | OutputFormat::Json => {
                        print_table(output, node_display.inner);
                    }
                    OutputFormat::None => {
                        print_table(output, node_display);
                    }
                }
            }
            Err(e) => {
                return Err(Error::ListNodesError { source: e });
            }
        }
        Ok(())
    }
}

/// The NodeDisplayLabels structure is responsible for controlling the display formatting of Node
/// objects. `#[serde(flatten)]` and `#[serde(skip)]` attributes are used to ensure that when the
/// object is serialised, only the `inner` object is represented.
#[derive(Serialize, Debug)]
pub struct NodeDisplayLabels {
    #[serde(flatten)]
    pub inner: Vec<openapi::models::Node>,
    #[serde(skip)]
    show_labels: bool,
}

impl NodeDisplayLabels {
    /// Create a new `NodeDisplayLabels` instance.
    pub(crate) fn new(node: openapi::models::Node, show_labels: bool) -> Self {
        let vec: Vec<openapi::models::Node> = vec![node];
        Self {
            inner: vec,
            show_labels,
        }
    }
    /// Create a new `NodeDisplay` instance from a vector of nodes.
    pub(crate) fn new_nodes(nodes: Vec<openapi::models::Node>, show_labels: bool) -> Self {
        Self {
            inner: nodes,
            show_labels,
        }
    }

    /// Get a list of node labels.
    pub(crate) fn get_node_label_list(node: &openapi::models::Node) -> Vec<String> {
        let mut node_labels: Vec<String> = vec![];

        if let Some(ns) = &node.spec {
            if let Some(ds) = &ns.labels {
                node_labels = ds
                    .iter()
                    .map(|(key, value)| format!("{key}={value}"))
                    .collect();
            }
        }
        node_labels.sort_unstable();
        node_labels
    }
}

// Create the header for a `NodeDisplayLabels` object.
impl GetHeaderRow for NodeDisplayLabels {
    fn get_header_row(&self) -> Row {
        let mut header = (*utils::NODE_HEADERS).clone();
        if self.show_labels {
            header.extend(vec!["LABELS"]);
        }
        header
    }
}

impl CreateRows for NodeDisplayLabels {
    fn create_rows(&self) -> Vec<Row> {
        let mut rows = vec![];
        for node in self.inner.iter() {
            let mut row = node.create_rows();
            if self.show_labels {
                let labelstring = NodeDisplayLabels::get_node_label_list(node).join(", ");
                // Add the node labels to each row.
                row[0].add_cell(Cell::new(&labelstring));
            }
            rows.push(row[0].clone());
        }
        rows
    }
}

/// Node resource.
#[derive(clap::Args, Debug)]
pub struct Node {}

#[async_trait(?Send)]
impl GetWithArgs for Node {
    type ID = NodeId;
    type Args = GetNodeArgs;
    async fn get(id: &Self::ID, args: &Self::Args, output: &utils::OutputFormat) -> PluginResult {
        match RestClient::client().nodes_api().get_node(id).await {
            Ok(node) => match output {
                OutputFormat::Yaml | OutputFormat::Json => {
                    print_table(output, node.clone().into_body());
                }
                OutputFormat::None => {
                    print_table(
                        output,
                        NodeDisplayLabels::new(node.into_body(), args.show_labels()),
                    );
                }
            },
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

/// Get the cordon labels from whichever state.
pub(super) fn cordon_labels_from_state(ds: &CordonDrainState) -> Vec<String> {
    match ds {
        CordonDrainState::cordonedstate(state) => state.cordonlabels.clone(),
        CordonDrainState::drainingstate(state) => state.cordonlabels.clone(),
        CordonDrainState::drainedstate(state) => state.cordonlabels.clone(),
    }
}

pub(super) fn drain_labels_from_state(ds: &CordonDrainState) -> Vec<String> {
    match ds {
        CordonDrainState::cordonedstate(_) => Vec::<String>::new(),
        CordonDrainState::drainingstate(state) => state.drainlabels.clone(),
        CordonDrainState::drainedstate(state) => state.drainlabels.clone(),
    }
}

#[async_trait(?Send)]
impl Cordoning for Node {
    type ID = NodeId;
    type CREQ = str;
    type UREQ = str;

    async fn cordon(id: &Self::ID, label: &str, output: &OutputFormat) -> PluginResult {
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
                            println!("Node {id} is not registered");
                            return Ok(());
                        }
                    }
                }
                Err(e) => {
                    return Err(Error::GetNodeError {
                        id: id.to_string(),
                        source: e,
                    });
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
                    println!("Node {id} cordoned successfully")
                }
            },
            Err(e) => {
                return Err(Error::NodeCordonError {
                    id: id.to_string(),
                    source: e,
                });
            }
        }
        Ok(())
    }

    async fn uncordon(id: &Self::ID, label: &str, output: &OutputFormat) -> PluginResult {
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
                        Some(spec) => {
                            if let Some(cds) = spec.cordondrainstate {
                                cordon_labels = cordon_labels_from_state(&cds);
                                drain_labels = drain_labels_from_state(&cds);
                            }
                        }
                        /* shouldn't happen */
                        None => {
                            println!("Error: Node {id} has no spec");
                        }
                    }
                    let labels = [cordon_labels, drain_labels].concat();
                    if labels.is_empty() {
                        println!("Node {id} successfully uncordoned");
                    } else {
                        println!(
                            "Cordon label successfully removed. Remaining cordon labels {labels:?}",
                        );
                    }
                }
            },
            Err(e) => {
                return Err(Error::NodeUncordonError {
                    id: id.to_string(),
                    source: e,
                });
            }
        }
        Ok(())
    }
}

/// Display format options for a `Node` object.
#[derive(Debug)]
pub enum NodeDisplayFormat {
    Default,
    CordonLabels,
    Drain,
}

/// The NodeDisplay structure is responsible for controlling the display formatting of Node objects.
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
    /// Create a new `NodeDisplay` instance from a vector of nodes.
    pub(crate) fn new_nodes(nodes: Vec<openapi::models::Node>, format: NodeDisplayFormat) -> Self {
        Self {
            inner: nodes,
            format,
        }
    }
    /// Get a list of node labels.
    pub(crate) fn get_label_list(node: &openapi::models::Node) -> Vec<String> {
        let mut cordon_labels: Vec<String> = vec![];
        let mut drain_labels: Vec<String> = vec![];

        if let Some(ns) = &node.spec {
            if let Some(ds) = &ns.cordondrainstate {
                cordon_labels = cordon_labels_from_state(ds);
                drain_labels = drain_labels_from_state(ds);
            }
        }
        [cordon_labels, drain_labels].concat()
    }
    /// Get a list of node drain labels.
    pub(crate) fn get_drain_label_list(node: &openapi::models::Node) -> Vec<String> {
        let mut drain_labels: Vec<String> = vec![];
        if let Some(ns) = &node.spec {
            if let Some(ds) = &ns.cordondrainstate {
                drain_labels = drain_labels_from_state(ds);
            }
        }
        drain_labels
    }
}

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
                let mut rows = Vec::with_capacity(self.inner.len());
                for node in self.inner.iter() {
                    let mut row = node.row();

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
                    row.add_cell(Cell::new(drain_status_string));
                    row.add_cell(Cell::new(&labelstring));
                    rows.push(row);
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
        let mut header = (*utils::NODE_HEADERS).clone();
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
    ) -> PluginResult {
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
                            println!("Node {id} is not registered");
                            return Ok(());
                        }
                    }
                }
                Err(e) => {
                    return Err(Error::GetNodeError {
                        id: id.to_string(),
                        source: e,
                    });
                }
            };
        if !already_has_drain_label {
            if let Err(error) = RestClient::client()
                .nodes_api()
                .put_node_drain(id, &label)
                .await
            {
                return Err(Error::PutNodeDrainError {
                    id: id.to_string(),
                    source: error,
                });
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
                                                println!("Node {id} drain has been cancelled");
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
                                                println!("Node {id} successfully drained");
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
                                            println!("Node {id} drain has been cancelled");
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
                            println!("Node {id} is not registered");
                            break;
                        }
                    }
                }
                Err(e) => {
                    return Err(Error::GetNodeError {
                        id: id.to_string(),
                        source: e,
                    });
                }
            }
            if timeout_instant.is_some() && time::Instant::now() > timeout_instant.unwrap() {
                println!("Node {id} drain command timed out");
                break;
            }
            let sleep = Duration::from_secs(2);
            tokio::time::sleep(sleep).await;
        }
        Ok(())
    }
}

#[async_trait(?Send)]
impl Label for Node {
    type ID = NodeId;
    async fn label(
        id: &Self::ID,
        label: String,
        overwrite: bool,
        output: &utils::OutputFormat,
    ) -> PluginResult {
        let result = if label.contains('=') {
            let [key, value] = label.split('=').collect::<Vec<_>>()[..] else {
                return Err(TopologyError::LabelMultiAssign {}.into());
            };

            validate_topology_key(key).context(super::error::NodeLabelFormatSnafu)?;
            validate_topology_value(value).context(super::error::NodeLabelFormatSnafu)?;
            match RestClient::client()
                .nodes_api()
                .put_node_label(id, key, value, Some(overwrite))
                .await
            {
                Err(source) => match source.status() {
                    Some(StatusCode::UNPROCESSABLE_ENTITY) if output.none() => {
                        Err(OpError::LabelExists {
                            resource: "Node".to_string(),
                            id: id.to_string(),
                        })
                    }
                    Some(StatusCode::PRECONDITION_FAILED) if output.none() => {
                        Err(OpError::LabelConflict {
                            resource: "Node".to_string(),
                            id: id.to_string(),
                        })
                    }
                    Some(StatusCode::NOT_FOUND) if output.none() => {
                        Err(OpError::ResourceNotFound {
                            resource: "Node".to_string(),
                            id: id.to_string(),
                        })
                    }
                    _ => Err(OpError::Generic {
                        resource: "Node".to_string(),
                        id: id.to_string(),
                        source,
                    }),
                },
                Ok(node) => Ok(node),
            }
        } else {
            snafu::ensure!(label.len() >= 2 && label.ends_with('-'), LabelAssignSnafu);
            let key = &label[..label.len() - 1];
            validate_topology_key(key)?;
            match RestClient::client()
                .nodes_api()
                .delete_node_label(id, key)
                .await
            {
                Err(source) => match source.status() {
                    Some(StatusCode::PRECONDITION_FAILED) if output.none() => {
                        Err(OpError::LabelNotFound {
                            resource: "Node".to_string(),
                            id: id.to_string(),
                        })
                    }
                    Some(StatusCode::NOT_FOUND) if output.none() => {
                        Err(OpError::ResourceNotFound {
                            resource: "Node".to_string(),
                            id: id.to_string(),
                        })
                    }
                    _ => Err(OpError::Generic {
                        resource: "Node".to_string(),
                        id: id.to_string(),
                        source,
                    }),
                },
                Ok(node) => Ok(node),
            }
        }?;
        let node = result.into_body();
        match output {
            OutputFormat::Yaml | OutputFormat::Json => {
                // Print json or yaml based on output format.
                print_table(output, node);
            }
            OutputFormat::None => {
                // In case the output format is not specified, show a success message.
                let labels = node.spec.unwrap().labels.unwrap_or_default();
                println!("Node {id} labelled successfully. Current labels: {labels:?}");
            }
        }
        Ok(())
    }
}

/// Arguments for deleting a node.
#[derive(Debug, Clone, clap::Args)]
pub struct DeleteNodeArgs {
    /// Id of the node to delete.
    node_id: NodeId,

    /// Purge node and all its resources without contacting io-engine.{n}
    /// Use this when the node is permanently offline or decommissioned.
    /// Requires the node to be offline and cordoned.
    #[arg(long)]
    purge: bool,

    /// Show what would happen if this node is purged, without actually deleting.
    /// Displays node status, cordon state, per-pool replica counts, and affected volumes.
    #[arg(long)]
    pub show_impact: bool,

    /// Accept both volume loss and snapshot loss.
    /// Shorthand for --accept-volume-loss --accept-snapshot-loss.
    #[arg(long)]
    accept_data_loss: bool,

    /// Accept volume loss for volumes losing their last healthy replica.
    /// Required when --purge would cause volume data loss.
    #[arg(long)]
    accept_volume_loss: bool,

    /// Accept snapshot loss for snapshots losing their last replica snapshot.
    /// Required when --purge would cause snapshot loss.
    #[arg(long)]
    accept_snapshot_loss: bool,
}

#[async_trait(?Send)]
impl Delete for Node {
    type ID = DeleteNodeArgs;

    async fn del(id: &Self::ID, _ignore_not_found: bool, output: &OutputFormat) -> PluginResult {
        // If --show-impact is set, show purge impact analysis and return without deleting.
        if id.show_impact {
            return Node::show_impact_analysis(&id.node_id, output).await;
        }

        // --accept-data-loss is shorthand for both volume and snapshot loss.
        let accept_volume_loss = id.accept_volume_loss || id.accept_data_loss;
        let accept_snapshot_loss = id.accept_snapshot_loss || id.accept_data_loss;
        let body = openapi::models::DeleteNodeBody {
            purge: Some(id.purge),
            accept: Some(true),
            accept_volume_loss: Some(accept_volume_loss),
            accept_snapshot_loss: Some(accept_snapshot_loss),
        };

        match RestClient::client()
            .nodes_api()
            .del_node(&id.node_id, Some(body))
            .await
        {
            Ok(response) => {
                utils::print_table(output, response.into_body());
                Ok(())
            }
            Err(source) => {
                // Translate core agent errors into user-friendly messages with CLI flags.
                use openapi::models::rest_json_error::Kind;
                if let Some(kind) = source.error_body().map(|b| b.kind) {
                    let hint = match kind {
                        Kind::NodeIsOnline => {
                            Some("Node is online. Only offline nodes can be deleted.")
                        }
                        Kind::NodeNotCordoned => {
                            Some("Node must be cordoned first. Use: kubectl mayastor cordon node <id> <label>")
                        }
                        Kind::NodeHasResources => {
                            Some("Node has resources. Use --purge to force-remove the node and all its resources.")
                        }
                        Kind::NodePurgeAcceptRequired => {
                            Some("Node has pools with data. Confirm with --yes to proceed.")
                        }
                        Kind::NodePurgeVolumeLossAcceptRequired => {
                            Some("Volumes would lose their last healthy replica. Use --accept-volume-loss or --accept-data-loss to proceed.")
                        }
                        Kind::NodePurgeSnapshotLossAcceptRequired => {
                            Some("Snapshots would lose their last replica snapshot. Use --accept-snapshot-loss or --accept-data-loss to proceed.")
                        }
                        _ => None,
                    };
                    if let Some(hint) = hint {
                        eprintln!("{hint}");
                    }
                }
                Err(Error::DeleteNodeError {
                    id: id.node_id.clone(),
                    source,
                })
            }
        }
    }
}

impl GetHeaderRow for models::NodeDeleteResult {
    fn get_header_row(&self) -> Row {
        row!["NODE", "VOLUME_LOSS", "SNAPSHOT_LOSS"]
    }
}

impl CreateRow for models::NodeDeleteResult {
    fn row(&self) -> Row {
        let volume_loss = optional_cell(
            (!self.volume_loss.volumes.is_empty())
                .then(|| format!("{} volume(s)", self.volume_loss.volumes.len())),
        );
        let snapshot_loss = optional_cell(
            (!self.snapshot_loss.snapshots.is_empty())
                .then(|| format!("{} snapshot(s)", self.snapshot_loss.snapshots.len())),
        );

        row![self.node_id, volume_loss, snapshot_loss]
    }
}

/// Per-pool impact detail within a node purge.
#[derive(Serialize, Debug, Clone)]
pub struct PoolImpact {
    /// The pool being analyzed.
    pub pool_id: String,
    /// Current runtime status of the pool.
    pub status: models::PoolStatus,
    /// Number of replicas residing on this pool.
    pub replica_count: usize,
    /// Volumes affected by purging this pool.
    pub affected_volumes: Vec<super::VolumeId>,
}

/// Impact analysis for a node purge operation.
///
/// Shown when using `--show-impact` to preview what would happen before
/// actually purging the node and all its pools.
#[derive(Serialize, Debug, Clone)]
pub struct NodePurgeImpact {
    /// The node being analyzed.
    pub node_id: NodeId,
    /// Deemed status of the node (Online, Offline, Unknown).
    pub status: models::NodeStatus,
    /// Whether the node is cordoned.
    pub cordoned: bool,
    /// Per-pool impact breakdown.
    pub pools: Vec<PoolImpact>,
    /// Total number of replicas across all pools on this node.
    pub total_replicas: usize,
    /// Total unique volumes affected across all pools.
    pub total_affected_volumes: usize,
    /// Whether the node meets all preconditions for purge:
    /// node is offline and cordoned.
    pub ready_for_purge: bool,
}

impl GetHeaderRow for NodePurgeImpact {
    fn get_header_row(&self) -> Row {
        row![
            "NODE",
            "STATUS",
            "CORDONED",
            "POOLS",
            "TOTAL_REPLICAS",
            "AFFECTED_VOLUMES",
            "READY",
        ]
    }
}

impl CreateRow for NodePurgeImpact {
    fn row(&self) -> Row {
        row![
            self.node_id,
            self.status,
            self.cordoned,
            self.pools.len(),
            self.total_replicas,
            self.total_affected_volumes,
            self.ready_for_purge,
        ]
    }
}

impl GetHeaderRow for PoolImpact {
    fn get_header_row(&self) -> Row {
        row!["POOL", "STATUS", "REPLICAS", "AFFECTED_VOLUMES"]
    }
}

impl CreateRow for PoolImpact {
    fn row(&self) -> Row {
        row![
            self.pool_id,
            self.status,
            self.replica_count,
            optional_cell((!self.affected_volumes.is_empty()).then(|| {
                self.affected_volumes
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            })),
        ]
    }
}

impl Node {
    /// Show what would happen if this node is purged.
    async fn show_impact_analysis(node_id: &NodeId, output: &OutputFormat) -> PluginResult {
        // Get node info.
        let node = match RestClient::client().nodes_api().get_node(node_id).await {
            Ok(n) => n.into_body(),
            Err(e) => {
                return Err(Error::GetNodeError {
                    id: node_id.to_string(),
                    source: e,
                });
            }
        };

        let status = node.status.unwrap_or(models::NodeStatus::Unknown);
        let cordoned = node
            .spec
            .as_ref()
            .and_then(|s| s.cordondrainstate.as_ref())
            .is_some();

        // Get pools on this node.
        let node_pools = RestClient::client()
            .pools_api()
            .get_node_pools(node_id)
            .await
            .map(|r| r.into_body())
            .unwrap_or_default();

        // Get all volumes for cross-referencing replica topology.
        let volumes = RestClient::client()
            .volumes_api()
            .get_volumes(0, None, None)
            .await
            .map(|r| r.into_body().entries)
            .unwrap_or_default();

        let mut pool_impacts = Vec::new();
        let mut all_affected_volumes: std::collections::HashSet<super::VolumeId> =
            std::collections::HashSet::new();
        let mut total_replicas = 0usize;

        for pool in &node_pools {
            let pool_id = pool.spec.as_ref().map(|s| s.id.clone()).unwrap_or_default();

            let pool_status = pool
                .state
                .as_ref()
                .map(|s| s.status)
                .unwrap_or(models::PoolStatus::Unknown);

            let mut replica_count = 0usize;
            let mut affected_volumes: Vec<super::VolumeId> = Vec::new();

            for volume in &volumes {
                for topo in volume.state.replica_topology.values() {
                    if topo.pool.as_deref() == Some(pool_id.as_str()) {
                        replica_count += 1;
                        if !affected_volumes.contains(&volume.spec.uuid) {
                            affected_volumes.push(volume.spec.uuid);
                        }
                        all_affected_volumes.insert(volume.spec.uuid);
                    }
                }
            }

            total_replicas += replica_count;

            pool_impacts.push(PoolImpact {
                pool_id,
                status: pool_status,
                replica_count,
                affected_volumes,
            });
        }

        let ready_for_purge = status == models::NodeStatus::Offline && cordoned;

        let impact = NodePurgeImpact {
            node_id: node_id.clone(),
            status,
            cordoned,
            pools: pool_impacts.clone(),
            total_replicas,
            total_affected_volumes: all_affected_volumes.len(),
            ready_for_purge,
        };

        // Print node-level summary.
        print_table(output, impact);

        // Print per-pool breakdown if there are pools.
        if !pool_impacts.is_empty() {
            println!();
            println!("Per-pool breakdown:");
            print_table(output, pool_impacts);
        }

        Ok(())
    }
}
