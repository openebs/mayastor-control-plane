use crate::{
    collect::{
        logs::create_directory_if_not_exist,
        resources,
        resources::{traits, utils},
        rest_wrapper::RestClient,
    },
    log,
};
use async_trait::async_trait;
use openapi::models::{BlockDevice, Node};
use prettytable::Row;
use resources::ResourceError;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    fs::File,
    io::Write,
    iter::FromIterator,
    path::{Path, PathBuf},
};
use traits::{
    ResourceInformation, Resourcer, Topologer, MAYASTOR_DAEMONSET_LABEL, RESOURCE_TO_CONTAINER_NAME,
};

/// NodeTopology represents information about
/// mayastor node and devices attached to node
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct NodeTopology {
    node: Node,
    devices: Option<Vec<BlockDevice>>,
}

/// Check the status of block device and return true when device is not online
pub(crate) fn is_device_not_online(_device: &BlockDevice) -> bool {
    // TODO: Update when Device API represents state of block device
    false
}

/// Check the status of node and return true only when when node is online
/// else false(which state of node is not online)
pub(crate) fn is_node_online(node_state: openapi::models::NodeStatus) -> bool {
    matches!(node_state, openapi::models::NodeStatus::Online)
}

impl NodeTopology {
    // fetch details of mayastor node where device is attached/accessible
    fn get_device_node_info(
        &self,
        predicate_fn: fn(&BlockDevice) -> bool,
    ) -> HashSet<ResourceInformation> {
        let resources: HashSet<ResourceInformation> = HashSet::new();
        if let Some(devices) = &self.devices {
            return HashSet::from_iter(devices.iter().filter(|d| predicate_fn(&(*d).clone())).map(
                |_d| {
                    let mut resource_info = ResourceInformation::default();
                    resource_info
                        .set_container_name(RESOURCE_TO_CONTAINER_NAME["device"].to_string());
                    resource_info.set_host_name(self.node.id.clone());
                    resource_info
                        .set_label_selector([MAYASTOR_DAEMONSET_LABEL.to_string()].to_vec());
                    resource_info
                },
            ));
        }
        resources
    }
}

/// Topologer Contains methods to build topology information for generic Object
/// which interns supports inspecting topology resources
#[async_trait(?Send)]
impl Topologer for NodeTopology {
    /// Convert node topology into JSON structure
    fn get_printable_topology(&self) -> Result<(String, String), ResourceError> {
        let topology_as_pretty = serde_json::to_string_pretty(self)?;
        let file_path = format!("node-{}-topology.json", self.node.id);
        Ok((file_path, topology_as_pretty))
    }

    /// Writes topology information into a file in specified directory
    fn dump_topology_info(&self, dir_path: String) -> Result<(), ResourceError> {
        create_directory_if_not_exist(PathBuf::from(dir_path.clone()))?;
        let file_path = Path::new(&dir_path).join(format!("node-{}-topology.json", self.node.id));
        let mut topo_file = File::create(file_path)?;
        let topology_as_pretty = serde_json::to_string_pretty(self)?;
        topo_file.write_all(topology_as_pretty.as_bytes())?;
        topo_file.flush()?;
        Ok(())
    }

    fn get_unhealthy_resource_info(&self) -> HashSet<ResourceInformation> {
        let mut resources: HashSet<ResourceInformation> = HashSet::new();
        if let Some(node_state) = &self.node.state {
            if !is_node_online(node_state.status) {
                let mut resource_info = ResourceInformation::default();
                resource_info.set_container_name(RESOURCE_TO_CONTAINER_NAME["node"].to_string());
                resource_info.set_host_name(node_state.id.clone());
                resource_info.set_label_selector([MAYASTOR_DAEMONSET_LABEL.to_string()].to_vec());
                resources.insert(resource_info);
            }
        } else if let Some(node_spec) = &self.node.spec {
            let mut resource_info = ResourceInformation::default();
            resource_info.set_container_name(RESOURCE_TO_CONTAINER_NAME["node"].to_string());
            resource_info.set_host_name(node_spec.id.clone());
            resource_info.set_label_selector([MAYASTOR_DAEMONSET_LABEL.to_string()].to_vec());
            resources.insert(resource_info);
        }
        resources.extend(self.get_device_node_info(is_device_not_online));
        resources
    }

    fn get_all_resource_info(&self) -> HashSet<ResourceInformation> {
        let mut resources = HashSet::new();
        if let Some(node_spec) = &self.node.spec {
            let mut resource_info = ResourceInformation::default();
            resource_info.set_container_name(RESOURCE_TO_CONTAINER_NAME["node"].to_string());
            resource_info.set_host_name(node_spec.id.clone());
            resource_info.set_label_selector([MAYASTOR_DAEMONSET_LABEL.to_string()].to_vec());
            resources.insert(resource_info);
        }
        resources
    }

    fn get_k8s_resource_names(&self) -> Vec<String> {
        // NOTE: AS of now there is only one direct dependency between Kubernetes
        // CustomResource and mayastor resource i.e disk pool resource(msp). Since mayastor
        // node doesn't have any dependency we can safely return empty vector
        vec![]
    }
}

/// TablePrinter holds methods to display node information in Tabular Manner
impl traits::TablePrinter for Node {
    fn get_header_row(&self) -> Row {
        utils::NODE_HEADERS.clone()
    }

    fn create_rows(&self) -> Vec<Row> {
        let mut row = vec![row![self.id]];
        let state = self.state.clone();
        if let Some(node_state) = state {
            row = vec![row![node_state.id, node_state.status]];
        }
        row
    }

    fn get_resource_id(&self, row_data: &Row) -> Result<String, ResourceError> {
        Ok(row_data.get_cell(1).unwrap().get_content())
    }
}

// Wrapper around mayastor REST client
#[derive(Debug)]
pub(crate) struct NodeClientWrapper {
    pub rest_client: RestClient,
}

impl NodeClientWrapper {
    /// Creates new instance of NodeClientWrapper
    pub(crate) fn new(rest_client: RestClient) -> Self {
        NodeClientWrapper { rest_client }
    }

    // TODO: Add pagination support when REST service supports it
    async fn list_nodes(&self) -> Result<Vec<Node>, ResourceError> {
        let nodes = self.rest_client.nodes_api().get_nodes().await?.into_body();
        Ok(nodes)
    }

    async fn get_node(&self, id: String) -> Result<Node, ResourceError> {
        let node = self
            .rest_client
            .nodes_api()
            .get_node(&id)
            .await?
            .into_body();
        Ok(node)
    }

    async fn list_node_block_devices(&self, id: String) -> Result<Vec<BlockDevice>, ResourceError> {
        let devices = match self
            .rest_client
            .block_devices_api()
            .get_node_block_devices(&id, Some(true))
            .await
        {
            Ok(response) => response.into_body(),
            Err(err) => {
                let _is_not_found =
                    ResourceError::RestJsonError(err).not_found_rest_json_error()?;
                Vec::new()
            }
        };
        Ok(devices)
    }
}

#[async_trait(?Send)]
impl Resourcer for NodeClientWrapper {
    type ID = String;

    async fn read_resource_id(&self) -> Result<Self::ID, ResourceError> {
        let nodes = self.list_nodes().await?;
        if nodes.is_empty() {
            println!("No Node resources, Are daemonset pods in Running State?!!");
            return Err(ResourceError::CustomError("No Node resources".to_string()));
        }
        let node_id = utils::print_table_and_get_id(nodes)?;
        Ok(node_id)
    }

    async fn get_topologer(
        &self,
        id: Option<Self::ID>,
    ) -> Result<Box<dyn Topologer>, ResourceError> {
        // When ID is provided then caller needs topologer for given node name
        if let Some(node_id) = id {
            let node = self.get_node(node_id.clone()).await?;
            let mut devices: Option<Vec<BlockDevice>> = None;
            if let Some(node_state) = node.clone().state {
                if matches!(node_state.status, openapi::models::NodeStatus::Online) {
                    devices = Some(self.list_node_block_devices(node_id).await?);
                }
            }
            let node_topology = NodeTopology { node, devices };
            return Ok(Box::new(node_topology));
        }
        // When ID is not provided then caller needs topology information to build for all
        // available nodes
        let mut nodes_topology: Vec<NodeTopology> = Vec::new();
        let mayastor_nodes = self.list_nodes().await?;
        for node in mayastor_nodes.iter() {
            let mut devices: Option<Vec<BlockDevice>> = None;
            if let Some(node_state) = node.clone().state {
                if matches!(node_state.status, openapi::models::NodeStatus::Online) {
                    devices = Some(self.list_node_block_devices(node.id.clone()).await?);
                }
            }
            let node_topology = NodeTopology {
                node: node.clone(),
                devices,
            };
            nodes_topology.push(node_topology);
        }
        if nodes_topology.is_empty() {
            log("No Node resources, Are daemonset pods in Running State?!!".to_string());
            return Err(ResourceError::CustomError("No Node resources".to_string()));
        }
        Ok(Box::new(nodes_topology))
    }
}
