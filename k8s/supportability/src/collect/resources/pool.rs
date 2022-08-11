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
use openapi::models::{BlockDevice, Node, Pool};
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

/// PoolTopology represents information about
/// mayastor pool and all it's descendants
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct PoolTopology {
    pool: Pool,
    node_info: Option<Node>,
    device_info: Option<Vec<BlockDevice>>,
}

/// Check the status of pool and returns true only when when pool is not online
/// else false(which states pool is online)
pub(crate) fn is_pool_not_online(pool_status: openapi::models::PoolStatus) -> bool {
    !matches!(pool_status, openapi::models::PoolStatus::Online)
}

impl PoolTopology {
    // fetch mayastor daemon information where mayastor pools are hosted
    fn get_pool_info(
        &self,
        predicate_fn: fn(openapi::models::PoolStatus) -> bool,
    ) -> HashSet<ResourceInformation> {
        let mut resources: HashSet<ResourceInformation> = HashSet::new();
        if let Some(pool_state) = &self.pool.state {
            if predicate_fn(pool_state.status) {
                let mut resource_info = ResourceInformation::default();
                resource_info.set_container_name(RESOURCE_TO_CONTAINER_NAME["pool"].to_string());
                resource_info.set_host_name(pool_state.node.clone());
                resource_info.set_label_selector([MAYASTOR_DAEMONSET_LABEL.to_string()].to_vec());
                resources.insert(resource_info);
            }
        } else if let Some(pool_spec) = &self.pool.spec {
            let mut resource_info = ResourceInformation::default();
            resource_info.set_container_name(RESOURCE_TO_CONTAINER_NAME["pool"].to_string());
            resource_info.set_host_name(pool_spec.node.clone());
            resource_info.set_label_selector([MAYASTOR_DAEMONSET_LABEL.to_string()].to_vec());
            resources.insert(resource_info);
        }
        resources
    }
}

/// Topologer Contains methods to inspect topology information of pool resource
#[async_trait(?Send)]
impl Topologer for PoolTopology {
    fn get_printable_topology(&self) -> Result<(String, String), ResourceError> {
        let topology_as_pretty = serde_json::to_string_pretty(self)?;
        let file_path = format!("pool-{}-topology.json", self.pool.id);
        Ok((file_path, topology_as_pretty))
    }

    fn dump_topology_info(&self, dir_path: String) -> Result<(), ResourceError> {
        create_directory_if_not_exist(PathBuf::from(dir_path.clone()))?;
        let file_path = Path::new(&dir_path).join(format!("pool-{}-topology.json", self.pool.id));
        let mut topo_file = File::create(file_path)?;
        let topology_as_pretty = serde_json::to_string_pretty(self)?;
        topo_file.write_all(topology_as_pretty.as_bytes())?;
        topo_file.flush()?;
        Ok(())
    }

    fn get_unhealthy_resource_info(&self) -> HashSet<ResourceInformation> {
        let mut resources = HashSet::new();
        if let Some(node_info) = self.node_info.clone() {
            if let Some(node_state) = node_info.state {
                if !matches!(node_state.status, openapi::models::NodeStatus::Online) {
                    let mut resource_info = ResourceInformation::default();
                    resource_info
                        .set_container_name(RESOURCE_TO_CONTAINER_NAME["node"].to_string());
                    resource_info.set_host_name(node_state.id);
                    resource_info
                        .set_label_selector([MAYASTOR_DAEMONSET_LABEL.to_string()].to_vec());
                    resources.insert(resource_info);
                }
            } else if let Some(node_spec) = node_info.spec {
                let mut resource_info = ResourceInformation::default();
                resource_info.set_container_name(RESOURCE_TO_CONTAINER_NAME["node"].to_string());
                resource_info.set_host_name(node_spec.id);
                resource_info.set_label_selector([MAYASTOR_DAEMONSET_LABEL.to_string()].to_vec());
                resources.insert(resource_info);
            }
        }
        resources.extend(self.get_pool_info(is_pool_not_online));
        resources
    }

    fn get_all_resource_info(&self) -> HashSet<ResourceInformation> {
        // Collecting pool information alone is good enough since there will be
        // always 1:1 mapping between pools & nodes
        self.get_pool_info(|_pool_state: openapi::models::PoolStatus| true)
    }

    fn get_k8s_resource_names(&self) -> Vec<String> {
        // NOTE: As of now mayastor disk pool is the only Kubernetes resource linked
        //       to mayastor disk pools. In future if any resource gets added update
        //       return type as HashMap<ResourceType, Vec<String>>
        vec![self.pool.id.clone()]
    }
}

// TablePrinter holds methods to display pool information in tabular manner
impl traits::TablePrinter for Pool {
    fn get_header_row(&self) -> Row {
        utils::POOL_HEADERS.clone()
    }

    fn create_rows(&self) -> Vec<Row> {
        let mut pool_node: String = String::new();
        let mut disks: String = String::new();
        // TODO: Is it Ok to assign pool status?
        let mut pool_status = openapi::models::PoolStatus::Unknown;
        if let Some(pool_spec) = &self.spec {
            pool_node = pool_spec.node.clone();
            disks = pool_spec.disks.join(",");
        }
        if let Some(pool_state) = &self.state {
            pool_status = pool_state.status;
        }
        vec![row![self.id, disks, pool_node, pool_status]]
    }

    fn get_resource_id(&self, row_data: &Row) -> Result<String, ResourceError> {
        Ok(row_data.get_cell(1).unwrap().get_content())
    }
}

/// Wrapper around mayastor REST client which interns used to interact with REST client
#[derive(Debug)]
pub struct PoolClientWrapper {
    rest_client: RestClient,
}

impl PoolClientWrapper {
    /// Creates new instance of PoolClientWrapper
    pub fn new(client: RestClient) -> Self {
        PoolClientWrapper {
            rest_client: client,
        }
    }

    // TODO: Add pagination support when REST service supports it
    async fn list_pools(&self) -> Result<Vec<Pool>, ResourceError> {
        let pools = self.rest_client.pools_api().get_pools().await?.into_body();
        Ok(pools)
    }

    async fn get_pool(&self, id: String) -> Result<Pool, ResourceError> {
        let pool = self
            .rest_client
            .pools_api()
            .get_pool(&id)
            .await?
            .into_body();
        Ok(pool)
    }

    async fn get_pool_node_info(&self, pool: Pool) -> Result<Option<Node>, ResourceError> {
        if let Some(pool_spec) = pool.spec {
            let node = self
                .rest_client
                .nodes_api()
                .get_node(&pool_spec.node.clone())
                .await?
                .into_body();
            return Ok(Some(node));
        }
        Ok(None)
    }

    async fn get_pool_disks_info(
        &self,
        pool: Pool,
    ) -> Result<Option<Vec<BlockDevice>>, ResourceError> {
        if let Some(pool_spec) = pool.spec {
            let devices = self
                .rest_client
                .block_devices_api()
                .get_node_block_devices(&pool_spec.node.clone(), Some(true))
                .await?
                .into_body();

            let filtered_devices: Vec<BlockDevice> = devices
                .into_iter()
                .filter(|device| is_it_pool_device(pool_spec.clone(), device))
                .collect::<Vec<BlockDevice>>();
            return Ok(Some(filtered_devices));
        }
        Ok(None)
    }
}

fn is_it_pool_device(pool_spec: openapi::models::PoolSpec, device: &BlockDevice) -> bool {
    let mut device_links: HashSet<String> = HashSet::from_iter(device.devlinks.iter().cloned());
    device_links.insert(device.devname.clone());
    device_links.insert(device.devpath.clone());
    for pool_device_name in pool_spec.disks {
        if device_links.contains(&pool_device_name) {
            return true;
        }
    }
    false
}

#[async_trait(?Send)]
impl Resourcer for PoolClientWrapper {
    type ID = String;

    async fn read_resource_id(&self) -> Result<Self::ID, ResourceError> {
        let pools = self.list_pools().await?;
        if pools.is_empty() {
            log("No Pool resources, Are Pools created?!!".to_string());
            return Err(ResourceError::CustomError("No Pool resources".to_string()));
        }
        let pool_id = utils::print_table_and_get_id(pools)?;
        Ok(pool_id)
    }

    async fn get_topologer(
        &self,
        id: Option<Self::ID>,
    ) -> Result<Box<dyn Topologer>, ResourceError> {
        // When ID is provided then caller needs topologer for given pool id
        if let Some(pool_id) = id {
            let pool = self.get_pool(pool_id.clone()).await?;
            let node_info = match self.get_pool_node_info(pool.clone()).await {
                Ok(node_info) => node_info,
                Err(e) => {
                    // TODO: Collect errors and return to caller at end
                    log(format!(
                        "Failed to get node information for pool: {}, error: {:?}",
                        pool_id, e
                    ));
                    None
                }
            };
            let device_info = match self.get_pool_disks_info(pool.clone()).await {
                Ok(d_info) => d_info,
                Err(e) => {
                    // TODO: Collect errors and return to caller at end
                    log(format!(
                        "Failed to get device information for pool: {}, error: {:?}",
                        pool_id, e
                    ));
                    None
                }
            };
            let pool_topology = PoolTopology {
                pool,
                node_info,
                device_info,
            };
            return Ok(Box::new(pool_topology));
        }

        // When ID is not provided then caller needs topology information to build for all
        // available pools
        let mut pools_topology: Vec<PoolTopology> = Vec::new();
        let pools = self.list_pools().await?;
        for pool in pools.into_iter() {
            let node_info = match self.get_pool_node_info(pool.clone()).await {
                Ok(node_info) => node_info,
                Err(e) => {
                    // TODO: Collect errors and return to caller at end
                    log(format!(
                        "Failed to get node information for pools, error: {:?}",
                        e
                    ));
                    None
                }
            };
            let device_info = match self.get_pool_disks_info(pool.clone()).await {
                Ok(d_info) => d_info,
                Err(e) => {
                    // TODO: Collect errors and return to caller at end
                    log(format!(
                        "Failed to get device information for pools, error: {:?}",
                        e
                    ));
                    None
                }
            };

            let pool_topology = PoolTopology {
                pool,
                node_info,
                device_info,
            };
            pools_topology.push(pool_topology);
        }
        Ok(Box::new(pools_topology))
    }
}
