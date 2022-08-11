use crate::{
    collect::{
        logs::create_directory_if_not_exist,
        resources,
        resources::{
            replica::{ReplicaClientWrapper, ReplicaTopology},
            traits, utils,
        },
        rest_wrapper::RestClient,
    },
    log,
};
use async_trait::async_trait;
use openapi::models::{Nexus, Volume};
use prettytable::Row;
use resources::ResourceError;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    fs::File,
    io::Write,
    path::{Path, PathBuf},
};
use traits::{
    ResourceInformation, Resourcer, Topologer, MAYASTOR_DAEMONSET_LABEL, RESOURCE_TO_CONTAINER_NAME,
};

/// Holds topological information of volume(like) --> {Replicas} --> {Pools} --> {Nodes}
/// of Volume resource
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct VolumeTopology {
    volume: Volume,
    target: Option<Nexus>,
    replicas_topology: Vec<ReplicaTopology>,
}

/// Implements functionality to inspect topological information of volume resource
impl Topologer for VolumeTopology {
    fn get_printable_topology(&self) -> Result<(String, String), ResourceError> {
        let topology_as_pretty = serde_json::to_string_pretty(self)?;
        let file_path = format!("volume-{}-topology.json", self.volume.spec.uuid);
        Ok((file_path, topology_as_pretty))
    }

    fn dump_topology_info(&self, dir_path: String) -> Result<(), ResourceError> {
        create_directory_if_not_exist(PathBuf::from(dir_path.clone()))?;
        let file_path =
            Path::new(&dir_path).join(format!("volume-{}-topology.json", self.volume.spec.uuid));
        let mut topo_file = File::create(file_path)?;
        let topology_as_pretty = serde_json::to_string_pretty(self)?;
        topo_file.write_all(topology_as_pretty.as_bytes())?;
        topo_file.flush()?;
        Ok(())
    }

    fn get_unhealthy_resource_info(&self) -> HashSet<ResourceInformation> {
        let mut resources = HashSet::new();
        for r in self.replicas_topology.iter() {
            resources.extend(r.get_unhealthy_resources());
        }
        if let Some(nexus) = &self.target {
            if !matches!(nexus.state, openapi::models::NexusState::Online) {
                let mut resource_info = ResourceInformation::default();
                resource_info.set_container_name(RESOURCE_TO_CONTAINER_NAME["nexus"].to_string());
                resource_info.set_host_name(nexus.node.clone());
                resource_info.set_label_selector([MAYASTOR_DAEMONSET_LABEL.to_string()].to_vec());
                resources.insert(resource_info);
            }
        }
        resources
    }

    fn get_all_resource_info(&self) -> HashSet<ResourceInformation> {
        let mut resources = HashSet::new();
        for r in self.replicas_topology.iter() {
            resources.extend(r.get_all_resources());
        }
        if let Some(nexus) = &self.target {
            let mut resource_info = ResourceInformation::default();
            resource_info.set_container_name(RESOURCE_TO_CONTAINER_NAME["nexus"].to_string());
            resource_info.set_host_name(nexus.node.clone());
            resource_info.set_label_selector([MAYASTOR_DAEMONSET_LABEL.to_string()].to_vec());
        }
        resources
    }

    fn get_k8s_resource_names(&self) -> Vec<String> {
        self.replicas_topology
            .clone()
            .into_iter()
            .flat_map(|r| r.get_k8s_resource_names())
            .collect::<Vec<String>>()
    }
}

// TablePrinter implements functionality to list volume resources in tabular format
impl traits::TablePrinter for openapi::models::Volume {
    fn get_header_row(&self) -> Row {
        utils::VOLUME_HEADERS.clone()
    }

    fn create_rows(&self) -> Vec<Row> {
        let state = self.state.clone();
        vec![row![state.uuid, state.status, state.size]]
    }

    fn get_resource_id(&self, row_data: &Row) -> Result<String, ResourceError> {
        Ok(row_data.get_cell(1).unwrap().get_content())
    }
}

// Wrapper around mayastor REST client
#[derive(Debug)]
pub(crate) struct VolumeClientWrapper {
    rest_client: RestClient,
    replica_client: ReplicaClientWrapper,
}

impl VolumeClientWrapper {
    /// Builds new instantance of VolumeClientWrapper
    pub(crate) fn new(client: RestClient) -> Self {
        VolumeClientWrapper {
            rest_client: client.clone(),
            replica_client: ReplicaClientWrapper::new(client),
        }
    }

    async fn list_volumes(&self) -> Result<Vec<Volume>, ResourceError> {
        let mut volumes: Vec<Volume> = Vec::new();
        let mut next_token: Option<isize> = Some(0);
        let max_entries: isize = utils::MAX_RESOURCE_ENTRIES;
        loop {
            let volumes_api_resp = self
                .rest_client
                .volumes_api()
                .get_volumes(max_entries, next_token)
                .await?
                .into_body();
            volumes.extend(volumes_api_resp.entries);
            if volumes_api_resp.next_token.is_none() {
                break;
            }
            next_token = volumes_api_resp.next_token;
        }
        Ok(volumes)
    }

    async fn get_volume(&self, id: openapi::apis::Uuid) -> Result<Volume, ResourceError> {
        let volume = self
            .rest_client
            .volumes_api()
            .get_volume(&id)
            .await?
            .into_body();
        Ok(volume)
    }

    async fn get_replica_topology(
        &self,
        id: openapi::apis::Uuid,
    ) -> Result<ReplicaTopology, ResourceError> {
        let topology = self.replica_client.get_replica_topology(id).await?;
        Ok(topology)
    }
}

#[async_trait(?Send)]
impl Resourcer for VolumeClientWrapper {
    type ID = openapi::apis::Uuid;

    async fn read_resource_id(&self) -> Result<Self::ID, ResourceError> {
        let volumes = self.list_volumes().await?;
        if volumes.is_empty() {
            log("No Volume resources, Are Volumes created?!!".to_string());
            return Err(ResourceError::CustomError(
                "Volume resources doesn't exist".to_string(),
            ));
        }
        let uuid_str = utils::print_table_and_get_id(volumes)?;
        let volume_uuid = openapi::apis::Uuid::parse_str(uuid_str.as_str())?;
        Ok(volume_uuid)
    }

    async fn get_topologer(
        &self,
        id: Option<Self::ID>,
    ) -> Result<Box<dyn Topologer>, ResourceError> {
        // When ID is provided then caller needs topology information for given volume ID
        if let Some(volume_id) = id {
            let volume = self.get_volume(volume_id).await?;
            let mut replicas_topology = Vec::new();
            for (replica_id_str, _value) in volume.state.replica_topology.clone() {
                let replica_uuid = openapi::apis::Uuid::parse_str(replica_id_str.as_str())?;
                let replica_topology = match self.get_replica_topology(replica_uuid).await {
                    Ok(val) => Some(val),
                    Err(err) => {
                        // TODO: As of now when mayastor daemon is in not Running state then
                        // fetching replica information from REST service will result
                        // error since REST service exposes only runtime spec of replica.
                        // So if it is not_found we are ignoring error
                        let _is_not_found_err = err.not_found_rest_json_error()?;
                        None
                    }
                };
                if let Some(topology) = replica_topology {
                    replicas_topology.push(topology);
                }
            }

            return Ok(Box::new(VolumeTopology {
                volume: volume.clone(),
                target: volume.state.target,
                replicas_topology,
            }));
        }

        // When ID is not provided then caller needs topology information for all volumes in the
        // cluster
        // available pools
        let mut volumes_topology = Vec::new();
        let volumes = self.list_volumes().await?;
        for volume in volumes.iter() {
            let mut replicas_topology = Vec::new();
            for (replica_id_str, _value) in volume.state.replica_topology.clone() {
                let replica_uuid = openapi::apis::Uuid::parse_str(replica_id_str.as_str()).unwrap();
                let replica_topology = match self.get_replica_topology(replica_uuid).await {
                    Ok(val) => Some(val),
                    // TODO: As of now when mayastor daemon is in not Running state then
                    // fetching replica information from REST service will result
                    // error since REST service exposes only runtime spec of replica.
                    // So if it is not_found we are ignoring error
                    Err(err) => {
                        let _is_not_found_err = err.not_found_rest_json_error()?;
                        None
                    }
                };
                if let Some(topology) = replica_topology {
                    replicas_topology.push(topology);
                }
            }
            volumes_topology.push(VolumeTopology {
                volume: volume.clone(),
                target: volume.state.target.clone(),
                replicas_topology,
            })
        }
        Ok(Box::new(volumes_topology))
    }
}
