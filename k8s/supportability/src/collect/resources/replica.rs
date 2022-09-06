use crate::collect::{
    resources,
    resources::{
        pool::{PoolClientWrapper, PoolTopology},
        traits,
        traits::Topologer,
        Resourcer,
    },
    rest_wrapper::RestClient,
};
use openapi::models::Replica;
use resources::ResourceError;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use traits::{ResourceInformation, MAYASTOR_DAEMONSET_LABEL, RESOURCE_TO_CONTAINER_NAME};

/// ReplicaTopology represents information about
/// mayastor volume replica and all it's descendants
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct ReplicaTopology {
    replica: Replica,
    pool_topology: PoolTopology,
}

impl ReplicaTopology {
    /// fetch unhealthy replica resource information(where replica is hosted) and all it's
    /// descendants(pool, node)
    pub(crate) fn get_unhealthy_resources(&self) -> HashSet<ResourceInformation> {
        let mut resources = HashSet::new();
        resources.extend(self.pool_topology.get_unhealthy_resource_info());
        if !resources.is_empty() {
            return resources;
        }
        if !matches!(self.replica.state, openapi::models::ReplicaState::Online) {
            let mut resource_info = ResourceInformation::default();
            resource_info.set_container_name(RESOURCE_TO_CONTAINER_NAME["replica"].to_string());
            resource_info.set_host_name(self.replica.node.clone());
            resource_info.set_label_selector([MAYASTOR_DAEMONSET_LABEL.to_string()].to_vec());
            resources.insert(resource_info);
        }
        resources
    }

    /// fetch replica resources information and all it's descendants will exist
    /// in same mayastor node
    pub fn get_all_resources(&self) -> HashSet<ResourceInformation> {
        // Filling replica node alone is good enough since pool & node will be always on replica
        // node
        let mut resources = HashSet::new();
        let mut resource_info = ResourceInformation::default();
        resource_info.set_container_name(RESOURCE_TO_CONTAINER_NAME["replica"].to_string());
        resource_info.set_host_name(self.replica.node.clone());
        resource_info.set_label_selector([MAYASTOR_DAEMONSET_LABEL.to_string()].to_vec());
        resources.insert(resource_info);

        resources
    }

    /// fetch pool name of replica resource
    pub fn get_k8s_resource_names(&self) -> Vec<String> {
        self.pool_topology.get_k8s_resource_names()
    }
}

// Wrapper around mayastor REST client
#[derive(Debug)]
pub struct ReplicaClientWrapper {
    rest_client: RestClient,
    pool_client: PoolClientWrapper,
}

impl ReplicaClientWrapper {
    /// Creates new instance of ReplicaClientWrapper
    pub fn new(client: RestClient) -> Self {
        ReplicaClientWrapper {
            rest_client: client.clone(),
            pool_client: PoolClientWrapper::new(client),
        }
    }

    // TODO: Add pagination support when REST service supports it
    #[allow(dead_code)]
    async fn list_replicas(&self) -> Result<Vec<Replica>, ResourceError> {
        let replicas = self
            .rest_client
            .replicas_api()
            .get_replicas()
            .await?
            .into_body();
        Ok(replicas)
    }

    async fn get_replica(&self, id: openapi::apis::Uuid) -> Result<Replica, ResourceError> {
        let replicas = self
            .rest_client
            .replicas_api()
            .get_replica(&id)
            .await?
            .into_body();
        Ok(replicas)
    }

    /// Fetch topological information of replica and all it's descendants(pool, node)
    pub(crate) async fn get_replica_topology(
        &self,
        id: openapi::apis::Uuid,
    ) -> Result<ReplicaTopology, ResourceError> {
        let replica = self.get_replica(id).await?;
        let topologer = self
            .pool_client
            .get_topologer(Some(replica.pool.clone()))
            .await?;
        let pool_topology = match topologer.downcast_ref::<PoolTopology>() {
            Some(val) => val.clone(),
            None => {
                panic!("Not a PoolTopology type");
            }
        };

        Ok(ReplicaTopology {
            replica,
            pool_topology,
        })
    }
}
