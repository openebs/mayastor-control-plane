use crate::collect::k8s_resources::common::KUBERNETES_HOST_LABEL_KEY;
use k8s_openapi::api::{
    apps::v1::{DaemonSet, Deployment, StatefulSet},
    core::v1::{Event, Node, Pod},
};
use k8s_operators::diskpool::crd::DiskPool;
use kube::{api::ListParams, Api, Client, Resource};

use std::{collections::HashMap, convert::TryFrom};

/// K8sResourceError holds errors that can obtain while fetching
/// information of Kubernetes Objects
#[allow(clippy::enum_variant_names)]
#[derive(Debug)]
pub(crate) enum K8sResourceError {
    ClientConfigError(kube::config::KubeconfigError),
    InferConfigError(kube::config::InferConfigError),
    ClientError(kube::Error),
    ResourceError(Box<dyn std::error::Error>),
    CustomError(String),
}

impl From<kube::config::KubeconfigError> for K8sResourceError {
    fn from(e: kube::config::KubeconfigError) -> K8sResourceError {
        K8sResourceError::ClientConfigError(e)
    }
}

impl From<kube::config::InferConfigError> for K8sResourceError {
    fn from(e: kube::config::InferConfigError) -> K8sResourceError {
        K8sResourceError::InferConfigError(e)
    }
}

impl From<kube::Error> for K8sResourceError {
    fn from(e: kube::Error) -> K8sResourceError {
        K8sResourceError::ClientError(e)
    }
}

impl From<Box<dyn std::error::Error>> for K8sResourceError {
    fn from(e: Box<dyn std::error::Error>) -> K8sResourceError {
        K8sResourceError::ResourceError(e)
    }
}

impl From<String> for K8sResourceError {
    fn from(e: String) -> K8sResourceError {
        K8sResourceError::CustomError(e)
    }
}

impl K8sResourceError {
    /// Returns K8sResourceError from provided message
    pub fn invalid_k8s_resource_value(err: String) -> Self {
        Self::CustomError(err)
    }
}

/// ClientSet is wrapper Kubernetes clientset and namespace of mayastor service
#[derive(Clone)]
pub(crate) struct ClientSet {
    client: kube::Client,
    namespace: String,
}

impl ClientSet {
    /// Create a new ClientSet, from the config file if provided, otherwise with default.
    pub(crate) async fn new(
        kube_config_path: Option<std::path::PathBuf>,
        namespace: String,
    ) -> Result<Self, K8sResourceError> {
        let config = match kube_config_path {
            Some(config_path) => {
                let kube_config = kube::config::Kubeconfig::read_from(&config_path)
                    .map_err(|e| -> K8sResourceError { e.into() })?;
                kube::Config::from_custom_kubeconfig(kube_config, &Default::default()).await?
            }
            None => kube::Config::infer().await?,
        };
        let client = Client::try_from(config)?;
        Ok(Self { client, namespace })
    }

    /// Get a clone of the inner `kube::Client`.
    pub(crate) fn kube_client(&self) -> kube::Client {
        self.client.clone()
    }

    /// Fetch node objects from API-server then form and return map of node name to node object
    pub(crate) async fn get_nodes_map(&self) -> Result<HashMap<String, Node>, K8sResourceError> {
        let node_api: Api<Node> = Api::all(self.client.clone());
        let nodes = node_api.list(&ListParams::default()).await?;
        let mut node_map = HashMap::new();
        for node in nodes.items {
            node_map.insert(
                node.metadata
                    .name
                    .as_ref()
                    .ok_or_else(|| {
                        K8sResourceError::CustomError("Unable to get node name".to_string())
                    })?
                    .clone(),
                node,
            );
        }
        Ok(node_map)
    }

    /// Fetch list of pods associated to given label_selector & field_selector
    pub(crate) async fn get_pods(
        &self,
        label_selector: &str,
        field_selector: &str,
    ) -> Result<Vec<Pod>, K8sResourceError> {
        let mut list_params = ListParams::default()
            .labels(label_selector)
            .fields(field_selector)
            .limit(100);

        let mut pods: Vec<Pod> = vec![];

        let pods_api: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);
        // Paginate to get 100 contents at a time
        loop {
            let mut result = pods_api.list(&list_params).await?;
            pods.append(&mut result.items);
            match result.metadata.continue_ {
                None => break,
                Some(token) => list_params = list_params.continue_token(token.as_str()),
            };
        }
        Ok(pods)
    }

    /// get the k8s pod api for pod operations, like logs_stream
    pub(crate) async fn get_pod_api(&self) -> Api<Pod> {
        Api::namespaced(self.client.clone(), &self.namespace)
    }

    /// Fetch list of disk pools associated to given names if None is provided then
    /// all results will be returned
    pub(crate) async fn list_pools(
        &self,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
    ) -> Result<Vec<DiskPool>, K8sResourceError> {
        let list_params = ListParams::default()
            .labels(label_selector.unwrap_or_default())
            .fields(field_selector.unwrap_or_default());
        let pools_api: Api<DiskPool> = Api::namespaced(self.client.clone(), &self.namespace);
        let pools = match pools_api.list(&list_params).await {
            Ok(val) => val,
            Err(kube_error) => match kube_error {
                kube::Error::Api(e) => {
                    if e.code == 404 {
                        return Ok(vec![]);
                    }
                    return Err(K8sResourceError::ClientError(kube::Error::Api(e)));
                }
                _ => return Err(K8sResourceError::ClientError(kube_error)),
            },
        };
        Ok(pools.items)
    }

    /// Fetch list of k8s events associated to given label_selector & field_selector
    pub(crate) async fn get_events(
        &self,
        label_selector: &str,
        field_selector: &str,
    ) -> Result<Vec<Event>, K8sResourceError> {
        let mut list_params = ListParams::default()
            .labels(label_selector)
            .fields(field_selector)
            .limit(100);

        let mut events: Vec<Event> = vec![];

        let events_api: Api<Event> = Api::namespaced(self.client.clone(), &self.namespace);
        // Paginate to get 100 contents at a time
        loop {
            let mut result = events_api.list(&list_params).await?;
            events.append(&mut result.items);
            match result.metadata.continue_ {
                None => break,
                Some(token) => list_params = list_params.continue_token(token.as_str()),
            };
        }

        Ok(events)
    }

    /// Fetch list of deployments associated to given label_selector & field_selector
    pub(crate) async fn get_deployments(
        &self,
        label_selector: &str,
        field_selector: &str,
    ) -> Result<Vec<Deployment>, K8sResourceError> {
        let list_params = ListParams::default()
            .labels(label_selector)
            .fields(field_selector);

        let deployments_api: Api<Deployment> =
            Api::namespaced(self.client.clone(), &self.namespace);
        let deployments = deployments_api.list(&list_params).await?;
        Ok(deployments.items)
    }

    /// Fetch list of daemonsets associated to given label_selector & field_selector
    pub(crate) async fn get_daemonsets(
        &self,
        label_selector: &str,
        field_selector: &str,
    ) -> Result<Vec<DaemonSet>, K8sResourceError> {
        let list_params = ListParams::default()
            .labels(label_selector)
            .fields(field_selector);

        let ds_api: Api<DaemonSet> = Api::namespaced(self.client.clone(), &self.namespace);
        let daemonsets = ds_api.list(&list_params).await?;
        Ok(daemonsets.items)
    }

    /// Fetch list of statefulsets associated to given label_selector & field_selector
    pub(crate) async fn get_statefulsets(
        &self,
        label_selector: &str,
        field_selector: &str,
    ) -> Result<Vec<StatefulSet>, K8sResourceError> {
        let list_params = ListParams::default()
            .labels(label_selector)
            .fields(field_selector);

        let sts_api: Api<StatefulSet> = Api::namespaced(self.client.clone(), &self.namespace);
        let statefulsets = sts_api.list(&list_params).await?;
        Ok(statefulsets.items)
    }

    /// Returns the hostname of provided node name by reading from Kubernetes
    /// object labels
    pub(crate) async fn get_hostname(&self, node_name: &str) -> Result<String, K8sResourceError> {
        let node_api: Api<Node> = Api::all(self.client.clone());
        let node = node_api.get(node_name).await?;

        // Labels will definitely exists on Kubernetes node object
        let labels = node.meta().labels.as_ref().ok_or_else(|| {
            K8sResourceError::CustomError(format!("No labels available on node '{}'", node_name))
        })?;

        let reqired_label_value = labels
            .get(KUBERNETES_HOST_LABEL_KEY)
            .ok_or_else(|| {
                K8sResourceError::CustomError(format!(
                    "Node '{}' label not found on node {}",
                    KUBERNETES_HOST_LABEL_KEY, node_name
                ))
            })?
            .as_str();
        Ok(reqired_label_value.to_string())
    }

    /// Get node name from a specified hostname
    pub(crate) async fn get_nodename(&self, host_name: &str) -> Result<String, K8sResourceError> {
        let node_api: Api<Node> = Api::all(self.client.clone());
        let node = node_api
            .list(
                &ListParams::default()
                    .labels(format!("{}={}", KUBERNETES_HOST_LABEL_KEY, host_name).as_str()),
            )
            .await?;
        if node.items.is_empty() {
            return Err(K8sResourceError::CustomError(format!(
                "No node found for hostname {}",
                host_name
            )));
        }
        // Since object fetched from Kube-apiserver node name will always exist
        if let Some(node) = node.items.first() {
            Ok(node
                .metadata
                .name
                .clone()
                .expect("Node Name should exist in kube-apiserver"))
        } else {
            Err(K8sResourceError::CustomError(format!(
                "No node found for hostname {}",
                host_name
            )))
        }
    }
}
