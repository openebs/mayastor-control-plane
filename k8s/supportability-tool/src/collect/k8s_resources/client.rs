use k8s_openapi::api::core::v1::{Node, Pod};
use kube::{api::ListParams, Api, Client};
use std::{collections::HashMap, convert::TryFrom};

/// K8sResourceError holds errors that can obtain while fetching
/// information of Kubernetes Objects
#[allow(clippy::enum_variant_names)]
#[derive(Debug)]
pub(crate) enum K8sResourceError {
    ClientConfigError(kube::config::KubeconfigError),
    ClientError(kube::Error),
    ResourceError(Box<dyn std::error::Error>),
    CustomError(String),
}

impl From<kube::config::KubeconfigError> for K8sResourceError {
    fn from(e: kube::config::KubeconfigError) -> K8sResourceError {
        K8sResourceError::ClientConfigError(e)
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
        let client = match kube_config_path {
            Some(config_path) => {
                let kube_config = kube::config::Kubeconfig::read_from(&config_path)
                    .map_err(|e| -> K8sResourceError { e.into() })?;
                let config =
                    kube::Config::from_custom_kubeconfig(kube_config, &Default::default()).await?;
                Client::try_from(config)?
            }
            None => Client::try_default().await?,
        };
        Ok(Self { client, namespace })
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
        let list_params = ListParams::default()
            .labels(label_selector)
            .fields(field_selector);

        let pod_api: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);
        let pods = pod_api.list(&list_params).await?;
        Ok(pods.items)
    }
}
