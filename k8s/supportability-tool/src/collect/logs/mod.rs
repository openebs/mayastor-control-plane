mod loki;
use crate::collect::{
    constants::{CONTROL_PLANE_SERVICES, DATA_PLANE_SERVICES, HOST_NAME_REQUIRED_SERVICES},
    k8s_resources::{
        client::{ClientSet, K8sResourceError},
        common::{KUBERNETES_HOST_LABEL_KEY, RUNNING_FIELD_SELECTOR},
    },
};
use async_trait::async_trait;
use k8s_openapi::api::core::v1::Pod;
use std::{collections::HashSet, iter::Iterator, path::PathBuf};

// Represents logging services in the system
const LOGGING_LABEL_SELECTOR: &str = "openebs.io/logging=true";

/// Error that can occur while interacting with logs module
#[derive(Debug)]
pub(crate) enum LogError {
    Loki(loki::LokiError),
    K8sResource(K8sResourceError),
    IOError(std::io::Error),
    Custom(String),
}

impl From<loki::LokiError> for LogError {
    fn from(e: loki::LokiError) -> LogError {
        LogError::Loki(e)
    }
}

impl From<K8sResourceError> for LogError {
    fn from(e: K8sResourceError) -> LogError {
        LogError::K8sResource(e)
    }
}

impl From<String> for LogError {
    fn from(e: String) -> LogError {
        LogError::Custom(e)
    }
}

impl From<std::io::Error> for LogError {
    fn from(e: std::io::Error) -> LogError {
        LogError::IOError(e)
    }
}

/// Contains fields to identify cluster resources
#[derive(Hash, PartialEq, Eq, Clone, Debug)]
pub(crate) struct LogResource {
    /// Defines the name of the service to fetch logs
    pub(crate) container_name: String,

    /// Identifiy hostname of the service
    pub(crate) host_name: Option<String>,

    /// Uniquely identifies the service via label selector
    pub(crate) label_selector: String,

    /// States the type of the service(mayastor/agents/...)
    pub(crate) service_type: String,
}

/// LogCollection is a wrapper around internal service of log collection
#[derive(Clone)]
pub(crate) struct LogCollection {
    loki_client: Option<loki::LokiClient>,
    clientset: ClientSet,
}

impl LogCollection {
    /// new create new instance of Logger service based on provided arguments
    /// param 'kube_config_path' --> Holds path to kubernetes config required to interact with
    /// Kube-API server param 'namespace' --> Defines the namespace of the product
    /// param 'loki_uri' --> Defines the address of loki instance
    /// param 'since'  --> Defines period from which logs needs to collect
    /// param 'timeout' --> Specifies the timeout while interacting with Loki Service
    pub(crate) async fn new_logger(
        kube_config_path: Option<std::path::PathBuf>,
        namespace: String,
        loki_uri: Option<String>,
        since: humantime::Duration,
        timeout: humantime::Duration,
    ) -> Result<Box<dyn Logger>, LogError> {
        Ok(Box::new(Self {
            loki_client: loki_uri.map(|uri| loki::LokiClient::new(uri, since, timeout)),
            clientset: ClientSet::new(kube_config_path, namespace).await?,
        }))
    }

    async fn get_logging_resources(
        &self,
        pods: Vec<Pod>,
    ) -> Result<HashSet<LogResource>, LogError> {
        let nodes_map = self.clientset.get_nodes_map().await?;
        let mut logging_resources = HashSet::new();

        for pod in pods {
            let service_name = pod
                .metadata
                .labels
                .as_ref()
                .ok_or_else(|| {
                    K8sResourceError::invalid_k8s_resource_value(format!(
                        "No labels found in pod {:?}",
                        pod.metadata.name
                    ))
                })?
                .get("app")
                .unwrap_or(&"".to_string())
                .clone();

            let mut hostname = None;
            if is_host_name_required(service_name.clone()) {
                let node_name = pod
                    .spec
                    .clone()
                    .ok_or_else(|| {
                        K8sResourceError::invalid_k8s_resource_value(format!(
                            "Pod spec not found in pod {:?} resource",
                            pod.metadata.name
                        ))
                    })?
                    .node_name
                    .as_ref()
                    .ok_or_else(|| {
                        K8sResourceError::invalid_k8s_resource_value(
                            "Node name not found in running pod resource".to_string(),
                        )
                    })?
                    .clone();
                hostname = Some(
                    nodes_map
                        .get(node_name.as_str())
                        .ok_or_else(|| {
                            K8sResourceError::invalid_k8s_resource_value(format!(
                                "Unable to find node: {} object",
                                node_name.clone()
                            ))
                        })?
                        .metadata
                        .labels
                        .as_ref()
                        .ok_or_else(|| {
                            K8sResourceError::invalid_k8s_resource_value(format!(
                                "No labels found in node {}",
                                node_name.clone()
                            ))
                        })?
                        .get(KUBERNETES_HOST_LABEL_KEY)
                        .ok_or_else(|| {
                            K8sResourceError::invalid_k8s_resource_value(format!(
                                "Hostname not found for node {}",
                                node_name.clone()
                            ))
                        })?
                        .clone(),
                );
            }
            // Since pod object fetched from Kube-apiserver there will be always
            // spec associated to pod
            let containers = pod
                .spec
                .ok_or_else(|| {
                    K8sResourceError::invalid_k8s_resource_value("Pod sepc not found".to_string())
                })?
                .containers;

            for container in containers {
                logging_resources.insert(LogResource {
                    container_name: container.name,
                    host_name: hostname.clone(),
                    label_selector: format!("app={}", service_name.clone()),
                    service_type: service_name.clone(),
                });
            }
        }
        Ok(logging_resources)
    }
}

#[async_trait(?Send)]
impl Logger for LogCollection {
    // Fetch logs of requested resource and dump into files
    async fn fetch_and_dump_logs(
        &self,
        resources: HashSet<LogResource>,
        working_dir: String,
    ) -> Result<(), LogError> {
        for resource in resources.iter() {
            let service_dir = std::path::Path::new(&working_dir.clone())
                .join("logs")
                .join(resource.service_type.clone());

            create_directory_if_not_exist(service_dir.clone())?;

            if let Some(loki_client) = self.loki_client.clone() {
                if loki_client
                    .fetch_and_dump_logs(
                        resource.label_selector.clone(),
                        resource.container_name.clone(),
                        resource.host_name.clone(),
                        service_dir.clone(),
                    )
                    .await
                    .is_ok()
                {
                    continue;
                }
            }
        }
        Ok(())
    }

    async fn get_control_plane_logging_services(&self) -> Result<HashSet<LogResource>, LogError> {
        let pods = self
            .clientset
            .get_pods(LOGGING_LABEL_SELECTOR, RUNNING_FIELD_SELECTOR)
            .await?;

        let control_plane_pods = pods
            .into_iter()
            .filter(|pod| {
                let service_name = pod
                    .metadata
                    .labels
                    .as_ref()
                    .unwrap_or(&std::collections::BTreeMap::new())
                    .get("app")
                    .unwrap_or(&"".to_string())
                    .clone();
                CONTROL_PLANE_SERVICES.contains_key::<str>(&service_name)
            })
            .collect::<Vec<Pod>>();

        self.get_logging_resources(control_plane_pods).await
    }

    async fn get_data_plane_logging_services(&self) -> Result<HashSet<LogResource>, LogError> {
        let pods = self
            .clientset
            .get_pods(LOGGING_LABEL_SELECTOR, RUNNING_FIELD_SELECTOR)
            .await?;
        let data_plane_pods = pods
            .into_iter()
            .filter(|pod| {
                let service_name = pod
                    .metadata
                    .labels
                    .as_ref()
                    .unwrap_or(&std::collections::BTreeMap::new())
                    .get("app")
                    .unwrap_or(&"".to_string())
                    .clone();
                DATA_PLANE_SERVICES.contains_key::<str>(&service_name)
            })
            .collect::<Vec<Pod>>();

        self.get_logging_resources(data_plane_pods).await
    }
}

fn is_host_name_required(service_name: String) -> bool {
    HOST_NAME_REQUIRED_SERVICES.contains_key(service_name.as_str())
}

/// Creates specified directory path if not already exist
pub(crate) fn create_directory_if_not_exist(dir_path: PathBuf) -> Result<(), std::io::Error> {
    if std::fs::metadata(dir_path.clone()).is_err() {
        std::fs::create_dir_all(dir_path)?;
    }
    Ok(())
}

/// Logger contains functionality to interact with service and fetch logs for requested service
#[async_trait(?Send)]
pub(crate) trait Logger {
    async fn fetch_and_dump_logs(
        &self,
        resources: HashSet<LogResource>,
        working_dir: String,
    ) -> Result<(), LogError>;
    async fn get_data_plane_logging_services(&self) -> Result<HashSet<LogResource>, LogError>;
    async fn get_control_plane_logging_services(&self) -> Result<HashSet<LogResource>, LogError>;
}
