use crate::{
    collect::{
        k8s_resources::{
            client::{ClientSet, K8sResourceError},
            common::{NODE_NAME_FIELD_SELECTOR, RUNNING_FIELD_SELECTOR},
        },
        logs::create_directory_if_not_exist,
    },
    log,
};
use futures::StreamExt;
use k8s_openapi::api::core::v1::Pod;
use kube::{api::LogParams, Error, Resource};
use std::{collections::HashMap, fs::File, io::Write, path::PathBuf};

/// Possible errors can occur while interacting with K8s for logs, and file creations
#[derive(Debug)]
pub(crate) enum K8sLoggerError {
    K8sResourceError(K8sResourceError),
    IOError(std::io::Error),
}

impl K8sLoggerError {
    pub fn invalid_k8s_resource_value(err: String) -> Self {
        Self::K8sResourceError(K8sResourceError::CustomError(err))
    }
}

impl From<std::io::Error> for K8sLoggerError {
    fn from(e: std::io::Error) -> K8sLoggerError {
        K8sLoggerError::IOError(e)
    }
}

impl From<K8sResourceError> for K8sLoggerError {
    fn from(e: K8sResourceError) -> K8sLoggerError {
        K8sLoggerError::K8sResourceError(e)
    }
}

impl From<kube::Error> for K8sLoggerError {
    fn from(e: Error) -> Self {
        K8sLoggerError::K8sResourceError(K8sResourceError::ClientError(e))
    }
}

/// K8sLoggerClient to be used for fetching k8s logs on central log management's failure
#[derive(Clone)]
pub(crate) struct K8sLoggerClient {
    k8s_client: ClientSet,
}

/// No of times we want to retry fetching from logs stream
pub(crate) const MAX_POLLING_RETRIES: u8 = 2;

impl K8sLoggerClient {
    /// Create a new ClientSet, with the config if provided, otherwise with default.
    pub(crate) fn new(k8s_client: ClientSet) -> Self {
        Self { k8s_client }
    }

    /// get the k8s client
    pub(crate) fn get_k8s_clientset(&self) -> ClientSet {
        self.k8s_client.clone()
    }

    /// Dumper function that creates the component level dump.
    /// It takes the component directory, and creates subfolders based on hostname and pods,
    /// adds the previous logs(if any), and current container logs in individual files.
    /// param `label_selector` --> to select particular component log, ex(app=io-engine)
    /// param `service_dir` --> to add the created pod directories and log files into this directory
    /// ex (logs/io-engine/pod1/cont1.log)
    /// param `hostname` --> if specified only those node's pods will be
    /// filtered out, if not specified all nodes would be considered.
    /// param `containers` --> to specify exactly which container logs we want
    pub(crate) async fn dump_pod_logs(
        &self,
        label_selector: &str,
        service_dir: PathBuf,
        hostname: Option<String>,
        containers: &[&str],
    ) -> Result<(), K8sLoggerError> {
        let field_selector = match hostname {
            None => RUNNING_FIELD_SELECTOR.to_string(),
            Some(name) => {
                let node_name = self.k8s_client.get_nodename(name.as_str()).await?;
                format!(
                    "{},{}={}",
                    RUNNING_FIELD_SELECTOR, NODE_NAME_FIELD_SELECTOR, node_name
                )
            }
        };

        let pods = self
            .k8s_client
            .get_pods(label_selector, field_selector.as_str())
            .await?;

        for pod in pods {
            match self
                .create_pod_logs(&pod, containers, service_dir.clone())
                .await
            {
                Ok(()) => {}
                Err(err) => {
                    log(format!(
                        "Error fetching logs for pod : {}, error: {:?}",
                        pod.meta().name.as_ref().unwrap_or(&"".to_string()),
                        err
                    ));
                    continue;
                }
            }
        }
        Ok(())
    }

    /// Creates the pod level log dumps, i.e would create list of directories of name
    /// hostname_pod_name with specified container log files inside.
    async fn create_pod_logs(
        &self,
        pod: &Pod,
        containers: &[&str],
        service_dir: PathBuf,
    ) -> Result<(), K8sLoggerError> {
        let mut pod_dir = service_dir.clone();

        let pod_name = pod.meta().name.as_ref().ok_or_else(|| {
            K8sLoggerError::invalid_k8s_resource_value("pod.name is invalid".to_string())
        })?;

        let node_name = pod
            .spec
            .as_ref()
            .ok_or_else(|| {
                K8sLoggerError::invalid_k8s_resource_value("pod.spec is invalid".to_string())
            })?
            .node_name
            .as_ref()
            .ok_or_else(|| {
                K8sLoggerError::invalid_k8s_resource_value(
                    "pod.spec.node_name is invalid".to_string(),
                )
            })?
            .as_str();

        let host_name = self.k8s_client.get_hostname(node_name).await?;

        pod_dir.push(format!("{}_{}", host_name, pod_name.clone()));
        create_directory_if_not_exist(pod_dir.clone())?;

        let mut container_restart_map: HashMap<String, bool> = HashMap::new();

        match pod.status.as_ref() {
            None => {}
            Some(status) => match status.container_statuses.as_ref() {
                None => {}
                Some(container_statuses) => {
                    for container_status in container_statuses {
                        container_restart_map.insert(
                            container_status.name.to_string(),
                            container_status.restart_count > 0,
                        );
                    }
                }
            },
        };

        for container in containers {
            let container_name = container.to_string();

            if *container_restart_map
                .get(container_name.as_str())
                .unwrap_or(&false)
            {
                self.create_container_log_file(
                    true,
                    pod_name.clone(),
                    container_name.clone(),
                    pod_dir.clone(),
                )
                .await?;
            }

            self.create_container_log_file(
                false,
                pod_name.clone(),
                container_name,
                pod_dir.clone(),
            )
            .await?;
        }

        Ok(())
    }

    /// Creates the container level log file. We can define whether we need previous logs or not.
    async fn create_container_log_file(
        &self,
        pod_restarted: bool,
        pod_name: String,
        container_name: String,
        pod_dir: PathBuf,
    ) -> Result<(), K8sLoggerError> {
        let mut container_file = pod_dir;
        if pod_restarted {
            container_file.push(format!("previous_{}.log", container_name));
        } else {
            container_file.push(format!("{}.log", container_name));
        }

        let log_file = File::create(container_file)?;

        let client_set = self.clone();

        client_set
            .write_logs_stream(
                pod_name.clone().as_str(),
                container_name.clone().as_str(),
                log_file.try_clone().unwrap(),
                pod_restarted,
            )
            .await?;
        Ok(())
    }

    /// fetches the logs stream from the kube-api-server and writes them to specified file.
    async fn write_logs_stream<W: Write>(
        &self,
        pod_name: &str,
        container_name: &str,
        mut writer: W,
        previous_logs: bool,
    ) -> Result<(), K8sLoggerError> {
        let log_params = LogParams {
            container: Some(container_name.to_string()),
            previous: previous_logs,
            ..Default::default()
        };

        let mut log_stream = self
            .k8s_client
            .get_pod_api()
            .await
            .log_stream(pod_name, &log_params)
            .await?;

        let mut max_retries = 0;
        while let Some(result_data) = log_stream.next().await {
            match result_data {
                Ok(data) => writer.write_all(&data)?,
                Err(err) => {
                    if max_retries > MAX_POLLING_RETRIES {
                        writer.flush()?;
                        return Err(K8sLoggerError::K8sResourceError(
                            K8sResourceError::ResourceError(Box::new(err)),
                        ));
                    }
                    max_retries += 1;
                }
            }
        }

        writer.flush()?;

        Ok(())
    }
}
