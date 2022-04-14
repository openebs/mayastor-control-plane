use crate::{
    collect::{
        k8s_resources::client::{ClientSet, K8sResourceError},
        logs::create_directory_if_not_exist,
    },
    log,
};
use k8s_openapi::{
    api::{apps::v1, core::v1::Event},
    apimachinery::pkg::apis::meta::v1::MicroTime,
};
use k8s_operators::diskpool::crd::DiskPool;
use kube::Resource;
use serde::Serialize;
use std::{collections::HashSet, fs::File, io::Write, iter::FromIterator, path::PathBuf};

/// K8s resource dumper client
#[derive(Clone)]
pub(crate) struct K8sResourceDumperClient {
    k8s_client: ClientSet,
}

/// Errors pertaining to k8s resource dumper module
#[derive(Debug)]
#[allow(clippy::enum_variant_names)]
pub(crate) enum K8sResourceDumperError {
    K8sResourceError(K8sResourceError),
    IOError(std::io::Error),
    YamlSerializationError(serde_yaml::Error),
    JsonSerializationError(serde_json::Error),
}

impl From<std::io::Error> for K8sResourceDumperError {
    fn from(e: std::io::Error) -> K8sResourceDumperError {
        K8sResourceDumperError::IOError(e)
    }
}

impl From<serde_yaml::Error> for K8sResourceDumperError {
    fn from(e: serde_yaml::Error) -> K8sResourceDumperError {
        K8sResourceDumperError::YamlSerializationError(e)
    }
}

impl From<serde_json::Error> for K8sResourceDumperError {
    fn from(e: serde_json::Error) -> K8sResourceDumperError {
        K8sResourceDumperError::JsonSerializationError(e)
    }
}

impl From<K8sResourceError> for K8sResourceDumperError {
    fn from(e: K8sResourceError) -> K8sResourceDumperError {
        K8sResourceDumperError::K8sResourceError(e)
    }
}

/// Newtype to wrap k8s DaemonSet
#[derive(Serialize)]
pub(crate) struct DaemonSet(v1::DaemonSet);
/// Newtype to wrap k8s Deployment
#[derive(Serialize)]
pub(crate) struct Deployment(v1::Deployment);
/// Newtype to wrap k8s StatefulSet
#[derive(Serialize)]
pub(crate) struct StatefulSet(v1::StatefulSet);

/// Trait to get the entity names
pub(crate) trait EntityName: Serialize {
    fn name(&self) -> String;
}

impl EntityName for DaemonSet {
    fn name(&self) -> String {
        self.0.metadata.name.as_ref().unwrap().to_string()
    }
}

impl EntityName for Deployment {
    fn name(&self) -> String {
        self.0.metadata.name.as_ref().unwrap().to_string()
    }
}

impl EntityName for StatefulSet {
    fn name(&self) -> String {
        self.0.metadata.name.as_ref().unwrap().to_string()
    }
}

impl K8sResourceDumperClient {
    /// get a new k8s resource dumper client
    pub(crate) async fn new(
        kube_config_path: Option<std::path::PathBuf>,
        namespace: String,
    ) -> Result<Self, K8sResourceDumperError> {
        let k8s_client = ClientSet::new(kube_config_path, namespace).await?;
        Ok(Self { k8s_client })
    }

    /// dump the kubernetes resources like deployments, daemonsets,
    /// pods, statefulsets, events, disk pools in the given root path
    pub(crate) async fn dump_k8s_resources(
        &self,
        root_path: String,
        required_pools: Option<Vec<String>>,
    ) -> Result<(), K8sResourceDumperError> {
        // Create the root dir path
        let mut root_dir = PathBuf::from(root_path);
        root_dir.push("k8s_resources");
        create_directory_if_not_exist(root_dir.clone())?;

        // Create the configurations path
        let mut configurations_path = root_dir.clone();
        configurations_path.push("configurations");
        // Create the configurations directory
        create_directory_if_not_exist(configurations_path.clone())?;

        // Fetch all Daemonsets in provided NAMESPACE
        let daemonsets = self.k8s_client.get_daemonsets("", "").await?;
        // Create all Daemonsets configurations
        create_app_configurations(
            daemonsets.into_iter().map(DaemonSet).collect(),
            configurations_path.clone(),
        )?;

        // Fetch all Deployments in provided NAMESPACE
        let deploys = self.k8s_client.get_deployments("", "").await?;
        // Create all Daemonsets configurations
        create_app_configurations(
            deploys.into_iter().map(Deployment).collect(),
            configurations_path.clone(),
        )?;

        // Fetch all StatefulSets in provided NAMESPACE
        let statefulsets = self.k8s_client.get_statefulsets("", "").await?;
        // Create all Daemonsets configurations
        create_app_configurations(
            statefulsets.into_iter().map(StatefulSet).collect(),
            configurations_path.clone(),
        )?;

        // Fetch all events in provided NAMESPACE
        let mut events = self.k8s_client.get_events("", "").await?;
        // Sort the events based on event_time
        events.sort_unstable_by_key(event_time);
        create_file_and_write(
            root_dir.clone(),
            "k8s_events.json".to_string(),
            serde_json::to_string_pretty(&events)?,
        )?;

        // Fetch all DiskPools in provided NAMESPACE
        let disk_pools = self.k8s_client.list_pools(None, None).await?;
        let filtered_pools = match required_pools {
            Some(p_names) => {
                let names: HashSet<String> = HashSet::from_iter(p_names);
                disk_pools
                    .into_iter()
                    .filter(|p| names.contains(p.meta().name.as_ref().unwrap()))
                    .collect::<Vec<DiskPool>>()
            }
            None => disk_pools,
        };
        create_file_and_write(
            root_dir.clone(),
            "k8s_disk_pools.yaml".to_string(),
            serde_yaml::to_string(&filtered_pools)?,
        )?;

        // Fetch all Pods in provided NAMESPACE
        let pods = self.k8s_client.get_pods("", "").await?;
        create_file_and_write(
            root_dir.clone(),
            "pods.yaml".to_string(),
            serde_yaml::to_string(&pods)?,
        )?;
        Ok(())
    }
}

/// Creates a file and writes the passed content in it
fn create_file_and_write(
    mut file_path: PathBuf,
    file_name: String,
    content: String,
) -> Result<(), std::io::Error> {
    file_path.push(file_name);
    let mut file = File::create(file_path)?;
    file.write_all(content.as_bytes())?;
    file.flush().unwrap();
    Ok(())
}

/// create the app specific yamls
fn create_app_configurations<T: EntityName>(
    apps: Vec<T>,
    dir_path: PathBuf,
) -> Result<(), K8sResourceDumperError> {
    for app in apps {
        let serialized = match serde_yaml::to_string(&app) {
            Ok(value) => value,
            Err(e) => {
                log(format!(
                    "Error serializing the app : {} , error: {}",
                    app.name(),
                    e
                ))?;
                continue;
            }
        };
        match create_file_and_write(dir_path.clone(), format!("{}.yaml", app.name()), serialized) {
            Ok(_) => {}
            Err(e) => {
                log(format!(
                    "Error creating or writing file for the app : {} , error: {}",
                    app.name(),
                    e
                ))?;
                continue;
            }
        }
    }
    Ok(())
}

/// kubectl's way of ensuring we always have a time to be used for sorting
/// ref: https://github.com/kubernetes/kubectl/blob/f0ce177e80077eb167dd17febe4b9a6c157c5684/pkg/cmd/events/events.go#L294-L319
fn event_time(event: &Event) -> MicroTime {
    if event.series.is_some() {
        return event
            .series
            .as_ref()
            .unwrap()
            .last_observed_time
            .as_ref()
            .unwrap()
            .clone();
    }
    if event.last_timestamp.is_some() {
        return MicroTime(event.last_timestamp.as_ref().unwrap().0);
    }
    event.event_time.as_ref().unwrap().clone()
}
