// use crate::collect::k8s_resources::client::ClientSet;
use crate::collect::{
    archive,
    error::Error,
    k8s_resources::k8s_resource_dump::K8sResourceDumperClient,
    logs::{LogCollection, Logger},
    resources::{
        node::NodeClientWrapper, pool::PoolClientWrapper, volume::VolumeClientWrapper, Resourcer,
    },
    rest_wrapper::rest_wrapper_client::RestClient,
};
use chrono::Local;
use std::process;

/// SystemDumper interacts with various services to collect information,
/// logs of mayastor service and state of mayastor artifacts in etcd
pub(crate) struct SystemDumper {
    rest_client: &'static RestClient,
    archive: archive::Archive,
    dir_path: String,
    logger: Box<dyn Logger>,
    k8s_resource_dumper: K8sResourceDumperClient,
}

impl SystemDumper {
    /// Instantiate new system dumper by performing following actions:
    /// 1.1 Create new archive in given directory and create temporary directory
    /// in given directory to generate dump files
    /// 1.2 Instantiate all required objects to interact with various other modules
    pub(crate) async fn get_or_panic_system_dumper(
        rest_client: &'static RestClient,
        dir_path: String,
        namespace: String,
        loki_uri: Option<String>,
        since: humantime::Duration,
        kube_config_path: Option<std::path::PathBuf>,
        timeout: humantime::Duration,
    ) -> Self {
        // creates a temporary directory inside given directory
        let new_dir = match create_and_get_directory_path(dir_path.clone()) {
            Ok(val) => val,
            Err(e) => {
                println!("Failed to create temporary, {:?}", e);
                process::exit(1);
            }
        };
        let archive = match archive::Archive::new(dir_path) {
            Ok(val) => val,
            Err(err) => {
                println!("Failed to create archive, {:?}", err);
                process::exit(1);
            }
        };

        let logger = LogCollection::new_logger(
            kube_config_path.clone(),
            namespace.clone(),
            loki_uri,
            since,
            timeout,
        )
        .await
        .expect("Failed to initialize logging service");

        let k8s_resource_dumper = K8sResourceDumperClient::new(kube_config_path, namespace)
            .await
            .expect("Failed to instantiate the k8s resource dumper client");

        SystemDumper {
            rest_client,
            archive,
            dir_path: new_dir,
            logger,
            k8s_resource_dumper,
        }
    }

    /// Dumps the state of the system
    pub(crate) async fn dump_system(&mut self) -> Result<(), Error> {
        // Dump information of all volume topologies exist in the system
        let volume_topologer = VolumeClientWrapper::new(self.rest_client)
            .get_topologer(None)
            .await?;
        volume_topologer.dump_topology_info(self.dir_path.clone())?;

        // Dump information of all pools topologies exist in the system
        let pool_topologer = PoolClientWrapper::new(self.rest_client)
            .get_topologer(None)
            .await?;
        pool_topologer.dump_topology_info(self.dir_path.clone())?;

        let node_topologer = NodeClientWrapper::new(self.rest_client)
            .get_topologer(None)
            .await?;
        node_topologer.dump_topology_info(self.dir_path.clone())?;

        // Fetch required logging resources
        let mut resources = self.logger.get_control_plane_logging_services().await?;
        resources.extend(self.logger.get_data_plane_logging_services().await?);
        println!("Collecting logs of following services: \n {:#?}", resources);

        self.logger
            .fetch_and_dump_logs(resources, self.dir_path.clone())
            .await?;

        self.k8s_resource_dumper
            .dump_k8s_resources(self.dir_path.clone())
            .await?;

        // Copy folder into archive
        self.archive
            .copy_to_archive(self.dir_path.clone(), ".".to_string())?;

        let _ignore = self.delete_temporary_directory();
        Ok(())
    }

    fn delete_temporary_directory(&self) -> Result<(), Error> {
        std::fs::remove_dir_all(self.dir_path.clone())?;
        Ok(())
    }
}

// Creates new temporary directory to store mayastor artifacts in given directory
fn create_and_get_directory_path(dir_path: String) -> Result<String, Error> {
    let date = Local::now();
    let suffix_dir_name = format!("tmp-mayastor-{}", date.format("%Y-%m-%d-%H-%M-%S"));
    let new_dir_path = std::path::Path::new(&dir_path).join(suffix_dir_name);
    std::fs::create_dir_all(new_dir_path.clone())?;
    Ok(new_dir_path.into_os_string().into_string().unwrap())
}
