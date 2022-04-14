use crate::{
    collect::{
        archive, common,
        common::{DumpConfig, Stringer},
        constants::MAYASTOR_SERVICE,
        error::Error,
        k8s_resources::k8s_resource_dump::K8sResourceDumperClient,
        logs::{LogCollection, LogResource, Logger},
        persistent_store::etcd::EtcdStore,
        resources::{
            node::NodeClientWrapper, pool::PoolClientWrapper, volume::VolumeClientWrapper,
            Resourcer,
        },
        rest_wrapper::rest_wrapper_client::RestClient,
        utils::init_tool_log_file,
    },
    log,
};
use std::{path::PathBuf, process};

/// SystemDumper interacts with various services to collect information like mayastor resource(s),
/// logs of mayastor service and state of mayastor artifacts in etcd
pub(crate) struct SystemDumper {
    rest_client: &'static RestClient,
    archive: archive::Archive,
    dir_path: String,
    logger: Box<dyn Logger>,
    k8s_resource_dumper: K8sResourceDumperClient,
    etcd_dumper: EtcdStore,
}

impl SystemDumper {
    /// Instantiate new system dumper by performing following actions:
    /// 1.1 Create new archive in given directory and create temporary directory
    /// in given directory to generate dump files
    /// 1.2 Instantiate all required objects to interact with various other modules
    pub(crate) async fn get_or_panic_system_dumper(config: DumpConfig) -> Self {
        // creates a temporary directory inside given directory
        let new_dir = match common::create_and_get_tmp_directory(config.output_directory.clone()) {
            Ok(val) => val,
            Err(e) => {
                println!("Failed to create temporary, {:?}", e);
                process::exit(1);
            }
        };

        // Create and initialise the support tool log file
        init_tool_log_file(PathBuf::from(format!("{}/support_tool_logs.log", new_dir)))
            .expect("Support Tool Log file should be created.");

        let archive = match archive::Archive::new(config.output_directory) {
            Ok(val) => val,
            Err(err) => {
                log(format!("Failed to create archive, {:?}", err))
                    .expect("Should be able to write to Tool Log File");
                process::exit(1);
            }
        };

        let logger = LogCollection::new_logger(
            config.kube_config_path.clone(),
            config.namespace.clone(),
            config.loki_uri,
            config.since,
            config.timeout,
        )
        .await
        .expect("Failed to initialize logging service");

        let k8s_resource_dumper =
            K8sResourceDumperClient::new(config.kube_config_path.clone(), config.namespace.clone())
                .await
                .expect("Failed to instantiate the k8s resource dumper client");

        let etcd_dumper =
            EtcdStore::new(config.kube_config_path, config.etcd_uri, config.namespace)
                .await
                .expect("Failed to initialize etcd service");

        SystemDumper {
            rest_client: config.rest_client,
            archive,
            dir_path: new_dir,
            logger,
            k8s_resource_dumper,
            etcd_dumper,
        }
    }

    /// Dumps the state of the system
    pub(crate) async fn dump_system(&mut self) -> Result<(), Error> {
        // Dump information of all volume topologies exist in the system
        let volume_topologer = VolumeClientWrapper::new(self.rest_client)
            .get_topologer(None)
            .await?;
        volume_topologer
            .dump_topology_info(format!("{}/topology/volume", self.dir_path.clone()))?;

        // Dump information of all pools topologies exist in the system
        let pool_topologer = PoolClientWrapper::new(self.rest_client)
            .get_topologer(None)
            .await?;
        pool_topologer.dump_topology_info(format!("{}/topology/pool", self.dir_path.clone()))?;

        let node_topologer = NodeClientWrapper::new(self.rest_client)
            .get_topologer(None)
            .await?;
        node_topologer.dump_topology_info(format!("{}/topology/node", self.dir_path.clone()))?;

        // Fetch required logging resources
        let mut resources = self.logger.get_control_plane_logging_services().await?;
        resources.extend(self.logger.get_data_plane_logging_services().await?);
        // NOTE: MAYASTOR-IO services will not be available when MAYASTOR-IO pod is down.
        //       Lets add information from mayastor node resources.
        let node_topologer_resources = node_topologer.get_all_resource_info();
        node_topologer_resources.iter().for_each(|topo_resource| {
            resources.insert(LogResource {
                container_name: topo_resource.get_container_name(),
                label_selector: topo_resource.get_label_selector().as_string(','),
                host_name: Some(topo_resource.get_host_name()),
                service_type: MAYASTOR_SERVICE.to_string(),
            });
        });

        log(format!(
            "Collecting logs of following services: \n {:#?}",
            resources
        ))?;

        self.logger
            .fetch_and_dump_logs(resources, self.dir_path.clone())
            .await?;

        self.k8s_resource_dumper
            .dump_k8s_resources(self.dir_path.clone(), None)
            .await?;

        let mut path: PathBuf = std::path::PathBuf::new();
        path.push(&self.dir_path.clone());
        self.etcd_dumper.dump(path).await?;

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
