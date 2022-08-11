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
        rest_wrapper::RestClient,
        utils::{flush_tool_log_file, init_tool_log_file, write_to_log_file},
    },
    log,
};
use futures::future;
use std::{path::PathBuf, process};

/// SystemDumper interacts with various services to collect information like mayastor resource(s),
/// logs of mayastor service and state of mayastor artifacts in etcd
pub(crate) struct SystemDumper {
    rest_client: RestClient,
    archive: archive::Archive,
    dir_path: String,
    logger: Box<dyn Logger>,
    k8s_resource_dumper: K8sResourceDumperClient,
    etcd_dumper: Option<EtcdStore>,
}

impl SystemDumper {
    /// Instantiate new system dumper by performing following actions:
    /// 1.1 Create new archive in given directory and create temporary directory
    /// in given directory to generate dump files
    /// 1.2 Instantiate all required objects to interact with various other modules
    pub(crate) async fn get_or_panic_system_dumper(config: DumpConfig) -> Self {
        // Creates a temporary directory inside user provided directory, to store
        // artifacts. If creation is failed then we can't continue the process.
        let new_dir = match common::create_and_get_tmp_directory(config.output_directory.clone()) {
            Ok(val) => val,
            Err(e) => {
                println!(
                    "Failed to create temporary directory to dump information, error: {:?}",
                    e
                );
                process::exit(1);
            }
        };

        // Create and initialise the support tool log file
        init_tool_log_file(PathBuf::from(format!("{}/support_tool_logs.log", new_dir)))
            .expect("Support Tool Log file should be created");

        // Creates an arcive file to dump mayastor resource information. If creation
        // of archive is failed then we can't continue process
        let archive = match archive::Archive::new(Some(config.output_directory)) {
            Ok(val) => val,
            Err(err) => {
                log(format!(
                    "Failed to create archive archive, error: {:?}",
                    err
                ));
                process::exit(1);
            }
        };

        let logger = match LogCollection::new_logger(
            config.kube_config_path.clone(),
            config.namespace.clone(),
            config.loki_uri,
            config.since,
            config.timeout,
        )
        .await
        {
            Ok(val) => val,
            Err(err) => {
                log(format!(
                    "Failed to initialize logging service, error: {:?}",
                    err
                ));
                process::exit(1);
            }
        };

        let k8s_resource_dumper = match K8sResourceDumperClient::new(
            config.kube_config_path.clone(),
            config.namespace.clone(),
        )
        .await
        {
            Ok(val) => val,
            Err(err) => {
                log(format!(
                    "Failed to instantiate K8s resource dumper, error: {:?}",
                    err
                ));
                process::exit(1);
            }
        };

        let etcd_dumper = match EtcdStore::new(
            config.kube_config_path,
            config.etcd_uri,
            config.namespace,
        )
        .await
        {
            Ok(val) => Some(val),
            Err(err) => {
                log(format!(
                    "Failed to initialize etcd client, error: {:?}",
                    err
                ));
                None
            }
        };

        SystemDumper {
            rest_client: config.rest_client.clone(),
            archive,
            dir_path: new_dir,
            logger,
            k8s_resource_dumper,
            etcd_dumper,
        }
    }

    /// Dumps the state of the system
    pub(crate) async fn dump_system(&mut self) -> Result<(), Error> {
        let mut errors: Vec<Error> = Vec::new();

        log("Collecting topology information...".to_string());
        // Dump information of all volume topologies exist in the system
        match VolumeClientWrapper::new(self.rest_client.clone())
            .get_topologer(None)
            .await
        {
            Ok(topologer) => {
                log("\t Collecting volume topology information".to_string());
                let _ = topologer
                    .dump_topology_info(format!("{}/topology/volume", self.dir_path.clone()))
                    .map_err(|e| {
                        errors.push(Error::ResourceError(e));
                        log("\t Failed to dump volume topology information".to_string());
                    });
            }
            Err(e) => errors.push(Error::ResourceError(e)),
        };

        // Dump information of all pools topologies exist in the system
        match PoolClientWrapper::new(self.rest_client.clone())
            .get_topologer(None)
            .await
        {
            Ok(topologer) => {
                log("\t Collecting pool topology information".to_string());
                let _ = topologer
                    .dump_topology_info(format!("{}/topology/pool", self.dir_path.clone()))
                    .map_err(|e| {
                        log("\t Failed to dump pool topology information".to_string());
                        errors.push(Error::ResourceError(e));
                    });
            }
            Err(e) => errors.push(Error::ResourceError(e)),
        };

        let node_topologer = match NodeClientWrapper::new(self.rest_client.clone())
            .get_topologer(None)
            .await
        {
            Ok(topologer) => {
                log("\t Collecting node topology information".to_string());
                let _ = topologer
                    .dump_topology_info(format!("{}/topology/node", self.dir_path.clone()))
                    .map_err(|e| {
                        log("\t Failed to dump node topology information".to_string());
                        errors.push(Error::ResourceError(e));
                    });
                Some(topologer)
            }
            Err(e) => {
                errors.push(Error::ResourceError(e));
                None
            }
        };
        log("Completed collection of topology information".to_string());

        // Fetch required logging resources
        let mut resources = self.logger.get_control_plane_logging_services().await?;
        resources.extend(self.logger.get_data_plane_logging_services().await?);
        // NOTE: MAYASTOR-IO services will not be available when MAYASTOR-IO pod is down.
        //       Lets add information from mayastor node resources.
        if let Some(topologer) = node_topologer {
            topologer
                .get_all_resource_info()
                .iter()
                .for_each(|node_topo| {
                    resources.insert(LogResource {
                        container_name: node_topo.get_container_name(),
                        host_name: Some(node_topo.get_host_name()),
                        label_selector: node_topo.get_label_selector().as_string(','),
                        service_type: MAYASTOR_SERVICE.to_string(),
                    });
                });
        }

        let _ = write_to_log_file(format!(
            "Collecting logs of following services: \n {:#?}",
            resources
        ));

        log("Collecting logs...".to_string());
        let _ = self
            .logger
            .fetch_and_dump_logs(resources, self.dir_path.clone())
            .await
            .map_err(|e| {
                log("Error occurred while collecting logs".to_string());
                errors.push(Error::LogCollectionError(e));
            });
        log("Completed collection of logs".to_string());

        log("Collecting Kubernetes resources specific to mayastor service".to_string());
        let _ = self
            .k8s_resource_dumper
            .dump_k8s_resources(self.dir_path.clone(), None)
            .await
            .map_err(|e| {
                errors.push(Error::K8sResourceDumperError(e));
                log("Error occured while collecting logs".to_string());
            });
        log("Completed collection of Kubernetes resource specific information".to_string());

        let mut path: PathBuf = std::path::PathBuf::new();
        path.push(&self.dir_path.clone());

        let _ = future::try_join_all(self.etcd_dumper.as_mut().map(|etcd_store| {
            log("Collecting mayastor specific information from Etcd...".to_string());
            etcd_store.dump(path, false)
        }))
        .await
        .map_err(|e| {
            log(format!(
                "Failed to collect etcd dump information, error: {:?}",
                e
            ));
            errors.push(Error::EtcdDumpError(e));
        });

        Ok(())
    }

    /// Copies the temporary directory into archive and delete temporary directory
    pub fn fill_archive_and_delete_tmp(&mut self) -> Result<(), Error> {
        // Log which is visible in archive system log file
        let _ = write_to_log_file("Will copy temporary directory content to archive".to_string());
        // Flush log file before copying contents
        flush_tool_log_file()?;

        // Copy folder into archive
        self.archive
            .copy_to_archive(self.dir_path.clone(), ".".to_string())
            .map_err(|e| {
                log(format!(
                    "Failed to move content into archive file, error: {}",
                    e
                ));
                e
            })?;

        self.delete_temporary_directory().map_err(|e| {
            log(format!(
                "Failed to delete temporary directory, error: {:?}",
                e
            ));
            e
        })?;
        Ok(())
    }

    fn delete_temporary_directory(&self) -> Result<(), Error> {
        std::fs::remove_dir_all(self.dir_path.clone())?;
        Ok(())
    }
}
