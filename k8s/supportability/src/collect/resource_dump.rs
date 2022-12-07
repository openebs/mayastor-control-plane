use crate::{
    collect::{
        archive, common,
        common::{DumpConfig, Stringer},
        constants::MAYASTOR_SERVICE,
        error::Error,
        k8s_resources::k8s_resource_dump::K8sResourceDumperClient,
        logs::{LogCollection, LogResource, Logger},
        persistent_store::{etcd::EtcdStore, EtcdError},
        resources::traits::Topologer,
        utils::{flush_tool_log_file, init_tool_log_file, write_to_log_file},
    },
    log, OutputFormat,
};
use futures::future;
use std::{path::PathBuf, process};

/// Dumper interacts with various services to collect information like mayastor resource(s),
/// mayastor service logs and state of mayastor artifacts and mayastor specific artifacts from
/// etcd
pub(crate) struct ResourceDumper {
    topologer: Option<Box<dyn Topologer>>,
    archive: archive::Archive,
    dir_path: String,
    logger: Box<dyn Logger>,
    k8s_resource_dumper: K8sResourceDumperClient,
    etcd_dumper: Option<EtcdStore>,
    output_format: OutputFormat,
}

impl ResourceDumper {
    /// Instantiate new dumper by performing following actions:
    /// 1.1 Create new archive in given directory and create temporary directory
    /// in given directory to generate dump files
    /// 1.2 Instantiate all required objects to interact with various other modules
    pub(crate) async fn get_or_panic_resource_dumper(config: DumpConfig) -> Self {
        // creates a temporary directory inside given directory
        let (new_dir, output_directory) = match config.output_format {
            OutputFormat::Tar => {
                let new_dir =
                    match common::create_and_get_tmp_directory(config.output_directory.clone()) {
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
                if let Err(e) =
                    init_tool_log_file(PathBuf::from(format!("{}/support_tool_logs.log", new_dir)))
                {
                    println!("Encountered error while creating log file: {} ", e);
                    process::exit(1);
                }

                (new_dir, Some(config.output_directory))
            }
            OutputFormat::Stdout => ("".into(), None),
        };

        let archive = match archive::Archive::new(output_directory) {
            Ok(val) => val,
            Err(err) => {
                log(format!("Failed to create archive, {:?}", err));
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

        ResourceDumper {
            topologer: config.topologer,
            archive,
            dir_path: new_dir,
            logger,
            k8s_resource_dumper,
            etcd_dumper,
            output_format: config.output_format,
        }
    }

    /// Dumps information associated to given resource(s)
    pub(crate) async fn dump_info(&mut self, folder_path: String) -> Result<(), Error> {
        let mut errors = Vec::new();
        let mut resources = match self.logger.get_control_plane_logging_services().await {
            Ok(list) => list,
            Err(e) => {
                log(format!(
                    "Failed to fetch control plane services, error: {:?}",
                    e
                ));
                errors.push(Error::LogCollectionError(e));
                std::collections::HashSet::new()
            }
        };
        let mut k8s_resources: Option<Vec<String>> = None;

        log("Collecting topology information of resource(s)...".to_string());
        if let Some(topologer) = self.topologer.as_ref() {
            let _igonre = topologer
                .dump_topology_info(format!("{}/{}", self.dir_path.clone(), folder_path))
                .map_err(|e| {
                    log(format!(
                        "Failed to collect topology information, error: {:?}",
                        e
                    ));
                    errors.push(Error::ResourceError(e));
                });

            // Fetch dataplane resources associated to Unhealthy resources
            // TODO: Check with team whether we have to collect data from all associated
            //       (or) only from offline associated resources?
            let unhealthy_resources = topologer.get_unhealthy_resource_info();
            unhealthy_resources.into_iter().for_each(|resource| {
                resources.insert(LogResource {
                    container_name: resource.get_container_name(),
                    label_selector: resource.get_label_selector().as_string(','),
                    host_name: Some(resource.get_host_name()),
                    service_type: MAYASTOR_SERVICE.to_string(),
                });
            });
            k8s_resources = Some(topologer.get_k8s_resource_names());
        }
        log("Completed collection of topology information".to_string());

        let _ = write_to_log_file(format!(
            "Collecting logs from following services: {:#?}",
            resources
        ));
        log("Collecting logs...".to_string());
        let _ = self
            .logger
            .fetch_and_dump_logs(resources, self.dir_path.clone())
            .await
            .map_err(|e| errors.push(Error::LogCollectionError(e)));
        log("Completed collection of logs".to_string());

        // Collect mayastor & kubernetes associated resources
        log("Collecting Kubernetes resources specific to mayastor service".to_string());
        let _ = self
            .k8s_resource_dumper
            .dump_k8s_resources(self.dir_path.clone(), k8s_resources)
            .await
            .map_err(|e| errors.push(Error::K8sResourceDumperError(e)));
        log("Completed collection of Kubernetes resource specific information".to_string());

        let mut path: PathBuf = std::path::PathBuf::new();
        path.push(&self.dir_path.clone());

        // Collect ETCD dump specific to mayastor
        log("Collecting mayastor specific information from Etcd...".to_string());
        let _ = future::try_join_all(
            self.etcd_dumper
                .as_mut()
                .map(|etcd_store| etcd_store.dump(path, false)),
        )
        .await
        .map_err(|e| {
            log(format!(
                "Failed to collect etcd dump information, error: {:?}",
                e
            ));
            errors.push(Error::EtcdDumpError(e));
        });
        log("Completed collection of mayastor specific resources from Etcd service".to_string());

        let _ = self
            .archive
            .copy_to_archive(self.dir_path.clone(), ".".to_string())
            .map_err(|e| {
                log(format!(
                    "Failed to move content into archive file, error: {}",
                    e
                ));
                errors.push(Error::ArchiveError(e));
            });

        let _ = self.delete_temporary_directory().map_err(|e| {
            log(format!(
                "Failed to delete temporary directory, error: {:?}",
                e
            ));
        });

        if !errors.is_empty() {
            return Err(Error::MultipleErrors(errors));
        }

        Ok(())
    }

    /// Dumps information associated to given resource(s)
    pub(crate) async fn dump_etcd(&mut self) -> Result<(), Error> {
        let mut path: PathBuf = std::path::PathBuf::new();
        path.push(&self.dir_path.clone());

        self.etcd_dumper
            .as_mut()
            .ok_or_else(|| EtcdError::Custom("etcd not configured".into()))?
            .dump(path, matches!(self.output_format, OutputFormat::Stdout))
            .await
            .map_err(|e| {
                log(format!(
                    "Failed to collect etcd dump information, error: {:?}",
                    e
                ));
                e
            })?;
        log("Completed collection of etcd dump information".to_string());

        if matches!(self.output_format, OutputFormat::Tar) {
            self.archive
                .copy_to_archive(self.dir_path.clone(), ".".to_string())
                .map_err(|e| {
                    log(format!(
                        "Failed to move content into archive file, error: {}",
                        e
                    ));
                    e
                })?;

            let _ = self.delete_temporary_directory().map_err(|e| {
                log(format!(
                    "Failed to delete temporary directory, error: {:?}",
                    e
                ));
            });
        }
        Ok(())
    }

    /// Copies the temporary directory content into archive and delete temporary directory
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
