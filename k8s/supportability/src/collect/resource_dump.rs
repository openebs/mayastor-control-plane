use crate::collect::{
    archive, common,
    common::{DumpConfig, Stringer},
    constants::MAYASTOR_SERVICE,
    error::Error,
    k8s_resources::k8s_resource_dump::K8sResourceDumperClient,
    logs::{LogCollection, LogResource, Logger},
    persistent_store::etcd::EtcdStore,
    resources::traits::Topologer,
};
use std::{path::PathBuf, process};

/// Dumper interacts with various services to collect information like mayastor resource(s),
/// mayastor service logs and state of mayastor artifacts and mayastor specific artifacts from
/// etcd
pub(crate) struct ResourceDumper {
    topologer: Box<dyn Topologer>,
    archive: archive::Archive,
    dir_path: String,
    logger: Box<dyn Logger>,
    k8s_resource_dumper: K8sResourceDumperClient,
    etcd_dumper: EtcdStore,
}

impl ResourceDumper {
    /// Instantiate new dumper by performing following actions:
    /// 1.1 Create new archive in given directory and create temporary directory
    /// in given directory to generate dump files
    /// 1.2 Instantiate all required objects to interact with various other modules
    pub(crate) async fn get_or_panic_resource_dumper(config: DumpConfig) -> Self {
        // creates a temporary directory inside given directory
        let new_dir = match common::create_and_get_tmp_directory(config.output_directory.clone()) {
            Ok(val) => val,
            Err(e) => {
                println!("Failed to create temporary directory, {:?}", e);
                process::exit(1);
            }
        };
        let archive = match archive::Archive::new(config.output_directory) {
            Ok(val) => val,
            Err(err) => {
                println!("Failed to create archive, {:?}", err);
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

        ResourceDumper {
            topologer: config.topologer.unwrap(),
            archive,
            dir_path: new_dir,
            logger,
            k8s_resource_dumper,
            etcd_dumper,
        }
    }

    /// Dumps information associated to given resource(s)
    pub(crate) async fn dump_info(&mut self, folder_path: String) -> Result<(), Error> {
        self.topologer
            .dump_topology_info(format!("{}/{}", self.dir_path.clone(), folder_path))?;

        // Fetch dataplane resources associated to Unhealthy resources
        // TODO: Check with team whether we have to collect data from all associated
        //       (or) only from offline associated resources?
        let unhealthy_resources = self.topologer.get_unhealthy_resource_info();
        let mut resources = self.logger.get_control_plane_logging_services().await?;
        unhealthy_resources.into_iter().for_each(|resource| {
            resources.insert(LogResource {
                container_name: resource.get_container_name(),
                label_selector: resource.get_label_selector().as_string(','),
                host_name: Some(resource.get_host_name()),
                service_type: MAYASTOR_SERVICE.to_string(),
            });
        });

        println!("Collecting logs from following services: {:#?}", resources);
        self.logger
            .fetch_and_dump_logs(resources, self.dir_path.clone())
            .await?;

        // Collect mayastor & kubernetes associated resources
        self.k8s_resource_dumper
            .dump_k8s_resources(
                self.dir_path.clone(),
                Some(self.topologer.get_k8s_resource_names()),
            )
            .await?;

        // Collect ETCD dump specific to mayastor
        let mut path: PathBuf = std::path::PathBuf::new();
        path.push(&self.dir_path.clone());
        self.etcd_dumper.dump(path).await?;

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
