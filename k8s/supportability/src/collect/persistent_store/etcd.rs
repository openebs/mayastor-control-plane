use crate::collect::{
    constants::{ETCD_SERVICE_LABEL_SELECTOR, ETCD_SERVICE_PORT_NAME},
    k8s_resources::client::ClientSet,
    persistent_store::EtcdError,
};
use common_lib::{store::etcd, types::v0::store::definitions::Store};
use std::{io::Write, path::PathBuf};

/// EtcdStore is used to abstract connection to etcd database for dumping the contents
pub(crate) struct EtcdStore {
    etcd: etcd::Etcd,
    key_prefix: String,
}

impl EtcdStore {
    /// Create a new etcd store client using the given endpoint or the kubeconfig path.
    /// The provided namespace will be used to search for etcd service, if the
    /// etcd point is not provided
    pub(crate) async fn new(
        kube_config_path: Option<std::path::PathBuf>,
        etcd_endpoint: Option<String>,
        namespace: String,
    ) -> Result<Self, EtcdError> {
        let client_set = ClientSet::new(kube_config_path, namespace.clone()).await?;
        let platform_info = common_lib::platform::k8s::K8s::from(client_set.kube_client())
            .await
            .map_err(|e| EtcdError::Custom(format!("Failed to get k8s platform info: {}", e)))?;
        let key_prefix = common_lib::store::etcd::build_key_prefix(platform_info, namespace);

        // if an endpoint is provided it will be used, else the kubeconfig path will be used
        // to find the endpoint of the headless etcd service
        let endpoint = if let Some(endpoint) = etcd_endpoint {
            endpoint
        } else {
            let e = client_set
                .get_service_endpoint(ETCD_SERVICE_LABEL_SELECTOR, ETCD_SERVICE_PORT_NAME)
                .await?;
            e.to_string()
        };
        let etcd = etcd::Etcd::new(endpoint.as_str()).await?;

        Ok(Self { etcd, key_prefix })
    }

    /// dump all the data from etcd in the selected namespace into a file in
    /// the given working directory.
    pub(crate) async fn dump(&mut self, working_dir: PathBuf) -> Result<(), EtcdError> {
        let prefix = self.key_prefix.as_str();
        let mut dump = self.etcd.get_values_prefix(prefix).await?;
        let file_name = "etcd_dump".to_string();
        let file_path = working_dir.join(file_name);
        let mut etcd_dump_file = std::fs::File::create(file_path)?;
        for val in dump.iter_mut() {
            // unwrap or default because we dont want the code to panic in case of errors. need to
            // write all data to file even if parsing of one value fails.
            let pretty_json = serde_json::to_string_pretty(&val.1).unwrap_or_default();
            write!(etcd_dump_file, "{}:\n{}\n\n", val.0, pretty_json)?;
        }
        etcd_dump_file.flush()?;
        Ok(())
    }
}
