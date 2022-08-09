use crate::collect::{
    constants::ETCD_PAGED_LIMIT, k8s_resources::client::ClientSet, persistent_store::EtcdError,
};
use common_lib::{store::etcd, types::v0::store::definitions::Store};
use std::{io::Write, path::PathBuf};

/// EtcdStore is used to abstract connection to etcd database for dumping the contents
#[derive(Clone)]
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
        let client_set = ClientSet::new(kube_config_path.clone(), namespace.clone()).await?;
        let platform_info = common_lib::platform::k8s::K8s::from(client_set.kube_client())
            .await
            .map_err(|e| EtcdError::Custom(format!("Failed to get k8s platform info: {}", e)))?;
        let key_prefix =
            common_lib::store::etcd::build_key_prefix(platform_info, namespace.clone());

        // if an endpoint is provided it will be used, else the kubeconfig path will be used
        // to find the endpoint of the headless etcd service
        let endpoint = if let Some(endpoint) = etcd_endpoint {
            endpoint
        } else {
            let uri = kube_proxy::ConfigBuilder::default_etcd()
                .with_kube_config(kube_config_path)
                .with_target_mod(|t| t.with_namespace(namespace))
                .build()
                .await
                .map_err(EtcdError::CreateClient)?;

            uri.to_string()
        };
        let etcd = etcd::Etcd::new(endpoint.as_str()).await?;

        Ok(Self { etcd, key_prefix })
    }

    /// dump all the data from etcd in the selected namespace into a file in
    /// the given working directory.
    pub(crate) async fn dump(
        &mut self,
        working_dir: PathBuf,
        stdout: bool,
    ) -> Result<(), EtcdError> {
        let mut prefix = &self.key_prefix;

        let mut etcd_dump_file = match stdout {
            false => {
                let file_name = "etcd_dump".to_string();
                let file_path = working_dir.join(file_name);
                Some(std::fs::File::create(file_path)?)
            }
            true => None,
        };

        let mut first = true;
        let mut dump;
        loop {
            dump = self.etcd.get_values_paged(prefix, ETCD_PAGED_LIMIT).await?;
            if !first && dump.get(0).is_some() {
                dump.remove(0);
            }
            first = false;

            for val in dump.iter_mut() {
                // unwrap or default because we dont want the code to panic in case of errors. need
                // to write all data to file even if parsing of one value fails.
                let pretty_json = serde_json::to_string_pretty(&val.1).unwrap_or_default();
                match &mut etcd_dump_file {
                    Some(etcd_dump_file) => {
                        write!(etcd_dump_file, "{}:\n{}\n\n", val.0, pretty_json)?;
                    }
                    None => {
                        println!("{}:\n{}\n", val.0, pretty_json);
                    }
                }
            }
            if let Some(etcd_dump_file) = &mut etcd_dump_file {
                etcd_dump_file.flush()?;
            }

            if let Some((key, _)) = dump.last() {
                prefix = key;
            } else {
                break;
            }
        }

        Ok(())
    }
}
