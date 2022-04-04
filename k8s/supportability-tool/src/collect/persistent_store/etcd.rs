use crate::collect::{k8s_resources::client::ClientSet, persistent_store::EtcdError};
use common_lib::{store::etcd, types::v0::store::definitions::Store};
use std::{
    io::Write,
    net::{SocketAddr, ToSocketAddrs},
    path::PathBuf,
};

const ETCD_SERVICE_LABEL_SELECTOR: &str = "app.kubernetes.io/name=etcd";

/// EtcdStore is used to abstract connection to etcd database for dumping the
/// contents
pub(crate) struct EtcdStore {
    etcd: etcd::Etcd,
    namespace: String,
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
        let etcd: etcd::Etcd;
        // if an endpoint is provided it will be used, else the kubeconfig path will be used
        // to find the endpoint of the headless etcd service
        if let Some(endpoint) = etcd_endpoint {
            etcd = etcd::Etcd::new(endpoint.as_str()).await?;
        } else {
            let c = ClientSet::new(kube_config_path, namespace.clone()).await?;
            let e = get_etcd_endpoint(c).await?;
            etcd = etcd::Etcd::new(e.to_string().as_str()).await?;
        }

        Ok(Self { etcd, namespace })
    }

    /// dump all the data from etcd in the selected namespace into a file in
    /// the given working directory.
    pub(crate) async fn dump(&mut self, working_dir: PathBuf) -> Result<(), EtcdError> {
        let key_prefix = format!("/namespace/{}", self.namespace);
        let mut dump = self.etcd.get_values_prefix(key_prefix.as_str()).await?;
        let file_name = "etcd_dump".to_string();
        let file_path = working_dir.join(file_name);
        let mut etcd_dump_file = std::fs::File::create(file_path)?;
        for val in dump.iter_mut() {
            // unwrap or default because we dont want the code to panic in case of errors. need to
            // write all data to file even if parsing of one value fails.
            let pretty_json = serde_json::to_string_pretty(&val.1).unwrap_or_default();
            write!(etcd_dump_file, "{}:\n{}\n\n", val.0, pretty_json)?;
        }
        Ok(())
    }
}

// used to get the etcd end point from the etcd service
async fn get_etcd_endpoint(c: ClientSet) -> Result<SocketAddr, EtcdError> {
    let svcs = c.get_svcs(ETCD_SERVICE_LABEL_SELECTOR).await?;

    // since the service object is returned from k8s, we can safely unwrap as the spec wont
    // be empty
    let svc = svcs
        .iter()
        .find(|s| s.spec.as_ref().unwrap().cluster_ip.is_some())
        .ok_or_else(|| EtcdError::Custom(String::from("cannot find etcd service")))?;
    let ip = svc.spec.as_ref().unwrap().cluster_ip.as_ref().unwrap();
    let client_port = svc
        .spec
        .as_ref()
        .unwrap()
        .ports
        .as_ref()
        .unwrap()
        .iter()
        // client port name defined in service
        .find(|p| p.name == Some("client".to_string()))
        .ok_or_else(|| EtcdError::Custom(String::from("cannot find client port for etcd service")))?
        .port;
    let address = format!("{}:{}", ip, client_port);
    match address.to_socket_addrs() {
        Ok(mut a) => a
            .next()
            .ok_or_else(|| EtcdError::Custom(String::from("could not find socket address"))),
        Err(e) => Err(EtcdError::Custom(format!(
            "cannot resolve endpoint for etcd: {}",
            e
        ))),
    }
}
