use crate::collect::{error::Error, resources::traits::Topologer, rest_wrapper::RestClient};
use chrono::Local;

/// DumpConfig helps to create new instance of Dumper
#[derive(Debug)]
pub(crate) struct DumpConfig {
    /// Client to interact with REST service
    pub(crate) rest_client: RestClient,
    /// directory path to create archive files
    pub(crate) output_directory: String,
    /// namespace of mayastor system
    pub(crate) namespace: String,
    /// Address of Loki service endpoint
    pub(crate) loki_uri: Option<String>,
    /// Address of etcd service endpoint
    pub(crate) etcd_uri: Option<String>,
    /// Period states to collect logs from specified duration
    pub(crate) since: humantime::Duration,
    /// Path to kubeconfig file, which requires to interact with Kube-Apiserver
    pub(crate) kube_config_path: Option<std::path::PathBuf>,
    /// Specifies the timeout value to interact with other systems
    pub(crate) timeout: humantime::Duration,
    /// Topologer implements functionality to build topological infotmation of system
    pub(crate) topologer: Option<Box<dyn Topologer>>,
    pub(crate) output_format: OutputFormat,
}

/// The output format.
#[derive(Debug)]
pub(crate) enum OutputFormat {
    /// A tar file.
    Tar,
    /// The STDOUT.
    Stdout,
}

/// Defines prefix name of temporary directory to create dump files
pub(crate) const DUMP_TMP_PREFIX: &str = "tmp-mayastor";

/// Creates new temporary directory in given path to store dump artifacts
pub(crate) fn create_and_get_tmp_directory(dir_path: String) -> Result<String, Error> {
    let date = Local::now();
    let suffix_dir_name = format!("{}-{}", DUMP_TMP_PREFIX, date.format("%Y-%m-%d-%H-%M-%S"));
    let new_dir_path = std::path::Path::new(&dir_path).join(suffix_dir_name);
    std::fs::create_dir_all(new_dir_path.clone())?;
    Ok(new_dir_path.into_os_string().into_string()?)
}

impl Stringer for Vec<String> {
    fn as_string(&self, delim: char) -> String {
        let mut concatenate_str: String = String::new();
        self.iter().for_each(|val| {
            concatenate_str.push_str(val);
            concatenate_str.push_str(delim.to_string().as_str());
        });
        concatenate_str.pop();
        concatenate_str
    }
}

/// Defines method to convert various objects to string
pub(crate) trait Stringer {
    fn as_string(&self, delim: char) -> String;
}
