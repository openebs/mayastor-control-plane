use crate::collect::{constants::MAYASTOR_SERVICE, resources::error::ResourceError};
use async_trait::async_trait;
use downcast_rs::{impl_downcast, Downcast};
use lazy_static::lazy_static;
use prettytable::Row;
use std::collections::{HashMap, HashSet};

lazy_static! {
    /// Represents map of resource name to service where resources are hosted
    pub(crate) static ref RESOURCE_TO_CONTAINER_NAME: HashMap<&'static str, &'static str> =
        HashMap::from([
            ("node", MAYASTOR_SERVICE),
            ("pool", MAYASTOR_SERVICE),
            ("nexus", MAYASTOR_SERVICE),
            ("replica", MAYASTOR_SERVICE),
            ("device", MAYASTOR_SERVICE),
        ]);
}

/// K8s label to identify mayastor daemon service
pub(crate) const MAYASTOR_DAEMONSET_LABEL: &str = "app=mayastor";

/// ResourceInformation holds fields to identify appropriate mayastor service
#[derive(PartialEq, Eq, Hash, Debug, Clone)]
pub(crate) struct ResourceInformation {
    container_name: String,
    host_name: String,
    label_selector: Vec<String>,
}

impl ResourceInformation {
    // default will populate current object with default values and return same
    pub(crate) fn default() -> Self {
        ResourceInformation {
            container_name: "".to_string(),
            host_name: "".to_string(),
            label_selector: vec![],
        }
    }

    /// Sets provided container name
    pub(crate) fn set_container_name(&mut self, container_name: String) {
        self.container_name = container_name;
    }

    /// Sets provided host name
    pub(crate) fn set_host_name(&mut self, host_name: String) {
        self.host_name = host_name;
    }

    /// Sets provided label selector
    pub(crate) fn set_label_selector(&mut self, label_selector: Vec<String>) {
        self.label_selector = label_selector;
    }
}

/// Implements functionality for displaying information in tabular manner and reading inputs
pub(crate) trait TablePrinter {
    fn get_header_row(&self) -> Row;
    fn create_rows(&self) -> Vec<Row>;
    fn get_resource_id(&self, row_data: &Row) -> Result<String, ResourceError>;
}

/// Implements functionality to inspect topology information
pub(crate) trait Topologer: Downcast {
    fn get_printable_topology(&self) -> Result<(String, String), ResourceError>;
    fn dump_topology_info(&self, dir_path: String) -> Result<(), ResourceError>;
    fn get_unhealthy_resource_info(&self) -> HashSet<ResourceInformation>;
    fn get_all_resource_info(&self) -> HashSet<ResourceInformation>;
}
impl_downcast!(Topologer);

/// Resourcer adds functionality to read inputs and build topology information
#[async_trait(?Send)]
pub(crate) trait Resourcer {
    type ID;
    async fn read_resource_id(&self) -> Result<Self::ID, ResourceError> {
        panic!("read_resource_id is not yet implemented");
    }
    async fn get_topologer(
        &self,
        _id: Option<Self::ID>,
    ) -> Result<Box<dyn Topologer>, ResourceError> {
        panic!("get_topologer is UnImplemented");
    }
}
