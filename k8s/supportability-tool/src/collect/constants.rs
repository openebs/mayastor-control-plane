use lazy_static::lazy_static;
use std::collections::HashMap;

// Defines the name of the core-agents service
pub(crate) const CORE_AGENT_SERVICE: &str = "core-agents";

// Defines the name of the csi-controller service
pub(crate) const CSI_CONTROLLER_SERVICE: &str = "csi-controller";

// Defines the name of the jaeger-operator service
pub(crate) const JAEGER_OPERATOR_SERVICE: &str = "jaeger-operator";

// Defines the name of the jaeger service
pub(crate) const JAEGER_SERVICE: &str = "jaeger";

// Defines the name of the pool-operator service
pub(crate) const POOL_OPERATOR_SERVICE: &str = "msp-operator";

// Defines the name of the rest service
pub(crate) const REST_SERVICE: &str = "rest";

// Defines the name of the csi node daemon service
pub(crate) const CSI_NODE_SERVICE: &str = "csi-node";

// Defines the name of the etcd service
pub(crate) const ETCD_SERVICE: &str = "etcd";

// Defines the name of mayastor service
pub(crate) const MAYASTOR_SERVICE: &str = "mayastor";

lazy_static! {
    // List of resources fall under control plane services
    pub(crate) static ref CONTROL_PLANE_SERVICES: HashMap<&'static str, bool> =
        HashMap::from([
            (CORE_AGENT_SERVICE, true),
            (CSI_CONTROLLER_SERVICE, true),
            (JAEGER_OPERATOR_SERVICE, true),
            (JAEGER_SERVICE, true),
            (POOL_OPERATOR_SERVICE, true),
            (REST_SERVICE, true),
            (CSI_NODE_SERVICE, true),
            (ETCD_SERVICE, true),
        ]);

    // List of resources fall under data plane services
    pub(crate) static ref DATA_PLANE_SERVICES: HashMap<&'static str, bool> =
        HashMap::from([
            (MAYASTOR_SERVICE, true),
        ]);

    // Represents the list of services that requires hostname to collect logs
    pub(crate) static ref HOST_NAME_REQUIRED_SERVICES: HashMap<&'static str, bool> =
        HashMap::from([
            (MAYASTOR_SERVICE, true),
            (ETCD_SERVICE, true),
            (CSI_NODE_SERVICE, true),
        ]);
}
