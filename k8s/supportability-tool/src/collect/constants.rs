use lazy_static::lazy_static;
use std::collections::HashMap;

/// Defines the name of the core-agents service
pub(crate) const CORE_AGENT_SERVICE: &str = "core-agents";

/// Defines the name of the csi-controller service
pub(crate) const CSI_CONTROLLER_SERVICE: &str = "csi-controller";

/// Defines the name of the jaeger-operator service
pub(crate) const JAEGER_OPERATOR_SERVICE: &str = "jaeger-operator";

/// Defines the name of the jaeger service
pub(crate) const JAEGER_SERVICE: &str = "jaeger";

/// Defines the name of the pool-operator service
pub(crate) const POOL_OPERATOR_SERVICE: &str = "msp-operator";

/// Defines the name of the rest service
pub(crate) const REST_SERVICE: &str = "rest";

/// Defines the name of the csi node daemon service
pub(crate) const CSI_NODE_SERVICE: &str = "csi-node";

/// Defines the name of the etcd service
pub(crate) const ETCD_SERVICE: &str = "etcd";

/// Defines the name of mayastor service
pub(crate) const MAYASTOR_SERVICE: &str = "mayastor";

/// Defines the name of mayastor-io container(dataplane container)
pub(crate) const DATA_PLANE_CONTAINER_NAME: &str = "mayastor";

/// Defines the logging label(key-value pair) on mayastor services
pub(crate) const LOGGING_LABEL_SELECTOR: &str = "openebs.io/logging=true";

/// Defines LOKI PORT NAME which exposes logs
pub(crate) const LOKI_METRICS_PORT_NAME: &str = "http-metrics";

lazy_static! {
    /// List of resources fall under control plane services
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

    /// List of resources fall under data plane services
    pub(crate) static ref DATA_PLANE_SERVICES: HashMap<&'static str, bool> =
        HashMap::from([
            (MAYASTOR_SERVICE, true),
        ]);

    /// Represents the list of services that requires hostname to collect logs
    pub(crate) static ref HOST_NAME_REQUIRED_SERVICES: HashMap<&'static str, bool> =
        HashMap::from([
            (MAYASTOR_SERVICE, true),
            (ETCD_SERVICE, true),
            (CSI_NODE_SERVICE, true),
        ]);
}

/// Defines the label selector to fetch Loki related services
pub(crate) const LOKI_SERVICE_LABEL_SELECTOR: &str = "app=loki";

/// Node port identifier to access etcd service running in cluster
pub(crate) const ETCD_SERVICE_PORT_NAME: &str = "client";

/// Defines the label selector to fetch etcd related services
pub(crate) const ETCD_SERVICE_LABEL_SELECTOR: &str = "app.kubernetes.io/name=etcd";
