/// Various common constants used by the control plane.

/// Branch specific image tag to test against.
/// Example, used to test against a particular branch of the dataplane.
use crate::test_constants::{TARGET_BRANCH, TARGET_REGISTRY};

/// Default request timeout for any NATS or GRPC request.
pub const DEFAULT_REQ_TIMEOUT: &str = "5s";

/// Default connection timeout for a GRPC connection.
pub const DEFAULT_CONN_TIMEOUT: &str = "1s";

/// Use a set of minimum timeouts for specific requests.
pub const ENABLE_MIN_TIMEOUTS: bool = true;

/// The timeout for all persistent store operations.
pub const STORE_OP_TIMEOUT: &str = "5s";
/// The lease lock ttl for the persistent store after which we'll lose the exclusive access.
pub const STORE_LEASE_LOCK_TTL: &str = "30s";

fn target_tag() -> String {
    TARGET_BRANCH.replace('/', "-")
}

/// Fio Spdk image.
pub fn fio_spdk_image() -> String {
    format!("{TARGET_REGISTRY}/{PRODUCT_NAME}-fio-spdk:{}", target_tag())
}

/// Io-Engine container image used for testing.
pub fn io_engine_image() -> String {
    format!(
        "{TARGET_REGISTRY}/{PRODUCT_NAME}-io-engine:{}",
        target_tag()
    )
}

/// Environment variable that points to an io-engine binary.
/// This must be in sync with shell.nix.
pub const DATA_PLANE_BINARY: &str = "IO_ENGINE_BIN";

/// The period at which a component updates its resource cache.
pub const CACHE_POLL_PERIOD: &str = "30s";

/// The diskpool api name.
/// Note, the CRDs are defined inline and use literal string so that must
/// be changed as well.
pub const DSP_API_NAME: &str = PRODUCT_DOMAIN_NAME;

/// The key to mark the creation source of a pool in labels.
pub fn dsp_created_by_key() -> String {
    format!("{DSP_API_NAME}/created-by")
}
/// The key to protect diskpool from deletion.
pub fn dsp_finalizer() -> String {
    format!("{DSP_API_NAME}/diskpool-protection")
}

/// The value to mark the creation source of a pool to be disk pool operator in labels.
pub const DSP_OPERATOR: &str = "operator-diskpool";

/// The service label for the api-rest service.
pub const API_REST_LABEL: &str = "app=api-rest";
/// The service port for the api-rest label for the etcd pods.
pub const API_REST_HTTP_PORT: &str = "http";

/// The service label for the upgrade operator service.
pub const UPGRADE_OPERATOR_LABEL: &str = "app=operator-upgrade";

/// The service port for upgrade operator.
pub const UPGRADE_OPERATOR_HTTP_PORT: &str = "http";

/// The pod label for the etcd pods.
pub const ETCD_LABEL: &str = "app=etcd";
/// The port for the etcd pods.
pub const ETCD_PORT: &str = "client";

/// The service label for the loki service.
pub const LOKI_LABEL: &str = "app=loki";
/// The service port for the loki.
pub const LOKI_PORT: &str = "http-metrics";

/// The default value to be assigned as GRPC server addr if not overridden.
pub const DEFAULT_GRPC_SERVER_ADDR: &str = "0.0.0.0:50051";

/// The default value to be assigned as GRPC client addr if not overridden.
pub const DEFAULT_GRPC_CLIENT_ADDR: &str = "https://core:50051";

/// The default value to be assigned as JSON GRPC server addr if not overridden.
pub const DEFAULT_JSON_GRPC_SERVER_ADDR: &str = "0.0.0.0:50052";

/// The default value to be assigned as JSON GRPC client addr if not overridden.
pub const DEFAULT_JSON_GRPC_CLIENT_ADDR: &str = "https://jsongrpc:50052";

/// The default value for a concurrency limit.
pub const DEFAULT_GRPC_CLIENT_CONCURRENCY: usize = 25;

/// The default value to be assigned as cluster agent GRPC server addr if not overridden.
pub const DEFAULT_CLUSTER_AGENT_SERVER_ADDR: &str = "0.0.0.0:11500";

/// The default value to be assigned as cluster agent GRPC client addr if not overridden.
pub const DEFAULT_CLUSTER_AGENT_CLIENT_ADDR: &str = "https://agent-ha-cluster:11500";

/// The default value to be assigned as node-agent GRPC server addr if not overridden.
pub const DEFAULT_NODE_AGENT_SERVER_ADDR: &str = "0.0.0.0:11600";

/// The default worker threads cap for the api-rest service.
pub const DEFAULT_REST_MAX_WORKER_THREADS: &str = "8";

/// The default kubernetes namespace for this project.
pub const DEFAULT_NAMESPACE: &str = PRODUCT_NAME;

/// NVMe NQN DATE used by the product.
pub const NVME_NQN_DATE: &str = "2019-05";
/// NVMe NQN ORG used by the product.
pub const NVME_NQN_ORG: &str = PRODUCT_REV_DOMAIN_NAME;
/// NQN prefix for NVMe targets created by the product.
pub fn nvme_target_nqn_prefix() -> String {
    format!("nqn.{NVME_NQN_DATE}.{NVME_NQN_ORG}")
}
/// NVMe HOST NQN prefix used by the product.
pub const NVME_HOST_NQN: &str = "node-name";
/// NQN prefix for NVMe HOSTNQN used by the product.
pub fn nvme_initiator_nqn_prefix() -> String {
    format!("nqn.{NVME_NQN_DATE}.{NVME_NQN_ORG}:{NVME_HOST_NQN}")
}

/// NVMe path check period.
pub const NVME_PATH_CHECK_PERIOD: &str = "3s";

/// NVMe path connection timeout for path replacement operation.
pub const NVME_PATH_CONNECTION_PERIOD: &str = "11s";

/// The default retransmission interval for reporting failed paths in case of network issues.
pub const NVME_PATH_RETRANSMISSION_PERIOD: &str = "10s";

/// Period for aggregating multiple failed paths before reporting them.
pub const NVME_PATH_AGGREGATION_PERIOD: &str = "1s";

/// NVMe subsystem refresh period when monitoring its state.
pub const NVME_SUBSYS_REFRESH_PERIOD: &str = "500ms";

/// Period for aggregating multiple failed paths before reporting them.
pub const DEFAULT_HOST_ACCESS_CONTROL: &str = "nexuses,replicas";

/// K8s sts pvc naming convention regex expression.
/// The naming format is {pvc-name-common}-{sts-name}-{index}.
/// A valid sts pvc name of above format is matched by the regex below.
pub const K8S_STS_PVC_NAMING_REGEX: &str = r"^([a-z0-9](?:[-a-z0-9]*[a-z0-9])?)-\d+$";

/// Maximum number of snapshot transactions to be pruned per call.
pub const SNAPSHOT_TRANSACTION_PRUNE_LIMIT: usize = 10;

/// Maximum number of snapshot transactions allowed.
pub const SNAPSHOT_MAX_TRANSACTION_LIMIT: usize = 5;

/// Label for the csi-node nvme ana multi-path.
pub fn csi_node_nvme_ana() -> String {
    format!("{PRODUCT_DOMAIN_NAME}/csi-node.nvme-ana")
}
/// The CSI plugin's name.
pub fn csi_plugin_name() -> String {
    format!("{PRODUCT_REV_DOMAIN_NAME}.csi-{PRODUCT_NAME}")
}

/// Domain name for the product.
pub const PRODUCT_DOMAIN_NAME: &str = "openebs.io";
/// Reverse domain name for the product.
pub const PRODUCT_REV_DOMAIN_NAME: &str = "io.openebs";

/// Name of the product.
pub const PRODUCT_NAME: &str = "mayastor";

/// Max limit for etcd pagination.
pub const ETCD_MAX_PAGE_LIMIT: &str = "500";
