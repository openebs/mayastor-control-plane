use crate::types::v0::transport::AppNodeId;
use openapi::models;
use pstor::{ApiVersion, ObjectKey, StorableObject, StorableObjectType};
use serde::{Deserialize, Serialize};

/// Key used by the store to uniquely identify a AppNodeSpec structure.
pub struct AppNodeSpecKey(AppNodeId);

impl From<&AppNodeId> for AppNodeSpecKey {
    fn from(id: &AppNodeId) -> Self {
        Self(id.clone())
    }
}

impl ObjectKey for AppNodeSpecKey {
    type Kind = StorableObjectType;

    fn version(&self) -> ApiVersion {
        ApiVersion::V0
    }

    fn key_type(&self) -> StorableObjectType {
        StorableObjectType::AppNodeSpec
    }

    fn key_uuid(&self) -> String {
        self.0.to_string()
    }
}

impl StorableObject for AppNodeSpec {
    type Key = AppNodeSpecKey;

    fn key(&self) -> Self::Key {
        AppNodeSpecKey(self.id.clone())
    }
}

/// App node labels.
pub type AppNodeLabels = std::collections::HashMap<String, String>;

/// Transport-layer capabilities reported by the csi-node at registration time.
/// Room to grow: further transport-related flags slot in here without needing
/// another `AppNodeSpec` field per capability.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
pub struct TransportCaps {
    /// RDMA HCA hardware is present on this node (reported by `ibv_devinfo`).
    pub rdma_hca_present: bool,
    /// The `nvme_rdma` kernel module is loaded on this node. Both this and
    /// `rdma_hca_present` are required for NVMe-oF RDMA to actually be usable.
    pub nvme_rdma_module_loaded: bool,
    /// NVMe ANA multipath is enabled on this node
    /// (`/sys/module/nvme_core/parameters/multipath = Y`).
    pub ana_capable: bool,
}

/// App node spec.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AppNodeSpec {
    /// App Node identification.
    pub id: AppNodeId,
    /// Endpoint of the App instance.
    pub endpoint: std::net::SocketAddr,
    /// App Node labels.
    pub labels: Option<AppNodeLabels>,
    /// Transport capabilities reported at registration.
    #[serde(default)]
    pub transport_caps: Option<TransportCaps>,
}

impl AppNodeSpec {
    pub fn new(
        id: AppNodeId,
        endpoint: std::net::SocketAddr,
        labels: Option<AppNodeLabels>,
        transport_caps: Option<TransportCaps>,
    ) -> Self {
        Self {
            id,
            endpoint,
            labels,
            transport_caps,
        }
    }
}

impl From<AppNodeSpec> for models::AppNodeSpec {
    fn from(src: AppNodeSpec) -> Self {
        Self::new_all(
            src.id,
            src.endpoint.to_string(),
            src.labels,
            src.transport_caps.map(Into::into),
        )
    }
}

impl From<TransportCaps> for models::TransportCaps {
    fn from(src: TransportCaps) -> Self {
        Self::new_all(
            src.rdma_hca_present,
            src.nvme_rdma_module_loaded,
            src.ana_capable,
        )
    }
}

impl From<models::TransportCaps> for TransportCaps {
    fn from(src: models::TransportCaps) -> Self {
        Self {
            rdma_hca_present: src.rdma_hca_present,
            nvme_rdma_module_loaded: src.nvme_rdma_module_loaded,
            ana_capable: src.ana_capable,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_caps_round_trip_openapi() {
        let store = TransportCaps {
            rdma_hca_present: true,
            nvme_rdma_module_loaded: false,
            ana_capable: false,
        };
        let api: models::TransportCaps = store.clone().into();
        let back: TransportCaps = api.into();
        assert_eq!(store, back);
    }

    #[test]
    fn app_node_spec_serde_back_compat_no_caps() {
        // Simulates an etcd entry written by a pre-TransportCaps build:
        // `transport_caps` is absent from the payload and must default to None.
        let json = r#"{"id":"csi-node-1","endpoint":"10.0.0.1:50052","labels":null}"#;
        let spec: AppNodeSpec = serde_json::from_str(json).unwrap();
        assert!(spec.transport_caps.is_none());
    }

    #[test]
    fn app_node_spec_serde_with_caps() {
        let spec = AppNodeSpec::new(
            "csi-node-2".into(),
            "10.0.0.2:50052".parse().unwrap(),
            None,
            Some(TransportCaps {
                rdma_hca_present: true,
                nvme_rdma_module_loaded: true,
                ana_capable: true,
            }),
        );
        let json = serde_json::to_string(&spec).unwrap();
        let back: AppNodeSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(spec, back);
    }
}
