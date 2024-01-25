use crate::types::v0::transport::AppNodeId;
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

/// App node spec.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AppNodeSpec {
    /// App Node identification.
    pub id: AppNodeId,
    /// Endpoint of the App instance.
    pub endpoint: std::net::SocketAddr,
    /// App Node labels.
    pub labels: Option<AppNodeLabels>,
}

impl AppNodeSpec {
    pub fn new(
        id: AppNodeId,
        endpoint: std::net::SocketAddr,
        labels: Option<AppNodeLabels>,
    ) -> Self {
        Self {
            id,
            endpoint,
            labels,
        }
    }
}
