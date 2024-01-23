use crate::types::v0::transport::FrontendNodeId;
use openapi::models;
use pstor::{ApiVersion, ObjectKey, StorableObject, StorableObjectType};
use serde::{Deserialize, Serialize};

/// Key used by the store to uniquely identify a FrontendNodeSpec structure.
pub struct FrontendNodeSpecKey(FrontendNodeId);

impl From<&FrontendNodeId> for FrontendNodeSpecKey {
    fn from(id: &FrontendNodeId) -> Self {
        Self(id.clone())
    }
}

impl ObjectKey for FrontendNodeSpecKey {
    type Kind = StorableObjectType;

    fn version(&self) -> ApiVersion {
        ApiVersion::V0
    }

    fn key_type(&self) -> StorableObjectType {
        StorableObjectType::FrontendNodeSpec
    }

    fn key_uuid(&self) -> String {
        self.0.to_string()
    }
}

impl StorableObject for FrontendNodeSpec {
    type Key = FrontendNodeSpecKey;

    fn key(&self) -> Self::Key {
        FrontendNodeSpecKey(self.id.clone())
    }
}

/// Frontend node labels.
pub type FrontendNodeLabels = std::collections::HashMap<String, String>;

/// Frontend node spec.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FrontendNodeSpec {
    /// Frontend Node identification.
    pub id: FrontendNodeId,
    /// Endpoint of the frontend instance.
    pub endpoint: std::net::SocketAddr,
    /// Frontend Node labels.
    pub labels: Option<FrontendNodeLabels>,
}

impl FrontendNodeSpec {
    pub fn new(
        id: FrontendNodeId,
        endpoint: std::net::SocketAddr,
        labels: Option<FrontendNodeLabels>,
    ) -> Self {
        Self {
            id,
            endpoint,
            labels,
        }
    }
}

impl From<FrontendNodeSpec> for models::FrontendNodeSpec {
    fn from(src: FrontendNodeSpec) -> Self {
        Self::new_all(src.id, src.endpoint.to_string(), src.labels)
    }
}
