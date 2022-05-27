use crate::platform::{PlatformError, PlatformInfo, PlatformUid};

use k8s_openapi::api::core::v1::Namespace;
use kube::{Api, Client, Resource};

/// Kubernetes Platform.
pub struct K8s {
    platform_uuid: PlatformUid,
}
impl K8s {
    /// Create new `Self`.
    pub(super) async fn new() -> Result<Self, PlatformError> {
        let client = Client::try_default()
            .await
            .map_err(|e| format!("Can't connect to k8s api-server: {}", e))?;
        let platform_uuid = Self::fetch_platform_uuid(client).await?;
        Ok(Self { platform_uuid })
    }
    /// Create new `Self` from a given `Client`.
    pub async fn from(client: Client) -> Result<Self, PlatformError> {
        let platform_uuid = Self::fetch_platform_uuid(client).await?;
        Ok(Self { platform_uuid })
    }
    async fn fetch_platform_uuid(client: Client) -> Result<PlatformUid, PlatformError> {
        let namespaces: Api<Namespace> = Api::all(client);
        let ns = namespaces
            .get("kube-system")
            .await
            .map_err(|e| format!("Failed to get kube-system namespace: {}", e))?;

        match &ns.meta().uid {
            None => Err("The kube-system namespace has no UID".to_string()),
            Some(uid) if uid.is_empty() => {
                Err("The kube-system namespace has an empty UID".to_string())
            }
            Some(uid) => Ok(uid.into()),
        }
    }
}

impl PlatformInfo for K8s {
    fn uid(&self) -> PlatformUid {
        self.platform_uuid.clone()
    }
}
