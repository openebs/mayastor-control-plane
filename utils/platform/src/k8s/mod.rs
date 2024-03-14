use crate::{PlatformError, PlatformInfo, PlatformNS, PlatformUid};

use k8s_openapi::api::core::v1::Namespace;
use kube::{Api, Client, Resource};

/// Kubernetes Platform.
pub struct K8s {
    platform_uuid: PlatformUid,
    platform_ns: PlatformNS,
}
impl K8s {
    /// Create new `Self`.
    pub(super) async fn new() -> Result<Self, PlatformError> {
        let client = Client::try_default()
            .await
            .map_err(|e| format!("Can't connect to k8s api-server: {e}"))?;
        Self::from(client).await
    }
    /// Create new `Self` from a given `Client`.
    pub async fn from(client: Client) -> Result<Self, PlatformError> {
        let platform_uuid = Self::fetch_platform_uuid(client).await?;
        // todo: should we use the default namespace from the client?
        let platform_ns = std::env::var("MY_POD_NAMESPACE").unwrap_or_else(|_| "default".into());
        Ok(Self {
            platform_uuid,
            platform_ns,
        })
    }
    /// Create new `Self` from a given `Client` and namespace.
    /// This may be necessary if we're running outside of the cluster itself, which
    /// means that MY_POD_NAMESPACE itself is not available... Perhaps using a client
    /// configured with the namespace as default would be better.
    pub async fn from_custom(client: Client, namespace: &str) -> Result<Self, PlatformError> {
        let platform_uuid = Self::fetch_platform_uuid(client).await?;
        Ok(Self {
            platform_uuid,
            platform_ns: namespace.to_string(),
        })
    }
    async fn fetch_platform_uuid(client: Client) -> Result<PlatformUid, PlatformError> {
        let namespaces: Api<Namespace> = Api::all(client);
        let ns = namespaces
            .get("kube-system")
            .await
            .map_err(|e| format!("Failed to get kube-system namespace: {e}"))?;

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

    fn namespace(&self) -> &PlatformNS {
        &self.platform_ns
    }
}
