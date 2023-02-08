use std::ops::Deref;
use tokio::{runtime::Builder, sync::OnceCell};

/// K8S Platform Information.
pub mod k8s;
mod none;

/// Platform Unique identifier.
pub type PlatformUid = String;

/// Platform Error.
pub type PlatformError = String;

/// Common Platform information.
pub trait PlatformInfo: Send + Sync {
    /// Get the platform `PlatformUid`.
    fn uid(&self) -> PlatformUid;
}

#[derive(Eq, PartialEq)]
/// The various types of platforms.
pub enum PlatformType {
    /// As it says in the tin.
    K8s,
    /// We don't have others, other than running the binary directly or on deployer "clusters".
    None,
}

/// Get the current `PlatformType`.
pub fn current_plaform_type() -> PlatformType {
    if std::env::var("KUBERNETES_SERVICE_HOST").is_ok() {
        PlatformType::K8s
    } else {
        PlatformType::None
    }
}

/// Cached Platform information.
static PLATFORM: OnceCell<Box<dyn PlatformInfo>> = OnceCell::const_new();

/// Init the cached `PlatformInfo` object.
pub async fn init_cluster_info() -> Result<&'static dyn PlatformInfo, PlatformError> {
    PLATFORM
        .get_or_try_init(|| async move {
            Ok(match current_plaform_type() {
                PlatformType::K8s => Box::new(k8s::K8s::new().await?) as Box<dyn PlatformInfo>,
                PlatformType::None => Box::new(none::None::new()) as Box<dyn PlatformInfo>,
            })
        })
        .await
        .map(|i| i.deref())
}

/// Init the cached `PlatformInfo` object or panic.
pub async fn init_cluster_info_or_panic() -> &'static dyn PlatformInfo {
    match init_cluster_info().await {
        Ok(info) => info,
        Err(error) => {
            let message = "Failed to collect critical information from the platform";
            tracing::error!(error=%error, message);
            panic!("{message}: {error}")
        }
    }
}

/// Get a cached `PlatformInfo` object.
pub(super) fn platform_info() -> &'static dyn PlatformInfo {
    if let Some(platform) = PLATFORM.get() {
        return platform.as_ref();
    }

    tracing::warn!("PlatformInfo should be pre-initialized before use");

    // only when we don't pre initialize and on the first iteration/(concurrent ones) do we fall
    // through here.
    let rt = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Should get a runtime");
    let thread =
        std::thread::spawn(move || rt.block_on(async move { init_cluster_info_or_panic().await }));

    thread.join().expect("thread should not panic")
}
