use crate::platform::{PlatformInfo, PlatformUid};

/// No specific platform.
/// Attempt to retrieve the platform uuid from (in order):
/// env var `NOPLATFORM_UUID_VAR`
/// hardcoded from var `DUMMY_PLATFORM_UUID`
pub(super) struct None {
    platform_uuid: PlatformUid,
}

const NOPLATFORM_UUID_VAR: &str = "NOPLATFORM_UUID";
const DUMMY_PLATFORM_UUID: &str = "dummy-uuid";

impl None {
    /// Create a new `Self`.
    pub(super) fn new() -> Self {
        let platform_uuid = if let Ok(uuid) = std::env::var(NOPLATFORM_UUID_VAR) {
            uuid
        } else {
            DUMMY_PLATFORM_UUID.to_string()
        };

        Self {
            platform_uuid: platform_uuid.trim().to_string(),
        }
    }
}

impl PlatformInfo for None {
    fn uid(&self) -> PlatformUid {
        self.platform_uuid.clone()
    }
}
