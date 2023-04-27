use crate::{PlatformInfo, PlatformNS, PlatformUid};

/// No specific platform.
/// Attempt to retrieve the platform uuid from (in order):
/// env var `NOPLATFORM_UUID_VAR`
/// machine id file `MACHINE_ID`
/// hardcoded from var `DUMMY_PLATFORM_UUID`
pub(super) struct None {
    platform_uuid: PlatformUid,
    platform_ns: PlatformNS,
}

const MACHINE_ID: &str = "/etc/machine-id";
const NOPLATFORM_UUID_VAR: &str = "NOPLATFORM_UUID";
const DUMMY_PLATFORM_UUID: &str = "dummy-uuid";
const DUMMY_PLATFORM_NS: &str = "default";

impl None {
    /// Create a new `Self`.
    pub(super) fn new() -> Self {
        let platform_uuid = if let Ok(uuid) = std::env::var(NOPLATFORM_UUID_VAR) {
            uuid
        } else {
            std::fs::read_to_string(MACHINE_ID).unwrap_or_else(|_| DUMMY_PLATFORM_UUID.to_string())
        };

        Self {
            platform_uuid: platform_uuid.trim().to_string(),
            platform_ns: PlatformNS::from(DUMMY_PLATFORM_NS),
        }
    }
}

impl PlatformInfo for None {
    fn uid(&self) -> PlatformUid {
        self.platform_uuid.clone()
    }

    fn namespace(&self) -> &PlatformNS {
        &self.platform_ns
    }
}
