pub mod constants;

pub use constants::*;

pub mod tracing_telemetry;

pub mod test_constants;

pub mod version;

pub use version::{long_raw_version_str, raw_version_str, raw_version_string};
pub use version_info::{version_info as version_info_inner, VersionInfo};

/// Byte conversion helpers.
pub mod bytes;

/// Check for the presence of nvme ana multipath.
pub fn check_nvme_core_ana() -> Result<bool, std::io::Error> {
    let multipath = match std::fs::read_to_string("/sys/module/nvme_core/parameters/multipath") {
        Ok(multipath) => Ok(multipath),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            // check the nvme_core is enabled, otherwise all this is moot.
            std::fs::exists("/sys/module/nvme_core")?;
            // there's a race here where the module may be unloaded and reloaded, though this may
            // happen even after we check here - also when a volume is attached the module cannot
            // be unloaded, so we're safe to check it.
            return Ok(false);
        }
        Err(error) => Err(error),
    };
    match multipath?.trim().to_uppercase().as_str() {
        "Y" => Ok(true),
        "N" => Ok(false),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Invalid value in NVMe multipath file",
        )),
    }
}
