pub mod constants;

pub use constants::*;

pub mod tracing_telemetry;

pub mod test_constants;

pub mod version;

pub use version::{long_raw_version_str, raw_version_str, raw_version_string};
pub use version_info::{version_info as version_info_inner, VersionInfo};

/// Byte conversion helpers.
pub mod bytes;

use std::fs;
/// Check for the presence of nvme ana multipath.
pub fn check_nvme_core_ana() -> Result<bool, std::io::Error> {
    match fs::read_to_string("/sys/module/nvme_core/parameters/multipath")?
        .trim()
        .to_uppercase()
        .as_str()
    {
        "Y" => Ok(true),
        "N" => Ok(false),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Invalid value in NVMe multipath file",
        )),
    }
}
