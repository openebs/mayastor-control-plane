pub mod constants;

pub use constants::*;

pub mod tracing_telemetry;

pub mod test_constants;

pub mod version;

pub use version::{long_raw_version_str, raw_version_str, raw_version_string};
pub use version_info::{version_info as version_info_inner, VersionInfo};

/// Select aws-lc-rs as Rustls's process-wide crypto provider when one has not been selected yet.
pub fn init_rustls_crypto_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

/// Byte conversion helpers.
pub mod bytes;
/// Dev path normalizer helpers.
pub mod disk;

/// Check for the presence of nvme ana multipath.
pub fn check_nvme_core_ana() -> Result<bool, std::io::Error> {
    let multipath = match std::fs::read_to_string("/sys/module/nvme_core/parameters/multipath") {
        Ok(multipath) => Ok(multipath),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            // check the nvme_core is enabled, otherwise all this is moot.
            if !std::fs::exists("/sys/module/nvme_core")? {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "kernel module nvme_core not found",
                ));
            }

            // there's a race here where the module may be unloaded and reloaded, though this may
            // happen even after we check here - also when a volume is attached the module cannot
            // be unloaded, so we're safe to check it.

            // RHEL10 and derivatives have defaulted to multipath enabled, but have done so whilst
            // removing the multipath parameter completely for some reason, which is a breaking
            // change for us: https://issues.redhat.com/browse/RHEL-67045
            //
            // If multipath parameter doesn't exist but iopolicy does, let's assume multipath is on?
            return std::fs::exists("/sys/module/nvme_core/parameters/iopolicy");
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
