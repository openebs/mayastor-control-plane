//! Utility functions for matching a udev record against a known device type.

use crate::error::DeviceError;
use udev::Device;

macro_rules! require {
    (let $name:ident = $attribute:expr) => {
        let $name = match $attribute {
            Some(outer) => match outer.to_str() {
                Some(inner) => inner,
                None => {
                    return None;
                }
            },
            None => {
                return None;
            }
        };
    };
    ($value:ident == $attribute:expr) => {
        match $attribute {
            Some(outer) => match outer.to_str() {
                Some(inner) => {
                    if $value != inner {
                        return None;
                    }
                }
                None => {
                    return None;
                }
            },
            None => {
                return None;
            }
        }
    };
    ($value:literal == $attribute:expr) => {
        match $attribute {
            Some(outer) => match outer.to_str() {
                Some(inner) => {
                    if $value != inner {
                        return None;
                    }
                }
                None => {
                    return None;
                }
            },
            None => {
                return None;
            }
        }
    };
}

pub(super) fn match_iscsi_device(device: &Device) -> Option<(&str, &str)> {
    require!("Nexus_CAS_Driver" == device.property_value("ID_MODEL"));
    require!("scsi" == device.property_value("ID_BUS"));

    require!(let devname = device.property_value("DEVNAME"));
    require!(let path = device.property_value("ID_PATH"));

    Some((devname, path))
}

pub(super) fn match_nvmf_device<'a>(device: &'a Device, key: &str) -> Option<&'a str> {
    let model_id = utils::nvme_controller_model_id();

    require!(let devname = device.property_value("DEVNAME"));

    let id_wwn = device.property_value("ID_WWN").and_then(|s| s.to_str());
    let wwid = device.attribute_value("wwid").and_then(|s| s.to_str());
    let udev_matches = id_wwn == Some(key)
        && device.property_value("ID_MODEL").and_then(|s| s.to_str()) == Some(model_id.as_str());

    let matches = nvme_namespace_matches(key, wwid, udev_matches, || {
        if id_wwn != Some(key) {
            tracing::error!(
                initialized = device.is_initialized(),
                "{devname} has wwid of {key} != ID_WWN of {id_wwn:?}, is udev bad?",
            );
        }
        device.parent().is_some_and(|parent| {
            parent
                .attribute_value("model")
                .is_some_and(|model| model.to_string_lossy().starts_with(model_id.as_str()))
        })
    });
    matches.then_some(devname)
}

fn nvme_namespace_matches(
    key: &str,
    wwid: Option<&str>,
    udev_matches: bool,
    sysfs_model_matches: impl FnOnce() -> bool,
) -> bool {
    if wwid == Some(key) {
        return sysfs_model_matches() || udev_matches;
    }
    if !udev_matches {
        return false;
    }
    // A stale udev record must not override a different UUID reported by the kernel.
    let uuid = |id: &str| uuid::Uuid::try_parse(id.strip_prefix("uuid.")?).ok();
    match (wwid.and_then(uuid), uuid(key)) {
        (Some(actual), Some(expected)) => actual == expected,
        _ => true,
    }
}

/// Match the device, if it's a nvmef device, but only if it's a valid block device.
/// See [`super::dev::nvmf::match_device`].
pub(super) fn match_nvmf_device_valid<'a>(
    device: &'a Device,
    key: &str,
    multipath: Option<bool>,
) -> Result<Option<&'a str>, DeviceError> {
    let cell = std::sync::atomic::AtomicBool::new(true);
    super::dev::nvmf::match_device(device, key, multipath, &cell)
}

#[cfg(test)]
mod tests {
    use super::nvme_namespace_matches;

    const VOLUME_A: &str = "uuid.274fe16d-ec41-40a1-8686-95df7517a424";
    const VOLUME_B: &str = "uuid.de733a18-d928-41b9-9b2c-1c0199bb881f";

    #[test]
    fn sysfs_identifies_volume_despite_stale_udev() {
        let wwid = Some(VOLUME_A);
        assert!(nvme_namespace_matches(VOLUME_A, wwid, false, || true));
        assert!(!nvme_namespace_matches(VOLUME_A, wwid, false, || false));
    }

    #[test]
    fn stale_udev_cannot_override_another_namespace_uuid() {
        let wwid = Some(VOLUME_B);
        assert!(!nvme_namespace_matches(VOLUME_A, wwid, true, || true));
    }

    #[test]
    fn udev_fallback_requires_a_match_when_sysfs_uuid_is_unavailable() {
        for wwid in [None, Some("eui.0123456789abcdef"), Some("uuid.invalid")] {
            assert!(nvme_namespace_matches(VOLUME_A, wwid, true, || false));
            assert!(!nvme_namespace_matches(VOLUME_A, wwid, false, || false));
        }
    }
}
