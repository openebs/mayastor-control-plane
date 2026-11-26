use crate::{error::DeviceError, filesystem_ops::FileSystem};
use csi_driver::filesystem::FileSystem as Fs;
use utils::csi_plugin_name;

use serde_json::Value;
use std::{
    borrow::Cow,
    collections::HashMap,
    os::unix::fs::FileTypeExt,
    path::{Path, PathBuf},
    str::FromStr,
    string::String,
    vec::Vec,
};
use tracing::{error, warn};

// Keys of interest we expect to find in the JSON output generated
// by findmnt.
const TARGET_KEY: &str = "target";
const SOURCE_KEY: &str = "source";
const FSTYPE_KEY: &str = "fstype";

#[derive(Debug)]
pub(crate) struct DeviceMount {
    mount_path: String,
    fstype: FileSystem,
}

impl std::fmt::Display for DeviceMount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MountPath: {}, FileSystem: {}",
            self.mount_path, self.fstype
        )
    }
}

impl DeviceMount {
    /// create new DeviceMount
    pub(crate) fn new(mount_path: String, fstype: FileSystem) -> DeviceMount {
        Self { mount_path, fstype }
    }
    /// File system type
    pub(crate) fn fstype(&self) -> FileSystem {
        self.fstype.clone()
    }
    /// Get the mount path.
    pub(crate) fn mount_path(&self) -> String {
        self.mount_path.clone()
    }
}

#[derive(Debug)]
struct Filter<'a> {
    key: &'a str,
    value: &'a str,
}

/// Convert a json value of a key-value pair, to a string,
/// adjusted if required, on the key.
///
/// The source field returned from findmnt can be different for
/// the same source on different systems, for example
///   dev[/nvme0n1], udev[/nvme0n1], tmpfs[/nvme0n1], devtmpfs[/nvme0n1]
/// this function converts those values to the expected /dev/nvme0n1
fn key_adjusted_value(key: &str, value: &Value) -> String {
    lazy_static::lazy_static! {
        static ref RE_UDEVPATH: regex::Regex = regex::Regex::new(
            r"(?x).*\[(?P<device>/.*)\]
        ",
        )
        .unwrap();
    }

    // Do NOT do
    //    let strvalue = value.to_string();
    // that will return a string delimited with quote characters.
    let strvalue: String = match value {
        Value::String(str) => str.to_string(),
        _ => value.to_string(),
    };
    if key == SOURCE_KEY {
        if let Some(caps) = RE_UDEVPATH.captures(&strvalue) {
            return format!("/dev{}", &caps["device"]);
        };
    }
    strvalue
}

const KEYS: &[&str] = &[TARGET_KEY, SOURCE_KEY, FSTYPE_KEY];

/// Convert the json map entry to a hashmap of strings
/// with source, target and fstype key-value pairs only.
/// The source field returned from findmnt is converted
/// to the /dev/xxx form if required.
fn jsonmap_to_hashmap(json_map: &serde_json::Map<String, Value>) -> HashMap<String, String> {
    let mut hmap: HashMap<String, String> = HashMap::new();
    for (key, value) in json_map {
        if KEYS.contains(&key.as_str()) {
            hmap.insert(key.clone(), key_adjusted_value(key, value));
        }
    }
    hmap
}

/// This function recurses over the de-serialised JSON returned by findmnt,
/// finding entries which have key-value pair's matching the
/// filter key-value pair, and populates a vector with those
/// entries as hashmaps of strings.
///
/// The assumptions made on the structure are:
///  1. An object has keys named "target" and "source" for a mount point.
///  2. An object may contain nested arrays of objects.
///
/// The search is deliberately generic (and hence slower) in an attempt to
/// be more robust to future changes in findmnt.
fn filter_findmnt(
    json_val: &serde_json::value::Value,
    filter: &Filter,
    results: &mut Vec<HashMap<String, String>>,
) {
    match json_val {
        Value::Array(json_array) => {
            for jsonvalue in json_array {
                filter_findmnt(jsonvalue, filter, results);
            }
        }
        Value::Object(json_map) => {
            if let Some(value) = json_map.get(filter.key) {
                if filter.value == value || filter.value == key_adjusted_value(filter.key, value) {
                    results.push(jsonmap_to_hashmap(json_map));
                }
            }
            // If the object has arrays, then the assumption is that they are
            // arrays of objects.
            for (_, jsonvalue) in json_map {
                if jsonvalue.is_array() {
                    filter_findmnt(jsonvalue, filter, results);
                }
            }
        }
        jvalue => {
            warn!("Unexpected json type {}", jvalue);
        }
    };
}

/// findmnt executable name.
const FIND_MNT: &str = "findmnt";
/// findmnt arguments, we only want source, target and filesystem type fields.
const FIND_MNT_ARGS: [&str; 3] = ["-J", "-o", "SOURCE,TARGET,FSTYPE"];

/// Execute the Linux utility findmnt, collect the json output,
/// invoke the filter function and return the filtered results.
async fn findmnt(params: Filter<'_>) -> Result<Vec<HashMap<String, String>>, DeviceError> {
    let output = tokio::process::Command::new(FIND_MNT)
        .args(FIND_MNT_ARGS)
        .output()
        .await?;

    if output.status.success() {
        let json_str = String::from_utf8(output.stdout)?;
        let json: Value = serde_json::from_str(&json_str)?;
        let mut results: Vec<HashMap<String, String>> = Vec::new();
        filter_findmnt(&json, &params, &mut results);
        Ok(results)
    } else {
        Err(DeviceError::new(String::from_utf8(output.stderr)?.as_str()))
    }
}

/// Use the Linux utility findmnt to find the name of the device mounted at a
/// directory or block special file, if any.
/// mount_path is the path a device is mounted on.
pub(crate) async fn get_devicepath(mount_path: &str) -> Result<Option<String>, DeviceError> {
    let tgt_filter = Filter {
        key: TARGET_KEY,
        value: mount_path,
    };
    let sources = findmnt(tgt_filter).await?;
    {
        match sources.len() {
            0 => Ok(None),
            1 => {
                if let Some(devicepath) = sources[0].get(SOURCE_KEY) {
                    Ok(Some(devicepath.to_string()))
                } else {
                    Err(DeviceError::new("missing source field"))
                }
            }
            _ => {
                // should be impossible ...
                warn!(
                    "multiple sources mounted on target {:?}->{}",
                    sources, mount_path
                );
                Err(DeviceError::new(
                    format!("multiple devices mounted at {mount_path}").as_str(),
                ))
            }
        }
    }
}

/// Use the Linux utility findmnt to find the mount paths for a block device,
/// if any.
/// device_path is the path to the device for example "/dev/sda1"
pub(crate) async fn get_mountpaths(device_path: &str) -> Result<Vec<DeviceMount>, DeviceError> {
    let dev_filter = Filter {
        key: SOURCE_KEY,
        value: device_path,
    };
    match findmnt(dev_filter).await {
        Ok(results) => {
            let mut mountpaths: Vec<DeviceMount> = Vec::new();
            for entry in results {
                if let Some(mountpath) = entry.get(TARGET_KEY) {
                    if let Some(fstype) = entry.get(FSTYPE_KEY) {
                        mountpaths.push(DeviceMount::new(
                            mountpath.to_string(),
                            Fs::from_str(fstype)
                                .unwrap_or(Fs::Unsupported(fstype.to_string()))
                                .into(),
                        ))
                    } else {
                        error!("Missing fstype for {}", mountpath);
                        mountpaths.push(DeviceMount::new(
                            mountpath.to_string(),
                            Fs::Unsupported("".to_string()).into(),
                        ))
                    }
                } else {
                    warn!("missing target field {:?}", entry);
                }
            }
            Ok(mountpaths)
        }
        Err(e) => Err(e),
    }
}

const DRIVER_NAME: &str = "driverName";
const VOLUME_HANDLE: &str = "volumeHandle";
const METADATA_FILE: &str = "vol_data.json";
const DEVICE_PATTERN: &str = "nvme";

/// Find stale mounts in the node plugin's mount namespace without spawning findmnt.
pub(crate) fn force_unstage_mountpaths(
    device_path: Option<&str>,
    volume_id: &str,
    kubelet_path: &str,
) -> Result<Vec<DeviceMount>, DeviceError> {
    let mounts = std::fs::read_to_string("/proc/self/mountinfo")?;
    scan_mountinfo(&mounts, device_path, volume_id, Path::new(kubelet_path))
}

fn scan_mountinfo(
    mounts: &str,
    device_path: Option<&str>,
    volume_id: &str,
    kubelet_path: &Path,
) -> Result<Vec<DeviceMount>, DeviceError> {
    let driver = csi_plugin_name();
    let mut mountpaths = Vec::new();
    for line in mounts.lines() {
        let mount = MountInfoEntry::parse(line)?;
        let block_device = mount.block_device_name();
        let target = Path::new(mount.target.as_ref());
        let matches = match device_path {
            Some(device) => {
                mount.source == device
                    || block_device.is_some_and(|name| device.strip_prefix("/dev/") == Some(name))
            }
            None => {
                block_device
                    .unwrap_or(&mount.source)
                    .contains(DEVICE_PATTERN)
                    && target.starts_with(kubelet_path)
                    && csi_mount_metadata_path(target, kubelet_path, block_device.is_some())
                        .is_some_and(|path| csi_volume_matches(&path, &driver, volume_id))
            }
        };
        // A tmpfs root can also name a regular file. Only treat a block special
        // file as a bind mount of a device node.
        if matches
            && (block_device.is_none()
                || std::fs::symlink_metadata(target)?
                    .file_type()
                    .is_block_device())
        {
            let fstype =
                Fs::from_str(mount.fstype).unwrap_or_else(|_| Fs::Unsupported(mount.fstype.into()));
            mountpaths.push(DeviceMount::new(mount.target.into_owned(), fstype.into()));
        }
    }
    Ok(mountpaths)
}

struct MountInfoEntry<'a> {
    root: Cow<'a, str>,
    target: Cow<'a, str>,
    fstype: &'a str,
    source: Cow<'a, str>,
}

impl<'a> MountInfoEntry<'a> {
    fn parse(line: &'a str) -> Result<Self, DeviceError> {
        let invalid = || DeviceError::new(format!("Invalid mountinfo entry: {line}"));
        let (mount, filesystem) = line.split_once(" - ").ok_or_else(invalid)?;
        let mut mount = mount.split_ascii_whitespace().skip(3);
        let root = mount.next().ok_or_else(invalid)?;
        let target = mount.next().ok_or_else(invalid)?;
        mount.next().ok_or_else(invalid)?; // mount options

        let mut filesystem = filesystem.split_ascii_whitespace();
        let fstype = filesystem.next().ok_or_else(invalid)?;
        let source = filesystem.next().ok_or_else(invalid)?;
        filesystem.next().ok_or_else(invalid)?; // superblock options

        Ok(Self {
            root: decode_mountinfo_path(root)?,
            target: decode_mountinfo_path(target)?,
            fstype,
            source: decode_mountinfo_path(source)?,
        })
    }

    // Bind mounts of /dev/<device> retain the filesystem source (such as udev)
    // and record the device name in the root field.
    fn block_device_name(&self) -> Option<&str> {
        if matches!(self.fstype, "devtmpfs" | "tmpfs") {
            self.root
                .strip_prefix('/')
                .filter(|name| !name.is_empty() && !name.contains('/'))
        } else {
            None
        }
    }
}

fn decode_mountinfo_path(path: &str) -> Result<Cow<'_, str>, DeviceError> {
    let Some((start, escaped)) = path.split_once('\\') else {
        return Ok(Cow::Borrowed(path));
    };

    let mut decoded = String::with_capacity(path.len());
    decoded.push_str(start);
    for part in escaped.split('\\') {
        let ch = match part.as_bytes().get(..3) {
            Some(b"040") => ' ',
            Some(b"011") => '\t',
            Some(b"012") => '\n',
            Some(b"134") => '\\',
            _ => return Err(DeviceError::new(format!("Invalid mountinfo path: {path}"))),
        };
        decoded.push(ch);
        decoded.push_str(&part[3..]);
    }
    Ok(Cow::Owned(decoded))
}

fn csi_mount_metadata_path(target: &Path, kubelet_path: &Path, block: bool) -> Option<PathBuf> {
    if block {
        let devices = kubelet_path.join("plugins/kubernetes.io/csi/volumeDevices");
        if let Ok(relative) = target.strip_prefix(&devices) {
            let mut components = relative.iter();
            // Kubelet publishes to publish/<volume>/<pod> and bind mounts
            // that path to <volume>/dev/<pod>. Both share the volume metadata.
            if let (Some(first), Some(second), Some(_), None) = (
                components.next(),
                components.next(),
                components.next(),
                components.next(),
            ) {
                let volume = if first == "publish" {
                    second
                } else if second == "dev" {
                    first
                } else {
                    return None;
                };
                return Some(devices.join(volume).join("data").join(METADATA_FILE));
            }
        }
    }
    target.parent().map(|parent| parent.join(METADATA_FILE))
}

fn csi_volume_matches(path: &Path, driver: &str, volume_id: &str) -> bool {
    let Ok(file) = std::fs::File::open(path) else {
        return false;
    };
    let reader = std::io::BufReader::new(file);
    let Ok(data) = serde_json::from_reader::<_, Value>(reader) else {
        return false;
    };
    data.get(DRIVER_NAME).and_then(Value::as_str) == Some(driver)
        && data.get(VOLUME_HANDLE).and_then(Value::as_str) == Some(volume_id)
}

#[cfg(test)]
mod mountinfo_tests {
    use super::*;

    #[test]
    fn parses_raw_block_bind_mount() {
        let mount = MountInfoEntry::parse(
            r"71 30 0:6 /nvme0n1 /kubelet/disk\040one rw shared:4 - devtmpfs udev rw",
        )
        .unwrap();

        assert_eq!(mount.block_device_name(), Some("nvme0n1"));
        assert_eq!(mount.target, "/kubelet/disk one");
    }

    #[test]
    fn matches_filesystem_mounts_and_returns_parse_errors() {
        let mount = "72 30 259:0 / /kubelet/staging rw - ext4 /dev/nvme0n1 rw";
        let kubelet = Path::new("/kubelet");
        let paths = scan_mountinfo(mount, Some("/dev/nvme0n1"), "volume", kubelet).unwrap();
        assert_eq!(paths[0].mount_path(), "/kubelet/staging");

        let bad_snapshot = format!("{mount}\ninvalid mountinfo");
        assert!(scan_mountinfo(&bad_snapshot, Some("/dev/nvme0n1"), "volume", kubelet).is_err());
    }

    #[test]
    fn locates_raw_block_metadata_for_both_kubelet_mounts() {
        let kubelet = Path::new("/var/lib/kubelet");
        let devices = kubelet.join("plugins/kubernetes.io/csi/volumeDevices");
        let expected = devices.join("pvc-volume/data/vol_data.json");
        for target in ["publish/pvc-volume/pod", "pvc-volume/dev/pod"] {
            assert_eq!(
                csi_mount_metadata_path(&devices.join(target), kubelet, true).unwrap(),
                expected
            );
        }
    }
}
