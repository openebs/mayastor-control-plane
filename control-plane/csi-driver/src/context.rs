use regex::Regex;
use std::{
    collections::HashMap,
    convert::AsRef,
    num::ParseIntError,
    str::{FromStr, ParseBoolError},
};
use stor_port::types::v0::openapi::models::VolumeShareProtocol;
use strum_macros::{AsRefStr, Display, EnumString};
use tracing::log::warn;
use utils::K8S_STS_PVC_NAMING_REGEX;

/// A type to enumerate used filesystems.
#[derive(EnumString, Clone, Debug, Eq, PartialEq)]
#[strum(serialize_all = "lowercase")]
pub enum FileSystem {
    Ext4,
    Xfs,
    DevTmpFs,
    Unsupported(String),
}

// Implement as ref for the FileSystem.
impl AsRef<str> for FileSystem {
    fn as_ref(&self) -> &str {
        match self {
            FileSystem::Ext4 => "ext4",
            FileSystem::Xfs => "xfs",
            FileSystem::DevTmpFs => "devtmpfs",
            FileSystem::Unsupported(inner) => inner,
        }
    }
}

// Implement Display for the filesystem
impl std::fmt::Display for FileSystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

/// Parse string protocol into REST API protocol enum.
pub fn parse_protocol(proto: Option<&String>) -> Result<VolumeShareProtocol, tonic::Status> {
    match proto.map(|s| s.as_str()) {
        None | Some("nvmf") => Ok(VolumeShareProtocol::Nvmf),
        _ => Err(tonic::Status::invalid_argument(format!(
            "Invalid protocol: {proto:?}"
        ))),
    }
}

/// The various volume context parameters.
#[derive(AsRefStr, EnumString, Display)]
#[strum(serialize_all = "camelCase")]
pub enum Parameters {
    IoTimeout,
    NvmeNrIoQueues,
    NvmeCtrlLossTmo,
    NvmeKeepAliveTmo,
    #[strum(serialize = "repl")]
    ReplicaCount,
    #[strum(serialize = "fsType")]
    FileSystem,
    #[strum(serialize = "protocol")]
    ShareProtocol,
    // TODO: Get the values from constant.
    #[strum(serialize = "csi.storage.k8s.io/pvc/name")]
    PvcName,
    #[strum(serialize = "csi.storage.k8s.io/pvc/namespace")]
    PvcNamespace,
    #[strum(serialize = "stsAffinityGroup")]
    StsAffinityGroup,
}
impl Parameters {
    fn parse_u32(value: Option<&String>) -> Result<Option<u32>, ParseIntError> {
        Ok(match value {
            Some(value) => value.parse::<u32>().map(Some)?,
            None => None,
        })
    }
    fn parse_bool(value: Option<&String>) -> Result<Option<bool>, ParseBoolError> {
        Ok(match value {
            Some(value) => value.parse::<bool>().map(Some)?,
            None => None,
        })
    }
    /// Parse the value for `Self::NvmeCtrlLossTmo`.
    pub fn ctrl_loss_tmo(value: Option<&String>) -> Result<Option<u32>, ParseIntError> {
        Self::parse_u32(value)
    }
    /// Parse the value for `Self::NvmeNrIoQueues`.
    pub fn nr_io_queues(value: Option<&String>) -> Result<Option<u32>, ParseIntError> {
        Self::parse_u32(value)
    }
    /// Parse the value for `Self::NvmeKeepAliveTmo`.
    pub fn keep_alive_tmo(value: Option<&String>) -> Result<Option<u32>, ParseIntError> {
        Self::parse_u32(value)
    }
    /// Parse the value for `Self::IoTimeout`.
    pub fn io_timeout(value: Option<&String>) -> Result<Option<u32>, ParseIntError> {
        Self::parse_u32(value)
    }
    /// Parse the value for `Self::StsAffinityGroup`
    pub fn sts_affinity_group(value: Option<&String>) -> Result<Option<bool>, ParseBoolError> {
        Self::parse_bool(value)
    }
}

/// Volume publish parameters.
#[allow(dead_code)]
pub struct PublishParams {
    io_timeout: Option<u32>,
    ctrl_loss_tmo: Option<u32>,
    keep_alive_tmo: Option<u32>,
    fs_type: Option<FileSystem>,
}
impl PublishParams {
    /// Get the `Parameters::IoTimeout` value.
    pub fn io_timeout(&self) -> &Option<u32> {
        &self.io_timeout
    }
    /// Get the `Parameters::NvmeCtrlLossTmo` value.
    pub fn ctrl_loss_tmo(&self) -> &Option<u32> {
        &self.ctrl_loss_tmo
    }
    /// Get the `Parameters::NvmeKeepAliveTmo` value.
    pub fn keep_alive_tmo(&self) -> &Option<u32> {
        &self.keep_alive_tmo
    }
    /// Convert `Self` into a publish context.
    pub fn into_context(self) -> HashMap<String, String> {
        let mut publish_context = HashMap::new();

        if let Some(io_timeout) = self.io_timeout() {
            publish_context.insert(Parameters::IoTimeout.to_string(), io_timeout.to_string());
        }
        if let Some(ctrl_loss_tmo) = self.ctrl_loss_tmo() {
            publish_context.insert(
                Parameters::NvmeCtrlLossTmo.to_string(),
                ctrl_loss_tmo.to_string(),
            );
        }
        if let Some(keep_alive_tmo) = self.keep_alive_tmo() {
            publish_context.insert(
                Parameters::NvmeKeepAliveTmo.to_string(),
                keep_alive_tmo.to_string(),
            );
        }

        publish_context
    }
}
impl TryFrom<&HashMap<String, String>> for PublishParams {
    type Error = tonic::Status;

    fn try_from(args: &HashMap<String, String>) -> Result<Self, Self::Error> {
        let fs_type = match args.get(Parameters::FileSystem.as_ref()) {
            Some(fs) => FileSystem::from_str(fs.as_str())
                .map(Some)
                .map_err(|_| tonic::Status::invalid_argument("Invalid filesystem type"))?,
            None => None,
        };

        let io_timeout = Parameters::io_timeout(args.get(Parameters::IoTimeout.as_ref()))
            .map_err(|_| tonic::Status::invalid_argument("Invalid I/O timeout"))?;
        let ctrl_loss_tmo =
            Parameters::ctrl_loss_tmo(args.get(Parameters::NvmeCtrlLossTmo.as_ref()))
                .map_err(|_| tonic::Status::invalid_argument("Invalid ctrl_loss_tmo"))?;
        let keep_alive_tmo =
            Parameters::keep_alive_tmo(args.get(Parameters::NvmeKeepAliveTmo.as_ref()))
                .map_err(|_| tonic::Status::invalid_argument("Invalid keep_alive_tmo"))?;

        Ok(Self {
            io_timeout,
            ctrl_loss_tmo,
            keep_alive_tmo,
            fs_type,
        })
    }
}

/// Volume Creation parameters.
#[allow(dead_code)]
pub struct CreateParams {
    publish_params: PublishParams,
    share_protocol: VolumeShareProtocol,
    replica_count: u8,
    sts_affinity_group: Option<String>,
}
impl CreateParams {
    /// Get the `Parameters::ShareProtocol` value.
    pub fn share_protocol(&self) -> VolumeShareProtocol {
        self.share_protocol
    }
    /// Get the `Parameters::ReplicaCount` value.
    pub fn replica_count(&self) -> u8 {
        self.replica_count
    }
    /// Get the final affinity group name, using the `Parameters::PvcName, Parameters::PvcNamespace,
    /// Parameters::AffinityGroup` values.
    pub fn sts_affinity_group(&self) -> &Option<String> {
        &self.sts_affinity_group
    }
}
impl TryFrom<&HashMap<String, String>> for CreateParams {
    type Error = tonic::Status;

    fn try_from(args: &HashMap<String, String>) -> Result<Self, Self::Error> {
        let publish_params = PublishParams::try_from(args)?;

        // Check storage protocol.
        let share_protocol = parse_protocol(args.get(Parameters::ShareProtocol.as_ref()))?;

        let replica_count = match args.get(Parameters::ReplicaCount.as_ref()) {
            Some(c) => match c.parse::<u8>() {
                Ok(c) => {
                    if c == 0 {
                        return Err(tonic::Status::invalid_argument(
                            "Replica count must be greater than zero",
                        ));
                    }
                    c
                }
                Err(_) => return Err(tonic::Status::invalid_argument("Invalid replica count")),
            },
            None => 1,
        };

        let sts_affinity_group =
            Parameters::sts_affinity_group(args.get(Parameters::StsAffinityGroup.as_ref()))
                .map_err(|_| {
                    tonic::Status::invalid_argument(
                        "Invalid `stsAffinityGroup` value, expected a bool",
                    )
                })?;

        let sts_affinity_group_name = if sts_affinity_group.unwrap_or(false) {
            generate_sts_affinity_group_name(
                &args.get(Parameters::PvcName.as_ref()).cloned(),
                &args.get(Parameters::PvcNamespace.as_ref()).cloned(),
            )
        } else {
            None
        };

        Ok(Self {
            publish_params,
            share_protocol,
            replica_count,
            sts_affinity_group: sts_affinity_group_name,
        })
    }
}

// Generate a affinity group name from the parameters.
// 1. Both pvc name and ns should be valid.
// 2. Pvc name should follow the sts pvc naming convention.
fn generate_sts_affinity_group_name(
    pvc_name: &Option<String>,
    pvc_ns: &Option<String>,
) -> Option<String> {
    match (pvc_name, pvc_ns) {
        (Some(pvc_name), Some(pvc_ns)) => {
            let re = Regex::from_str(K8S_STS_PVC_NAMING_REGEX);
            if let Ok(regex) = re {
                if regex.is_match(pvc_name.as_str()) {
                    if let Some(captures) = regex.captures(pvc_name.as_str()) {
                        if let Some(common_binding) = captures.get(1) {
                            return Some(format!("{pvc_ns}/{}", common_binding.as_str()));
                        }
                    }
                }
            }
            warn!("PVC Name: {pvc_name} is not a valid statefulset pvc naming format, not triggering statefulset volume replica anti-affinity");
            None
        }
        _ => {
            warn!("Invalid PVC Name: {pvc_name:?} or PVC Namespace: {pvc_ns:?}, not triggering statefulset volume replica anti-affinity");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::context::generate_sts_affinity_group_name;

    struct VolGrpTestEntry {
        pvc_name: Option<String>,
        pvc_namespace: Option<String>,
        result: Option<String>,
    }

    impl VolGrpTestEntry {
        fn new(pvc_name: Option<&str>, pvc_namespace: Option<&str>, result: Option<&str>) -> Self {
            Self {
                pvc_name: pvc_name.map(|s| s.to_string()),
                pvc_namespace: pvc_namespace.map(|s| s.to_string()),
                result: result.map(|s| s.to_string()),
            }
        }
    }

    #[test]
    fn ag_name_generator() {
        let vol_grp_test_entries: Vec<VolGrpTestEntry> = vec![
            VolGrpTestEntry::new(
                Some("mongo-db-0"),
                Some("default"),
                Some("default/mongo-db"),
            ),
            VolGrpTestEntry::new(None, Some("default"), None),
            VolGrpTestEntry::new(Some("mongo-db-0"), None, None),
            VolGrpTestEntry::new(None, None, None),
            VolGrpTestEntry::new(
                Some("mongo-db-2424"),
                Some("mayastor-123"),
                Some("mayastor-123/mongo-db"),
            ),
            VolGrpTestEntry::new(
                Some("mongo-db-123-abcd-2"),
                Some("default"),
                Some("default/mongo-db-123-abcd"),
            ),
            VolGrpTestEntry::new(Some("mongo-db-123-abcd"), Some("xyz-12"), None),
        ];

        for test_entry in vol_grp_test_entries {
            assert_eq!(
                generate_sts_affinity_group_name(&test_entry.pvc_name, &test_entry.pvc_namespace),
                test_entry.result
            );
        }
    }
}
