use crate::filesystem::FileSystem;
use stor_port::types::v0::openapi::models::VolumeShareProtocol;
use utils::K8S_STS_PVC_NAMING_REGEX;
use regex::Regex;
use std::{
    collections::HashMap,
    convert::AsRef,
    num::ParseIntError,
    str::{FromStr, ParseBoolError},
};
use serde_json::Value;
use strum_macros::{AsRefStr, Display, EnumString};
use tracing::log::warn;
use uuid::{Error as UuidError, Uuid};

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
    /// This value is now considered deprecated.
    IoTimeout,
    /// This value is used globally and not on a per volume context.
    /// todo: split Parameters into 2 separate enums?
    NvmeIoTimeout,
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
    #[strum(serialize = "cloneFsIdAsVolumeId")]
    CloneFsIdAsVolumeId,
    #[strum(serialize = "fsId")]
    FsId,
    #[strum(serialize = "PoolTopologyInclusion")]
    PoolTopologyInclusion,
}

// /// Placement pool topology used by volume operations.
// #[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
// pub enum Labelled { 
//     PoolTopology(LabelledTopology),
// }

// impl Default for Labelled {
//     fn default() -> Self {
//         Labelled::PoolTopology(LabelledTopology{ inclusion : HashMap::new() })
//     }
// }

// /// Volume placement topology using resource labels.
// #[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
// pub struct LabelledTopology {
//     /// Inclusive labels.
//     #[serde(default)]
//     pub inclusion: HashMap<String, String>,
// }

impl Parameters {
    fn parse_human_time(
        value: Option<&String>,
    ) -> Result<Option<humantime::Duration>, humantime::DurationError> {
        Ok(match value {
            Some(value) => humantime::Duration::from_str(value).map(Some)?,
            None => None,
        })
    }
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
    fn parse_uuid(value: Option<&String>) -> Result<Option<Uuid>, UuidError> {
        Ok(match value {
            Some(value) => value.parse::<Uuid>().map(Some)?,
            None => None,
        })
    }
    fn parse_map(value: Option<&String>) -> Result<Option<HashMap<String, String>>, serde_json::Error> {
        tracing::info!( " World cup  1  {:?}", value);
        Ok(match value {
            Some(json_str) => {
                let value: Value = serde_json::from_str(json_str)?;
                tracing::info!( " World cup  2 {:?}", value);
                // Convert Value to HashMap
                if let Value::Object(map) = value {
                    let hashmap: HashMap<String, String> = map
                        .into_iter()
                        .map(|(k, v)| (k, v.as_str().unwrap().to_string()))
                        .collect();
    
                    println!("{:?}", hashmap);
                    Some(hashmap)
                } else {
                    None
                }
            }
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
    /// Parse the value for `Self::IoTimeout`.
    pub fn nvme_io_timeout(
        value: Option<&String>,
    ) -> Result<Option<humantime::Duration>, humantime::DurationError> {
        Self::parse_human_time(value)
    }
    /// Parse the value for `Self::StsAffinityGroup`
    pub fn sts_affinity_group(value: Option<&String>) -> Result<Option<bool>, ParseBoolError> {
        Self::parse_bool(value)
    }
    /// Parse the value for `Self::CloneFsAsIdVolumeId`
    pub fn clone_fs_id_as_volume_id(
        value: Option<&String>,
    ) -> Result<Option<bool>, ParseBoolError> {
        Self::parse_bool(value)
    }
    /// Parse the value for `Self::FsId`
    pub fn fs_id(value: Option<&String>) -> Result<Option<Uuid>, UuidError> {
        Self::parse_uuid(value)
    }

    /// Parse the value for `Self::IoTimeout`.
    pub fn pool_topology_inclusion(value: Option<&String>) -> Result<Option<HashMap<String, String>>, serde_json::Error> {
        tracing::info!( " hai na bolo bolo {:?}", value);
        Self::parse_map(value)
    }
}

/// Volume publish parameters.
#[allow(dead_code)]
#[derive(Debug)]
pub struct PublishParams {
    io_timeout: Option<u32>,
    nvme_io_timeout: Option<humantime::Duration>,
    ctrl_loss_tmo: Option<u32>,
    keep_alive_tmo: Option<u32>,
    fs_type: Option<FileSystem>,
    fs_id: Option<Uuid>,
    pool_topology_inclusion: Option<HashMap<String, String>>,
}
impl PublishParams {
    /// Get the `Parameters::IoTimeout` value.
    pub fn io_timeout(&self) -> &Option<u32> {
        &self.io_timeout
    }
    /// Get the `Parameters::NvmeIoTimeout` value.
    pub fn nvme_io_timeout(&self) -> &Option<humantime::Duration> {
        &self.nvme_io_timeout
    }
    /// Get the `Parameters::NvmeCtrlLossTmo` value.
    pub fn ctrl_loss_tmo(&self) -> &Option<u32> {
        &self.ctrl_loss_tmo
    }
    /// Get the `Parameters::NvmeKeepAliveTmo` value.
    pub fn keep_alive_tmo(&self) -> &Option<u32> {
        &self.keep_alive_tmo
    }
    /// Get the `Parameters::FsId` value.
    pub fn fs_id(&self) -> &Option<Uuid> {
        &self.fs_id
    }
    /// Get the `Parameters::PoolTopologyInclusion` value.
    pub fn pool_topology_inclusion(&self) -> &Option<HashMap<String, String>> {
        &self.pool_topology_inclusion
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
        if let Some(fs_id) = self.fs_id() {
            publish_context.insert(Parameters::FsId.to_string(), fs_id.to_string());
        }
        // if let Some(pool_topology_inclusion) = self.pool_topology_inclusion() {
        //     if hashmap_to_json_string(pool_topology_inclusion).is_ok() {

        //     }

        //     publish_context.insert(Parameters::PoolTopologyInclusion.to_string(), pool_topology_inclusion.to_string());
        // }
        publish_context.insert("ashish".to_string(), "sinha".to_string());

        publish_context
    }
}

// fn hashmap_to_json_string(hashmap: &HashMap<String, String>) -> Result<String, serde_json::Error> {
//     let json_value: Value = hashmap.iter().map(|(k, v)| (k, v)).collect();
//     serde_json::to_string(&json_value)
// }

impl TryFrom<&HashMap<String, String>> for PublishParams {
    type Error = tonic::Status;

    fn try_from(args: &HashMap<String, String>) -> Result<Self, Self::Error> {

        tracing::info!(" Inside try_from {:?}", args);
        tracing::info!(" Value  is the {:?}", args.get(Parameters::PoolTopologyInclusion.as_ref()));
        
        let fs_type = match args.get(Parameters::FileSystem.as_ref()) {
            Some(fs) => FileSystem::from_str(fs.as_str())
                .map(Some)
                .map_err(|_| tonic::Status::invalid_argument("Invalid filesystem type"))?,
            None => None,
        };

        let io_timeout = Parameters::io_timeout(args.get(Parameters::IoTimeout.as_ref()))
            .map_err(|_| tonic::Status::invalid_argument("Invalid I/O timeout"))?;
        let nvme_io_timeout =
            Parameters::nvme_io_timeout(args.get(Parameters::NvmeIoTimeout.as_ref()))
                .map_err(|_| tonic::Status::invalid_argument("Invalid I/O timeout"))?;
        let ctrl_loss_tmo =
            Parameters::ctrl_loss_tmo(args.get(Parameters::NvmeCtrlLossTmo.as_ref()))
                .map_err(|_| tonic::Status::invalid_argument("Invalid ctrl_loss_tmo"))?;
        let keep_alive_tmo =
            Parameters::keep_alive_tmo(args.get(Parameters::NvmeKeepAliveTmo.as_ref()))
                .map_err(|_| tonic::Status::invalid_argument("Invalid keep_alive_tmo"))?;
        let fs_id = Parameters::fs_id(args.get(Parameters::FsId.as_ref()))
            .map_err(|_| tonic::Status::invalid_argument("Invalid fs_id"))?;

        let pool_topology_inclusion = Parameters::pool_topology_inclusion(args.get(Parameters::PoolTopologyInclusion.as_ref()))
            .map_err(|_| tonic::Status::invalid_argument("Invalid pool_topology_inclusion timeout"))?;

        Ok(Self {
            io_timeout,
            nvme_io_timeout,
            ctrl_loss_tmo,
            keep_alive_tmo,
            fs_type,
            fs_id,
            pool_topology_inclusion,
        })
    }
}

/// Volume Creation parameters.
#[allow(dead_code)]
#[derive(Debug)]
pub struct CreateParams {
    publish_params: PublishParams,
    share_protocol: VolumeShareProtocol,
    replica_count: u8,
    sts_affinity_group: Option<String>,
    clone_fs_id_as_volume_id: Option<bool>,
}
impl CreateParams {

    /// Get the `Parameters::PublishParams` value.
    pub fn publish_params(&self) -> &PublishParams {
        &self.publish_params
    }
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
    /// Get the `Parameters::CloneFsIdAsVolumeId` value.
    pub fn clone_fs_id_as_volume_id(&self) -> &Option<bool> {
        &self.clone_fs_id_as_volume_id
    }
}
impl TryFrom<&HashMap<String, String>> for CreateParams {
    type Error = tonic::Status;

    fn try_from(args: &HashMap<String, String>) -> Result<Self, Self::Error> {
        let publish_params = PublishParams::try_from(args)?;
        tracing::info!(" The publish_params are {:#?}", publish_params);

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

        let clone_fs_id_as_volume_id = Parameters::clone_fs_id_as_volume_id(
            args.get(Parameters::CloneFsIdAsVolumeId.as_ref()),
        )
        .map_err(|_| tonic::Status::invalid_argument("Invalid clone_fs_id_as_volume_id"))?;

        Ok(Self {
            publish_params,
            share_protocol,
            replica_count,
            sts_affinity_group: sts_affinity_group_name,
            clone_fs_id_as_volume_id,
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
