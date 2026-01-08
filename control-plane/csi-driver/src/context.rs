use crate::filesystem::FileSystem;
use k8s_openapi::api::core::v1::PersistentVolumeClaim;
use kube::api::{Patch, PatchParams};
use kube::{Api, Client};
use parse_size::parse_size;
use regex::Regex;
use std::{
    collections::HashMap,
    convert::AsRef,
    num::ParseIntError,
    str::{FromStr, ParseBoolError},
};
use stor_port::platform;
use stor_port::types::v0::openapi::models::VolumeShareProtocol;
use strum_macros::{AsRefStr, Display, EnumString};
use tracing::log::warn;
use tracing::{debug, trace};
use utils::K8S_STS_PVC_NAMING_REGEX;

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
    #[strum(serialize = "maxSnapshots")]
    MaxSnapshots,
    #[strum(serialize = "quiesceFs")]
    QuiesceFs,
    #[strum(serialize = "poolAffinityTopologyLabel")]
    PoolAffinityTopologyLabel,
    #[strum(serialize = "poolAffinityTopologyKey")]
    PoolAffinityTopologyKey,
    #[strum(serialize = "poolHasTopologyKey")]
    PoolHasTopologyKey,
    #[strum(serialize = "nodeAffinityTopologyLabel")]
    NodeAffinityTopologyLabel,
    #[strum(serialize = "nodeHasTopologyKey")]
    NodeHasTopologyKey,
    #[strum(serialize = "nodeSpreadTopologyKey")]
    NodeSpreadTopologyKey,
    #[strum(serialize = "encrypted")]
    Encrypted,
    #[strum(serialize = "formatOptions")]
    FormatOptions,
    #[strum(serialize = "overrideGlobalFormatOpts")]
    OverrideGlobalFormatOpts,
    #[strum(serialize = "poolClusterSize")]
    PoolClusterSize,
    #[strum(serialize = "rwxBlock")]
    RwxBlock,
}
impl Parameters {
    fn parse_human_time(
        value: Option<&String>,
    ) -> Result<Option<humantime::Duration>, humantime::DurationError> {
        Ok(match value {
            Some(value) => humantime::Duration::from_str(value).map(Some)?,
            None => None,
        })
    }

    /// Parses storage class label when passed as a string
    /// PoolAffinityTopologyLabel: |
    ///   A1: B1
    ///   CE : D1
    /// The input value to this function is like Some("A1: B1\nCE  : D1\n")
    /// and it will be parsed as {"A1": "B1", "CE ": "D1"}
    /// PoolHasTopologyKey: |
    ///   A1
    ///   CE
    ///  The input value to this function is like Some("A1\nCE \n")
    ///  and it will be parsed as {"A1": "", "CE ": ""}
    fn parse_topology_param(
        value: Option<&String>,
    ) -> Result<Option<HashMap<String, String>>, tonic::Status> {
        Ok(match value {
            Some(labels) => {
                let mut result_map = HashMap::new();
                for label in labels.split('\n') {
                    if !label.is_empty() {
                        if label.contains(':') {
                            let parts: Vec<&str> = label.split(':').map(|s| s.trim()).collect();
                            if let [key, val, ..] = parts.as_slice() {
                                result_map.insert(key.to_string(), val.to_string());
                            } else {
                                return Err(tonic::Status::invalid_argument(format!(
                                    "Invalid label : {value:?}"
                                )));
                            }
                        } else {
                            result_map.insert(label.to_string(), "".to_string());
                        }
                    }
                }
                Some(result_map)
            }
            None => None,
        })
    }

    /// Parses the value for affinity key
    /// PoolAffinityTopologyKey: |
    ///   A1
    ///   CE
    ///  The input value to this function is like Some("A1\nCE \n")
    ///  and it will be parsed as {"A1": "", "CE ": ""}
    /// and it will be parsed as ["A1", "CE "]
    fn parse_topology_param_vec(
        value: Option<&String>,
    ) -> Result<Option<Vec<String>>, tonic::Status> {
        Ok(match value {
            Some(labels) => {
                let mut result_vec = Vec::new();
                for label in labels.split('\n') {
                    if !label.is_empty() {
                        result_vec.push(label.to_string())
                    } else {
                        return Err(tonic::Status::invalid_argument(format!(
                            "Invalid affinity key : {value:?}"
                        )));
                    }
                }
                Some(result_vec)
            }
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
    /// Parse the value for `Self::RwxBlock`
    pub fn rwx_block(value: Option<&String>) -> Result<Option<bool>, ParseBoolError> {
        Self::parse_bool(value)
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
    /// Parse the value for `Self::PoolAffinityTopologyLabel`.
    pub fn pool_affinity_topology_label(
        value: Option<&String>,
    ) -> Result<Option<HashMap<String, String>>, tonic::Status> {
        Self::parse_topology_param(value)
    }
    /// Parse the value for `Self::PoolAffinityTopologyKey`.
    pub fn pool_affinity_topology_key(
        value: Option<&String>,
    ) -> Result<Option<Vec<String>>, tonic::Status> {
        Self::parse_topology_param_vec(value)
    }
    /// Parse the value for `Self::PoolHasTopologyKey`.
    pub fn pool_has_topology_key(
        value: Option<&String>,
    ) -> Result<Option<HashMap<String, String>>, tonic::Status> {
        Self::parse_topology_param(value)
    }
    /// Parse the value for `Self::NodeAffinityTopologyLabel`.
    pub fn node_affinity_topology_label(
        value: Option<&String>,
    ) -> Result<Option<HashMap<String, String>>, tonic::Status> {
        Self::parse_topology_param(value)
    }
    /// Parse the value for `Self::NodeHasTopologyKey`.
    pub fn node_has_topology_key(
        value: Option<&String>,
    ) -> Result<Option<HashMap<String, String>>, tonic::Status> {
        Self::parse_topology_param(value)
    }
    /// Parse the value for `Self::NodeSpreadTopologyKey`.
    pub fn node_spread_topology_key(
        value: Option<&String>,
    ) -> Result<Option<HashMap<String, String>>, tonic::Status> {
        Self::parse_topology_param(value)
    }
    /// Parse the value for `Self::MaxSnapshots`.
    pub fn max_snapshots(value: Option<&String>) -> Result<Option<u32>, ParseIntError> {
        Self::parse_u32(value)
    }
    /// Parse the value for `Self::Encrypted`.
    pub fn encrypted(value: Option<&String>) -> Result<Option<bool>, ParseBoolError> {
        Self::parse_bool(value)
    }
    /// Parse the value for `Self::OverrideGlobalFormatOpts`
    pub fn override_global_format_opts(
        value: Option<&String>,
    ) -> Result<Option<bool>, ParseBoolError> {
        Self::parse_bool(value)
    }
    /// Parse the value for `Self::PoolClusterSize`
    pub fn pool_cluster_size(value: Option<&String>) -> Result<Option<u64>, tonic::Status> {
        if let Some(value) = value {
            let alloc_unit_bytes = parse_size(value).ok();
            if alloc_unit_bytes.is_none() {
                return Err(tonic::Status::invalid_argument(format!(
                    "Invalid `PoolClusterSize` value {value:?}, expected a number with capacity unit suffix"
                )));
            }
            return Ok(alloc_unit_bytes);
        }
        Ok(None)
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
        let nvme_io_timeout =
            Parameters::nvme_io_timeout(args.get(Parameters::NvmeIoTimeout.as_ref()))
                .map_err(|_| tonic::Status::invalid_argument("Invalid nvme_io_timeout"))?;
        let ctrl_loss_tmo =
            Parameters::ctrl_loss_tmo(args.get(Parameters::NvmeCtrlLossTmo.as_ref()))
                .map_err(|_| tonic::Status::invalid_argument("Invalid ctrl_loss_tmo"))?;
        let keep_alive_tmo =
            Parameters::keep_alive_tmo(args.get(Parameters::NvmeKeepAliveTmo.as_ref()))
                .map_err(|_| tonic::Status::invalid_argument("Invalid keep_alive_tmo"))?;
        let fs_id = Parameters::fs_id(args.get(Parameters::FsId.as_ref()))
            .map_err(|_| tonic::Status::invalid_argument("Invalid fs_id"))?;

        Ok(Self {
            io_timeout,
            nvme_io_timeout,
            ctrl_loss_tmo,
            keep_alive_tmo,
            fs_type,
            fs_id,
        })
    }
}

/// Ensure there's only 1 topology parameter enabled at a time.
pub(crate) fn validate_single_topology_param(
    pool_affinity_topology_label: &Option<HashMap<String, String>>,
    pool_has_topology_key: &Option<HashMap<String, String>>,
    node_affinity_topology_label: &Option<HashMap<String, String>>,
    node_has_topology_key: &Option<HashMap<String, String>>,
    node_spread_topology_key: &Option<HashMap<String, String>>,
) -> Result<(), tonic::Status> {
    let topology_params = [
        pool_affinity_topology_label.is_some(),
        pool_has_topology_key.is_some(),
        node_affinity_topology_label.is_some(),
        node_has_topology_key.is_some(),
        node_spread_topology_key.is_some(),
    ];

    let number_of_topology_params = topology_params.iter().filter(|&&x| x).count();

    if number_of_topology_params > 1 {
        return Err(tonic::Status::invalid_argument(
            "Multiple topology parameters are not allowed",
        ));
    }
    Ok(())
}

/// Validate the topology parameters.
/// Errors out of nodeSpreadTopologyKey has same values as of
/// node_has_topology_key/nodeAffinityTopologyLabel
pub(crate) fn validate_topology_params(
    map1: &Option<HashMap<String, String>>,
    map2: &Option<HashMap<String, String>>,
) -> Result<(), tonic::Status> {
    if let (Some(map1), Some(map2)) = (map1, map2) {
        let has_common_keys = map1.len() == map2.len() && map1.keys().any(|k| map2.contains_key(k));

        if has_common_keys {
            return Err(tonic::Status::invalid_argument(
                "Invalid node topology. `nodeSpreadTopologyKey` can't have same values as `nodeHasTopologyKey` or `nodeAffinityTopologyLabel`",
            ));
        }
    }
    Ok(())
}

const ANNOTATION_KEY: &str = "openebs.io/stsAffinityGroup";

/// Volume Creation parameters.
#[allow(dead_code)]
#[derive(Debug)]
pub struct CreateParams {
    publish_params: PublishParams,
    rwx_block: Option<bool>,
    share_protocol: VolumeShareProtocol,
    replica_count: u8,
    sts_affinity_group: Option<bool>,
    clone_fs_id_as_volume_id: Option<bool>,
    max_snapshots: Option<u32>,
    encrypted: Option<bool>,
    pvc_name: Option<String>,
    pvc_namespace: Option<String>,
    pool_cluster_size: Option<u64>,
    pool_affinity_topology_label: Option<HashMap<String, String>>,
    pool_affinity_topology_key: Option<Vec<String>>,
    pool_has_topology_key: Option<HashMap<String, String>>,
    node_affinity_topology_label: Option<HashMap<String, String>>,
    node_has_topology_key: Option<HashMap<String, String>>,
    node_spread_topology_key: Option<HashMap<String, String>>,
}
impl CreateParams {
    /// Get the `Parameters::RwxBlock` value.
    pub fn rwx_block(&self) -> &Option<bool> {
        &self.rwx_block
    }
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
    /// Get the `Parameters::CloneFsIdAsVolumeId` value.
    pub fn clone_fs_id_as_volume_id(&self) -> &Option<bool> {
        &self.clone_fs_id_as_volume_id
    }
    /// Get the `Parameters::MaxSnapshots` value.
    pub fn max_snapshots(&self) -> Option<u32> {
        self.max_snapshots
    }
    /// Get the `Parameters::Encrypted` value.
    pub fn encrypted(&self) -> Option<bool> {
        self.encrypted
    }
    /// Get the `Parameters::PoolClusterSize` value.
    pub fn pool_cluster_size(&self) -> Option<u64> {
        self.pool_cluster_size
    }
    /// Get the `Parameters::PoolAffinityTopologyLabel` value.
    pub fn pool_affinity_topology_label(&self) -> &Option<HashMap<String, String>> {
        &self.pool_affinity_topology_label
    }
    /// Get the `Parameters::PoolAffinityTopologyKey` value.
    pub fn pool_affinity_topology_key(&self) -> &Option<Vec<String>> {
        &self.pool_affinity_topology_key
    }
    /// Get the `Parameters::PoolHasTopologyKey` value.
    pub fn pool_has_topology_key(&self) -> &Option<HashMap<String, String>> {
        &self.pool_has_topology_key
    }
    /// Get the `Parameters::NodeAffinityTopologyLabel` value.
    pub fn node_affinity_topology_label(&self) -> &Option<HashMap<String, String>> {
        &self.node_affinity_topology_label
    }
    /// Get the `Parameters::NodeHasTopologyKey` value.
    pub fn node_has_topology_key(&self) -> &Option<HashMap<String, String>> {
        &self.node_has_topology_key
    }
    /// Get the `Parameters::NodeSpreadTopologyKey` value.
    pub fn node_spread_topology_key(&self) -> &Option<HashMap<String, String>> {
        &self.node_spread_topology_key
    }

    /// Get the sts_affinity_group name from annotations if exists else generate it.
    pub async fn sts_affinity_group(&self) -> Result<Option<StsAffinityGroupInfo>, tonic::Status> {
        if !self.sts_affinity_group.unwrap_or(false) {
            return Ok(None);
        }

        if platform::current_platform_type() != platform::PlatformType::K8s {
            debug!("Detected non-K8s platform, skipping annotation fetch for STS affinity group");
            return Ok(None);
        }

        let pvc_name = self
            .pvc_name
            .as_ref()
            .ok_or_else(|| tonic::Status::invalid_argument("PVC name is missing"))?;
        let pvc_namespace = self
            .pvc_namespace
            .as_ref()
            .ok_or_else(|| tonic::Status::invalid_argument("PVC namespace is missing"))?;

        let client = Client::try_default()
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        let pvc_api: Api<PersistentVolumeClaim> = Api::namespaced(client, pvc_namespace);

        let pvc = pvc_api
            .get(pvc_name)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;

        let annotation_value = pvc
            .metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get(ANNOTATION_KEY))
            .map(|val| val.to_string());

        let generated_value = generate_sts_affinity_group_name(pvc_name, pvc_namespace);

        // 1. If the annotation was set and the generated name is different, we should use the annotation value, this can happen at the point of restore, so we will just stick to the set value.
        // 2. If the annotation was set and the generated name is same, we should use the annotation value, this can happen at the point of import of the same volume.
        // 3. If the annotation was not set and the generated name is not None, we should use the generated value to patch, this would be at the first creation
        let (annotation_exists, final_name) = match (&annotation_value, &generated_value) {
            (Some(a_val), Some(gen_val)) => {
                if a_val != gen_val {
                    warn!(
                        "Conflicting STS affinity group names: annotation = {a_val}, generated = {gen_val}"
                    );
                }
                (true, a_val)
            }
            (Some(a_val), None) => (true, a_val),
            (None, Some(gen_val)) => (false, gen_val),
            (None, None) => return Ok(None),
        };

        Ok(Some(StsAffinityGroupInfo {
            pvc_name: pvc_name.to_string(),
            pvc_namespace: pvc_namespace.to_string(),
            name: final_name.to_string(),
            annotation_exists,
        }))
    }
}

/// Information about the STS affinity group.
pub struct StsAffinityGroupInfo {
    pvc_name: String,
    pvc_namespace: String,
    name: String,
    annotation_exists: bool,
}

impl StsAffinityGroupInfo {
    /// If sts_affinity_group was requested we should patch pvc with it.
    pub async fn set_sts_affinity_annotation(&self) -> Result<(), tonic::Status> {
        if self.annotation_exists {
            trace!(pvc.name=%self.pvc_name, pvc.stsAffinityGroup=%self.name(), "Annotation for STS affinity group already present, skipping patch");
            return Ok(());
        }
        let client = Client::try_default()
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;

        let pvc_api: Api<PersistentVolumeClaim> = Api::namespaced(client, &self.pvc_namespace);

        let patch = serde_json::json!({
            "metadata": {
                "annotations": {
                    ANNOTATION_KEY: self.name()
                }
            }
        });

        pvc_api
            .patch(
                &self.pvc_name,
                &PatchParams::default(),
                &Patch::Merge(&patch),
            )
            .await
            .map_err(|e| tonic::Status::internal(format!("Failed to patch PVC: {e}")))?;

        trace!(
            "Patched annotation: {ANNOTATION_KEY}: {} on the pvc {} in namespace {}",
            self.name(),
            self.pvc_name,
            self.pvc_namespace
        );

        Ok(())
    }

    pub fn name(&self) -> &String {
        &self.name
    }
}

impl TryFrom<&HashMap<String, String>> for CreateParams {
    type Error = tonic::Status;

    fn try_from(args: &HashMap<String, String>) -> Result<Self, Self::Error> {
        let publish_params = PublishParams::try_from(args)?;

        let rwx_block = Parameters::rwx_block(args.get(Parameters::RwxBlock.as_ref()))
            .map_err(|_| tonic::Status::invalid_argument("Invalid rwx block boolean"))?;

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

        let pvc_name = args.get(Parameters::PvcName.as_ref()).cloned();
        let pvc_namespace = args.get(Parameters::PvcNamespace.as_ref()).cloned();

        if sts_affinity_group == Some(true) && (pvc_name.is_none() || pvc_namespace.is_none()) {
            return Err(tonic::Status::invalid_argument(
                "`pvcName` and `pvcNamespace` must be present when `stsAffinityGroup` is true",
            ));
        }

        let clone_fs_id_as_volume_id = Parameters::clone_fs_id_as_volume_id(
            args.get(Parameters::CloneFsIdAsVolumeId.as_ref()),
        )
        .map_err(|_| tonic::Status::invalid_argument("Invalid clone_fs_id_as_volume_id"))?;

        let max_snapshots = Parameters::max_snapshots(args.get(Parameters::MaxSnapshots.as_ref()))
            .map_err(|_| {
                tonic::Status::invalid_argument("Invalid `maxSnapshots` value, expected an u32")
            })?;

        let encrypted =
            Parameters::encrypted(args.get(Parameters::Encrypted.as_ref())).map_err(|_| {
                tonic::Status::invalid_argument("Invalid `Encrypted` value, expected a boolean")
            })?;

        let pool_cluster_size =
            Parameters::pool_cluster_size(args.get(Parameters::PoolClusterSize.as_ref()))?;

        let pool_affinity_topology_label = Parameters::pool_affinity_topology_label(
            args.get(Parameters::PoolAffinityTopologyLabel.as_ref()),
        )
        .map_err(|_| tonic::Status::invalid_argument("Invalid pool_affinity_topology_label"))?;

        let pool_affinity_topology_key = Parameters::pool_affinity_topology_key(
            args.get(Parameters::PoolAffinityTopologyKey.as_ref()),
        )
        .map_err(|_| tonic::Status::invalid_argument("Invalid pool_affinity_topology_key"))?;

        let pool_has_topology_key =
            Parameters::pool_has_topology_key(args.get(Parameters::PoolHasTopologyKey.as_ref()))
                .map_err(|_| tonic::Status::invalid_argument("Invalid pool_has_topology_key"))?;

        let node_affinity_topology_label = Parameters::node_affinity_topology_label(
            args.get(Parameters::NodeAffinityTopologyLabel.as_ref()),
        )
        .map_err(|_| tonic::Status::invalid_argument("Invalid node_affinity_topology_label"))?;

        let node_has_topology_key =
            Parameters::node_has_topology_key(args.get(Parameters::NodeHasTopologyKey.as_ref()))
                .map_err(|_| tonic::Status::invalid_argument("Invalid node_has_topology_key"))?;
        let node_spread_topology_key = Parameters::node_spread_topology_key(
            args.get(Parameters::NodeSpreadTopologyKey.as_ref()),
        )
        .map_err(|_| tonic::Status::invalid_argument("Invalid node_spread_topology_key"))?;

        // add validation for one-to-one mapping of topology params to storage class
        validate_single_topology_param(
            &pool_affinity_topology_label,
            &pool_has_topology_key,
            &node_affinity_topology_label,
            &node_has_topology_key,
            &node_spread_topology_key,
        )?;

        validate_topology_params(&node_affinity_topology_label, &node_spread_topology_key)?;
        validate_topology_params(&node_has_topology_key, &node_spread_topology_key)?;

        Ok(Self {
            publish_params,
            rwx_block,
            share_protocol,
            replica_count,
            sts_affinity_group,
            clone_fs_id_as_volume_id,
            max_snapshots,
            encrypted,
            pvc_name,
            pvc_namespace,
            pool_cluster_size,
            pool_affinity_topology_label,
            pool_affinity_topology_key,
            pool_has_topology_key,
            node_affinity_topology_label,
            node_has_topology_key,
            node_spread_topology_key,
        })
    }
}

// Generate a affinity group name from the parameters.
// 1. Both pvc name and ns should be valid.
// 2. Pvc name should follow the sts pvc naming convention.
fn generate_sts_affinity_group_name(pvc_name: &String, pvc_ns: &String) -> Option<String> {
    let re = Regex::from_str(K8S_STS_PVC_NAMING_REGEX);
    if let Ok(regex) = re {
        if regex.is_match(pvc_name.as_str()) {
            if let Some(captures) = regex.captures(pvc_name.as_str()) {
                if let Some(common_binding) = captures.get(1) {
                    let name = format!("{pvc_ns}/{}", common_binding.as_str());
                    return Some(name);
                }
            }
        }
    }
    warn!("PVC Name: {pvc_name} is not a valid statefulset pvc naming format, not triggering statefulset volume replica anti-affinity");
    None
}

#[derive(EnumString, Clone, Debug, Eq, PartialEq)]
#[strum(serialize_all = "lowercase")]
pub enum QuiesceFsCandidate {
    None,
    Freeze,
}

/// Snapshot creation parameters.
#[allow(dead_code)]
#[derive(Debug)]
pub struct CreateSnapshotParams {
    queisce: Option<QuiesceFsCandidate>,
}
impl CreateSnapshotParams {
    /// Get the `Parameters::quiesce` value.
    pub fn quiesce(&self) -> &Option<QuiesceFsCandidate> {
        &self.queisce
    }
}
impl TryFrom<&HashMap<String, String>> for CreateSnapshotParams {
    type Error = tonic::Status;

    fn try_from(args: &HashMap<String, String>) -> Result<Self, Self::Error> {
        let queisce = match args.get(Parameters::QuiesceFs.as_ref()) {
            Some(fs) => QuiesceFsCandidate::from_str(fs.as_str())
                .map(Some)
                .map_err(|_| tonic::Status::invalid_argument("Invalid quiesce type"))?,
            None => None,
        };

        Ok(Self { queisce })
    }
}

#[cfg(test)]
mod tests {
    use crate::context::generate_sts_affinity_group_name;

    struct VolGrpTestEntry {
        pvc_name: String,
        pvc_namespace: String,
        result: Option<String>,
    }

    impl VolGrpTestEntry {
        fn new(pvc_name: &str, pvc_namespace: &str, result: Option<&str>) -> Self {
            Self {
                pvc_name: pvc_name.to_string(),
                pvc_namespace: pvc_namespace.to_string(),
                result: result.map(|s| s.to_string()),
            }
        }
    }

    #[test]
    fn ag_name_generator() {
        let vol_grp_test_entries: Vec<VolGrpTestEntry> = vec![
            VolGrpTestEntry::new("mongo-db-0", "default", Some("default/mongo-db")),
            VolGrpTestEntry::new("", "default", None),
            VolGrpTestEntry::new("mongo-db-0", "", Some("/mongo-db")),
            VolGrpTestEntry::new("", "", None),
            VolGrpTestEntry::new(
                "mongo-db-2424",
                "mayastor-123",
                Some("mayastor-123/mongo-db"),
            ),
            VolGrpTestEntry::new(
                "mongo-db-123-abcd-2",
                "default",
                Some("default/mongo-db-123-abcd"),
            ),
            VolGrpTestEntry::new("mongo-db-123-abcd", "xyz-12", None),
        ];

        for test_entry in vol_grp_test_entries {
            assert_eq!(
                generate_sts_affinity_group_name(&test_entry.pvc_name, &test_entry.pvc_namespace),
                test_entry.result
            );
        }
    }
}
