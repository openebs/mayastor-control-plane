use std::{collections::HashMap, num::ParseIntError, str::FromStr};
use stor_port::types::v0::openapi::models::VolumeShareProtocol;

/// The currently supported filesystems.
#[derive(strum_macros::AsRefStr, strum_macros::EnumString)]
#[strum(serialize_all = "lowercase")]
pub enum FileSystem {
    Ext4,
    Xfs,
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
#[derive(strum_macros::AsRefStr, strum_macros::Display)]
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
}
impl Parameters {
    fn parse_u32(value: Option<&String>) -> Result<Option<u32>, ParseIntError> {
        Ok(match value {
            Some(value) => value.parse::<u32>().map(Some)?,
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

        Ok(Self {
            publish_params,
            share_protocol,
            replica_count,
        })
    }
}
