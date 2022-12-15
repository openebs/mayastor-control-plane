use std::{
    collections::HashMap,
    convert::{From, TryFrom},
    path::Path,
};

use nvmeadm::{
    error::NvmeError,
    nvmf_discovery::{disconnect, ConnectArgsBuilder},
};

use csi_driver::PublishParams;
use glob::glob;
use nvmeadm::nvmf_subsystem::Subsystem;
use regex::Regex;
use tracing::debug;
use udev::{Device, Enumerator};
use url::Url;
use uuid::Uuid;

use crate::{
    config::{config, NvmeConfig, NvmeParseParams},
    dev::util::extract_uuid,
    match_dev::match_nvmf_device,
};

use super::{Attach, Detach, DeviceError, DeviceName};

lazy_static::lazy_static! {
    static ref DEVICE_REGEX: Regex = Regex::new(r"nvme(\d{1,3})n1").unwrap();
}

pub(super) struct NvmfAttach {
    host: String,
    port: u16,
    uuid: Uuid,
    nqn: String,
    io_timeout: Option<u32>,
    nr_io_queues: Option<u32>,
    ctrl_loss_tmo: Option<u32>,
    keep_alive_tmo: Option<u32>,
    hostnqn: Option<String>,
}

impl NvmfAttach {
    #[allow(clippy::too_many_arguments)]
    fn new(
        host: String,
        port: u16,
        uuid: Uuid,
        nqn: String,
        nr_io_queues: Option<u32>,
        ctrl_loss_tmo: Option<u32>,
        keep_alive_tmo: Option<u32>,
        hostnqn: Option<String>,
    ) -> NvmfAttach {
        NvmfAttach {
            host,
            port,
            uuid,
            nqn,
            io_timeout: None,
            nr_io_queues,
            ctrl_loss_tmo,
            keep_alive_tmo,
            hostnqn,
        }
    }

    fn get_device(&self) -> Result<Option<Device>, DeviceError> {
        let key: String = format!("uuid.{}", self.uuid);
        let mut enumerator = Enumerator::new()?;

        enumerator.match_subsystem("block")?;
        enumerator.match_property("DEVTYPE", "disk")?;

        for device in enumerator.scan_devices()? {
            if match_nvmf_device(&device, &key).is_some() {
                return Ok(Some(device));
            }
        }

        Ok(None)
    }
}

impl TryFrom<&Url> for NvmfAttach {
    type Error = DeviceError;

    fn try_from(url: &Url) -> Result<Self, Self::Error> {
        let host = url
            .host_str()
            .ok_or_else(|| DeviceError::new("missing host"))?;

        let segments: Vec<&str> = url
            .path_segments()
            .ok_or_else(|| DeviceError::new("no path segment"))?
            .collect();

        let uuid = get_volume_uuid_from_uri(url)?;

        let port = url.port().unwrap_or(4420);

        let nr_io_queues = config().nvme().nr_io_queues();
        let ctrl_loss_tmo = config().nvme().ctrl_loss_tmo();
        let keep_alive_tmo = config().nvme().keep_alive_tmo();

        let hash_query: HashMap<_, _> = url.query_pairs().collect();
        let hostnqn = hash_query.get("hostnqn").map(ToString::to_string);

        Ok(NvmfAttach::new(
            host.to_string(),
            port,
            uuid,
            segments[0].to_string(),
            nr_io_queues,
            ctrl_loss_tmo,
            keep_alive_tmo,
            hostnqn,
        ))
    }
}

#[tonic::async_trait]
impl Attach for NvmfAttach {
    async fn parse_parameters(
        &mut self,
        context: &HashMap<String, String>,
    ) -> Result<(), DeviceError> {
        let publish_context = PublishParams::try_from(context)
            .map_err(|error| DeviceError::new(&error.to_string()))?;

        if let Some(val) = publish_context.io_timeout() {
            self.io_timeout = Some(*val);
        }
        if let Some(val) = publish_context.ctrl_loss_tmo() {
            self.ctrl_loss_tmo = Some(*val);
        }

        // todo: fold the nvme params into a node-specific publish context?
        let nvme_config = NvmeConfig::try_from(context as NvmeParseParams)?;

        if let Some(nr_io_queues) = nvme_config.nr_io_queues() {
            self.nr_io_queues = Some(nr_io_queues);
        }
        if let Some(keep_alive_tmo) = nvme_config.keep_alive_tmo() {
            self.keep_alive_tmo = Some(keep_alive_tmo);
        }
        Ok(())
    }

    async fn attach(&self) -> Result<(), DeviceError> {
        // Get the subsystem, if not found issue a connect.
        match Subsystem::get(self.host.as_str(), &self.port, self.nqn.as_str()) {
            Ok(_) => Ok(()),
            Err(NvmeError::SubsystemNotFound { .. }) => {
                // The default reconnect delay in linux kernel is set to 10s. Use the
                // same default value unless the timeout is less or equal to 10.
                let reconnect_delay = match self.io_timeout {
                    Some(io_timeout) => {
                        if io_timeout <= 10 {
                            Some(1)
                        } else {
                            Some(10)
                        }
                    }
                    None => None,
                };
                let ca = ConnectArgsBuilder::default()
                    .traddr(&self.host)
                    .trsvcid(self.port.to_string())
                    .nqn(&self.nqn)
                    .ctrl_loss_tmo(self.ctrl_loss_tmo)
                    .reconnect_delay(reconnect_delay)
                    .nr_io_queues(self.nr_io_queues)
                    .hostnqn(self.hostnqn.clone())
                    .keep_alive_tmo(self.keep_alive_tmo)
                    .build()?;
                return match ca.connect() {
                    // Should we remove this arm?
                    Err(NvmeError::ConnectInProgress) => Ok(()),
                    Err(err) => Err(err.into()),
                    Ok(_) => Ok(()),
                };
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn find(&self) -> Result<Option<DeviceName>, DeviceError> {
        self.get_device().map(|device_maybe| match device_maybe {
            Some(device) => device
                .property_value("DEVNAME")
                .map(|path| path.to_str().unwrap().into()),
            None => None,
        })
    }

    async fn fixup(&self) -> Result<(), DeviceError> {
        if let Some(io_timeout) = self.io_timeout {
            let device = self
                .get_device()?
                .ok_or_else(|| DeviceError::new("NVMe device not found"))?;
            let dev_name = device.sysname().to_str().unwrap();
            let major = DEVICE_REGEX
                .captures(dev_name)
                .ok_or_else(|| {
                    DeviceError::new(&format!(
                        "NVMe device \"{}\" does not match \"{}\"",
                        dev_name, *DEVICE_REGEX,
                    ))
                })?
                .get(1)
                .unwrap()
                .as_str();
            let pattern = format!("/sys/class/nvme/nvme{}/nvme*n1/queue", major);
            let path = glob(&pattern)
                .unwrap()
                .next()
                .ok_or_else(|| {
                    DeviceError::new(&format!(
                        "failed to look up sysfs device directory \"{}\"",
                        pattern,
                    ))
                })?
                .map_err(|_| {
                    DeviceError::new(&format!(
                        "IO error when reading device directory \"{}\"",
                        pattern
                    ))
                })?;
            // If the timeout was higher than nexus's timeout then IOs could
            // error out earlier than they should. Therefore we should make sure
            // that timeouts in the nexus are set to a very high value.
            debug!(
                "Setting IO timeout on \"{}\" to {}s",
                path.to_string_lossy(),
                io_timeout
            );
            sysfs::write_value(&path, "io_timeout", 1000 * io_timeout)?;
        }
        Ok(())
    }
}

pub(super) struct NvmfDetach {
    name: DeviceName,
    nqn: String,
}

impl NvmfDetach {
    pub(super) fn new(name: DeviceName, nqn: String) -> NvmfDetach {
        NvmfDetach { name, nqn }
    }
}

#[tonic::async_trait]
impl Detach for NvmfDetach {
    async fn detach(&self) -> Result<(), DeviceError> {
        if disconnect(&self.nqn)? == 0 {
            return Err(DeviceError::from(format!(
                "nvmf disconnect {} failed: no device found",
                self.nqn
            )));
        }

        Ok(())
    }

    fn devname(&self) -> DeviceName {
        self.name.clone()
    }
}

/// Set the nvme_core module IO timeout
/// (note, this is a system-wide parameter)
pub(crate) fn set_nvmecore_iotimeout(io_timeout_secs: u32) -> Result<(), std::io::Error> {
    let path = Path::new("/sys/module/nvme_core/parameters");
    debug!(
        "Setting nvme_core IO timeout on \"{}\" to {}s",
        path.to_string_lossy(),
        io_timeout_secs
    );
    sysfs::write_value(path, "io_timeout", io_timeout_secs)?;
    Ok(())
}

/// Extract uuid from Url.
pub(crate) fn get_volume_uuid_from_uri(url: &Url) -> Result<Uuid, DeviceError> {
    let segments: Vec<&str> = url
        .path_segments()
        .ok_or_else(|| DeviceError::new("no path segment"))?
        .collect();

    if segments.is_empty() || (segments.len() == 1 && segments[0].is_empty()) {
        return Err(DeviceError::new("no path segment"));
    }

    if segments.len() > 1 {
        return Err(DeviceError::new("too many path segments"));
    }

    let components: Vec<&str> = segments[0].split(':').collect();

    if components.len() != 2 {
        return Err(DeviceError::new("invalid NQN"));
    }

    extract_uuid(components[1])
        .map_err(|error| DeviceError::from(format!("invalid UUID: {}", error)))
}
