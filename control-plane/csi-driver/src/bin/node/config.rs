use clap::ArgMatches;
use csi_driver::Parameters;
use heck::ToKebabCase;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
};

/// Command line arg name for `Parameters::NvmeNrIoQueues`.
pub fn nvme_nr_io_queues() -> String {
    Parameters::NvmeNrIoQueues.as_ref().to_kebab_case()
}
/// Command line arg name for `Parameters::NvmeCtrlLossTmo`.
pub fn nvme_ctrl_loss_tmo() -> String {
    Parameters::NvmeCtrlLossTmo.as_ref().to_kebab_case()
}

/// Global configuration parameters.
#[derive(Debug, Default)]
pub(crate) struct Config {
    nvme: NvmeConfig,
}
impl Config {
    /// Get the `NvmeConfig` as mut.
    pub(crate) fn nvme_as_mut(&mut self) -> &mut NvmeConfig {
        &mut self.nvme
    }
    /// Get the `NvmeConfig`.
    pub(crate) fn nvme(&self) -> &NvmeConfig {
        &self.nvme
    }
}

/// Nvme Configuration parameters.
#[derive(Debug, Default)]
pub(crate) struct NvmeConfig {
    nr_io_queues: Option<u32>,
    /// Default value for `ctrl_loss_tmo` when not specified via the volume parameters (sc).
    ctrl_loss_tmo: Option<u32>,
}
impl NvmeConfig {
    fn new(nr_io_queues: Option<u32>, ctrl_loss_tmo: Option<u32>) -> Self {
        Self {
            nr_io_queues,
            ctrl_loss_tmo,
        }
    }
    /// Number of IO Queues.
    pub(crate) fn nr_io_queues(&self) -> Option<u32> {
        self.nr_io_queues
    }
    /// The `ctrl_loss_tmo` value.
    /// Used to setup the max number of reconnects until the initiator gives up.
    pub(crate) fn ctrl_loss_tmo(&self) -> Option<u32> {
        self.ctrl_loss_tmo
    }
}

/// Get a mutex guard over the `Config`.
pub(crate) fn config<'a>() -> MutexGuard<'a, Config> {
    lazy_static::lazy_static! {
        static ref CONFIG: Arc<Mutex<Config>> = Arc::new(Mutex::new(Config::default()));
    }
    CONFIG.lock().expect("not poisoned")
}

/// Nvme-specific configuration values.
#[derive(Default)]
pub(crate) struct NvmeArgValues(HashMap<String, String>);
impl TryFrom<NvmeArgValues> for NvmeConfig {
    type Error = anyhow::Error;
    fn try_from(src: NvmeArgValues) -> Result<Self, Self::Error> {
        let nvme_nr_ioq = Parameters::nr_io_queues(src.0.get(Parameters::NvmeNrIoQueues.as_ref()))
            .map_err(|error| {
                anyhow::anyhow!(
                    "Invalid value for {}, error = {}",
                    Parameters::NvmeNrIoQueues.as_ref(),
                    error
                )
            })?;
        let ctrl_loss_tmo = Parameters::ctrl_loss_tmo(
            src.0.get(Parameters::NvmeCtrlLossTmo.as_ref()),
        )
        .map_err(|error| {
            anyhow::anyhow!(
                "Invalid value for {}, error = {}",
                Parameters::NvmeCtrlLossTmo.as_ref(),
                error
            )
        })?;
        Ok(Self::new(nvme_nr_ioq, ctrl_loss_tmo))
    }
}
/// Nvme Arguments taken from the CSI volume calls (storage class parameters).
pub(crate) type NvmeParseParams<'a> = &'a HashMap<String, String>;
impl TryFrom<NvmeParseParams<'_>> for NvmeArgValues {
    type Error = anyhow::Error;
    fn try_from(value: NvmeParseParams) -> Result<Self, Self::Error> {
        fn add_param(from: &NvmeParseParams, to: &mut NvmeArgValues, name: &str) {
            if let Some(value) = from.get(name) {
                to.0.insert(name.to_string(), value.to_string());
            }
        }
        let mut us = Self::default();
        add_param(&value, &mut us, Parameters::NvmeCtrlLossTmo.as_ref());
        Ok(us)
    }
}
impl TryFrom<NvmeParseParams<'_>> for NvmeConfig {
    type Error = anyhow::Error;
    fn try_from(value: NvmeParseParams) -> Result<Self, Self::Error> {
        Self::try_from(NvmeArgValues::try_from(value)?)
    }
}
impl TryFrom<&ArgMatches<'_>> for NvmeArgValues {
    type Error = anyhow::Error;
    fn try_from(matches: &ArgMatches<'_>) -> Result<Self, Self::Error> {
        let mut map = NvmeArgValues::default();
        if let Some(value) = matches.value_of(nvme_nr_io_queues()) {
            map.0
                .insert(Parameters::NvmeNrIoQueues.to_string(), value.to_string());
        }

        if let Some(value) = matches.value_of(nvme_ctrl_loss_tmo()) {
            map.0
                .insert(Parameters::NvmeCtrlLossTmo.to_string(), value.to_string());
        }
        Ok(map)
    }
}
impl TryFrom<&ArgMatches<'_>> for NvmeConfig {
    type Error = anyhow::Error;
    fn try_from(matches: &ArgMatches) -> Result<Self, Self::Error> {
        Self::try_from(NvmeArgValues::try_from(matches)?)
    }
}
