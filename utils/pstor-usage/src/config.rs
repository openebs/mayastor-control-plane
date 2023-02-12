use crate::{printer::TabledData, simulation::SimulationOpts};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    convert::TryFrom,
    ffi::{OsStr, OsString},
};

use clap::Parser;

#[derive(Parser, Debug, Clone)]
pub(crate) struct ClusterOpts {
    /// Reads cluster configuration from a YAML configuration file.
    /// Example format:
    /// ```yaml
    /// clusters:
    ///   tiny:
    ///     replicas: 1
    ///     volume_turnover: 1
    ///     volume_attach_cycles: 5
    ///   small:
    ///     replicas: 2
    ///     volume_turnover: 10
    ///     volume_attach_cycles: 15
    /// ```
    /// When using this option you must specify which cluster to extrapolate using --cluster-name.
    #[clap(long, short, verbatim_doc_comment, value_parser = clap::builder::OsStringValueParser::new())]
    config: Option<Config>,

    /// When using a cluster config (--config), you can specify a single cluster to extrapolate.
    /// Otherwise, we'll extrapolate all clusters.
    #[clap(long, requires = "config")]
    cluster_name: Option<ClusterName>,
}
impl ClusterOpts {
    /// Get the cluster config.
    pub(crate) fn simulation(&self) -> Option<SimulationOpts> {
        self.config.as_ref().and_then(|c| c.simulation())
    }
    /// Get the all the specified clusters.
    pub(crate) fn clusters(&self) -> Option<HashMap<ClusterName, ClusterConfig>> {
        self.config.as_ref().map(|c| c.clusters.clone())
    }

    /// Get the specified cluster.
    pub(crate) fn cluster(&self) -> anyhow::Result<Option<(ClusterName, ClusterConfig)>> {
        match (&self.config, &self.cluster_name) {
            (Some(config), Some(name)) => {
                let cluster = config.cluster(name).cloned();
                anyhow::ensure!(
                    cluster.is_some(),
                    "Cluster ({}) not found in the config file",
                    name
                );
                Ok(cluster.map(|cluster| (name.clone(), cluster)))
            }
            (Some(_), None) => Ok(None),
            (None, None) => Ok(None),
            (None, Some(_)) => unreachable!(),
        }
    }
}

/// Cluster Identifier
pub(crate) type ClusterName = String;

/// The various cluster configurations
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub(crate) struct Config {
    clusters: HashMap<ClusterName, ClusterConfig>,
    simulation: Option<SimulationOpts>,
}

impl TryFrom<&OsStr> for Config {
    type Error = OsString;

    fn try_from(value: &OsStr) -> Result<Self, Self::Error> {
        Config::parse_config(value)
    }
}

impl Config {
    /// Parse config from the given path location.
    pub(crate) fn parse_config(path: &OsStr) -> Result<Config, OsString> {
        let path = std::path::PathBuf::from(path);
        let content =
            std::fs::read(path).map_err(|e| format!("Failed to read config file: {e}"))?;
        Ok(serde_yaml::from_slice(&content)
            .map_err(|e| format!("Invalid config file format: {e}"))?)
    }
    /// Get the cluster with the provided name.
    pub(crate) fn cluster(&self, name: &ClusterName) -> Option<&ClusterConfig> {
        self.clusters.get(name)
    }
    /// Get the simulation options
    pub(crate) fn simulation(&self) -> Option<SimulationOpts> {
        self.simulation.clone()
    }
}

/// Cluster configuration
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub(crate) struct ClusterConfig {
    replicas: u8,
    volume_turnover: u64,
    volume_attach_cycles: u64,
}

impl ClusterConfig {
    /// Get the number of replicas per volume.
    pub(crate) fn replicas(&self) -> u8 {
        self.replicas
    }
    /// Get the volume turnover (create+delete) per day.
    pub(crate) fn turnover(&self) -> u64 {
        self.volume_turnover
    }
    /// Get the number of volume modifications per day.
    pub(crate) fn mods(&self) -> u64 {
        self.volume_attach_cycles
    }
    /// Return new `Self` with the given parameters.
    pub(crate) fn new(replicas: u8, volume_turnover: u64, volume_attach_cycles: u64) -> Self {
        Self {
            replicas,
            volume_turnover,
            volume_attach_cycles,
        }
    }
}

impl TabledData for ClusterConfig {
    type Row = prettytable::Row;

    fn titles(&self) -> Self::Row {
        prettytable::Row::new(vec![
            crate::new_cell("Replicas"),
            crate::new_cell("Volume Turnover"),
            crate::new_cell("Volume Attaches/Detaches"),
        ])
    }

    fn rows(&self) -> Vec<Self::Row> {
        vec![prettytable::Row::new(vec![
            crate::new_cell(&self.replicas.to_string()),
            crate::new_cell(&self.turnover().to_string()),
            crate::new_cell(&self.mods().to_string()),
        ])]
    }
}
