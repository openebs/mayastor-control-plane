use anyhow::Context;
use clap::ArgMatches;
use once_cell::sync::OnceCell;
use std::{collections::HashMap, time::Duration};

static CONFIG: OnceCell<CsiControllerConfig> = OnceCell::new();

// Global CSI Controller config.
pub(crate) struct CsiControllerConfig {
    /// REST API endpoint URL.
    rest_endpoint: String,
    /// I/O timeout for REST API operations.
    io_timeout: Duration,
    /// IO-Engine DaemonSet selector labels.
    io_engine_selector: HashMap<String, String>,
}

impl CsiControllerConfig {
    /// Initialize global instance of the CSI config. Must be called prior to using the config.
    pub(crate) fn initialize(args: &ArgMatches) -> anyhow::Result<()> {
        assert!(
            CONFIG.get().is_none(),
            "CSI Controller config already initialized"
        );

        let rest_endpoint = args
            .value_of("endpoint")
            .context("rest endpoint must be specified")?;

        let io_timeout = args
            .value_of("timeout")
            .context("I/O timeout must be specified")?
            .parse::<humantime::Duration>()?;

        let io_engine_selector = {
            let values = match args.values_of("io-engine-selector") {
                Some(values) => values.map(ToString::to_string).collect::<Vec<_>>(),
                None => vec![Self::default_io_selector()],
            };
            let values = values.iter().map(|source| match source.split_once(':') {
                None => Err(anyhow::anyhow!(
                    "Each io-engine-selector label must be in the format: 'Key=Value'"
                )),
                Some((key, value)) => Ok((key.to_string(), value.to_string())),
            });
            if let Some(error) = values.clone().find_map(|f| match f {
                Ok(_) => None,
                Err(error) => Some(error),
            }) {
                return Err(error);
            }
            values
                .filter_map(|source| source.ok())
                .collect::<HashMap<_, _>>()
        };

        CONFIG.get_or_init(|| Self {
            rest_endpoint: rest_endpoint.into(),
            io_timeout: io_timeout.into(),
            io_engine_selector,
        });
        Ok(())
    }

    /// Default IO-Engine DaemonSet selector label.
    pub(crate) fn default_io_selector() -> String {
        format!(
            "{}:{}",
            utils::IO_ENGINE_SELECTOR_KEY,
            utils::IO_ENGINE_SELECTOR_VALUE
        )
    }

    /// Get global instance of CSI controller config.
    pub(crate) fn get_config() -> &'static CsiControllerConfig {
        CONFIG
            .get()
            .expect("CSI Controller config is not initialized")
    }

    /// Get REST API endpoint.
    pub(crate) fn rest_endpoint(&self) -> &str {
        &self.rest_endpoint
    }

    /// Get I/O timeout for REST API operations.
    pub(crate) fn io_timeout(&self) -> Duration {
        self.io_timeout
    }

    /// IO-Engine DaemonSet selector labels.
    pub(crate) fn io_engine_selector(&self) -> HashMap<String, String> {
        self.io_engine_selector.clone()
    }
}
