mod csi;
pub mod dns;
mod elastic;
mod empty;
mod etcd;
mod jaeger;
mod kibana;
mod mayastor;
pub mod nats;
mod rest;

use self::nats::bus;
use super::StartOptions;
use async_trait::async_trait;
use common_lib::{
    mbus_api::Message,
    types::v0::message_bus::{ChannelVs, Liveness},
};
use composer::{Binary, Builder, BuilderConfigure, ComposeTest, ContainerSpec};
use futures::future::join_all;
use paste::paste;
use std::{cmp::Ordering, convert::TryFrom, str::FromStr};
use structopt::StructOpt;
use strum::VariantNames;
use strum_macros::{EnumVariantNames, ToString};

/// Error type used by the deployer
pub type Error = Box<dyn std::error::Error>;

#[macro_export]
macro_rules! impl_ctrlp_agents {
    ($($name:ident,)+) => {
        /// List of Control Plane Agents to deploy
        #[derive(Debug, Clone)]
        pub struct ControlPlaneAgents(Vec<ControlPlaneAgent>);

        impl ControlPlaneAgents {
            /// Get inner vector of ControlPlaneAgent's
            pub fn into_inner(self) -> Vec<ControlPlaneAgent> {
                self.0
            }
        }

        /// All the Control Plane Agents
        #[derive(Debug, Clone, StructOpt, ToString, EnumVariantNames)]
        #[structopt(about = "Control Plane Agents")]
        pub enum ControlPlaneAgent {
            Empty(Empty),
            $(
                $name($name),
            )+
        }

        impl TryFrom<Vec<&str>> for ControlPlaneAgents {
            type Error = String;

            fn try_from(src: Vec<&str>) -> Result<Self, Self::Error> {
                let mut vec = vec![];
                for src in src {
                    vec.push(ControlPlaneAgent::from_str(src)?);
                }
                Ok(ControlPlaneAgents(vec))
            }
        }

        impl From<&ControlPlaneAgent> for Component {
            fn from(ctrlp_svc: &ControlPlaneAgent) -> Self {
                match ctrlp_svc {
                    ControlPlaneAgent::Empty(obj) => Component::Empty(obj.clone()),
                    $(ControlPlaneAgent::$name(obj) => Component::$name(obj.clone()),)+
                }
            }
        }

        paste! {
            impl FromStr for ControlPlaneAgent {
                type Err = String;

                fn from_str(source: &str) -> Result<Self, Self::Err> {
                    Ok(match source.trim().to_ascii_lowercase().as_str() {
                        "" => Self::Empty(Empty::default()),
                        "empty" => Self::Empty(Empty::default()),
                        $(stringify!([<$name:lower>]) => Self::$name($name::default()),)+
                        _ => return Err(format!(
                            "\"{}\" is an invalid type of agent! Available types: {:?}",
                            source,
                            Self::VARIANTS
                        )),
                    })
                }
            }
        }

        $(#[async_trait]
        impl ComponentAction for $name {
            fn configure(&self, options: &StartOptions, cfg: Builder) -> Result<Builder, Error> {
                let name = stringify!($name).to_ascii_lowercase();
                if options.build {
                    let status = std::process::Command::new("cargo")
                        .args(&["build", "-p", "agents", "--bin", &name])
                        .status()?;
                    build_error(&format!("the {} agent", name), status.code())?;
                }
                let mut binary = Binary::from_dbg(&name).with_nats("-n");
                if let Some(env) = &options.agents_env {
                    for kv in env {
                        binary = binary.with_env(kv.key.as_str(), kv.value.as_str().as_ref());
                    }
                }
                if name == "core" {
                    let etcd = format!("etcd.{}:2379", options.cluster_label.name());
                    binary = binary.with_args(vec!["--store", &etcd]);
                    if let Some(cache_period) = &options.cache_period {
                        binary = binary.with_args(vec!["-c", &cache_period.to_string()]);
                    }
                    if let Some(deadline) = &options.node_deadline {
                        binary = binary.with_args(vec!["-d", &deadline.to_string()]);
                    }
                    if let Some(timeout) = &options.node_conn_timeout {
                        binary = binary.with_args(vec!["--connect-timeout", &timeout.to_string()]);
                    }
                    if let Some(timeout) = &options.request_timeout {
                        binary = binary.with_args(vec!["--request-timeout", &timeout.to_string()]);
                    }
                    if options.no_min_timeouts {
                        binary = binary.with_arg("--no-min-timeouts");
                    }
                    if let Some(timeout) = &options.store_timeout {
                        binary = binary.with_args(vec!["--store-timeout", &timeout.to_string()]);
                    }
                    if let Some(ttl) = &options.store_lease_ttl {
                        binary = binary.with_args(vec!["--store-lease-ttl", &ttl.to_string()]);
                    }
                    if let Some(period) = &options.reconcile_period {
                        binary = binary.with_args(vec!["--reconcile-period", &period.to_string()]);
                    }
                    if let Some(period) = &options.reconcile_idle_period {
                        binary = binary.with_args(vec!["--reconcile-idle-period", &period.to_string()]);
                    }
                    if cfg.container_exists("jaeger") {
                        let jaeger_config = format!("jaeger.{}:6831", cfg.get_name());
                        binary = binary.with_args(vec!["--jaeger", &jaeger_config]);
                    }
                }
                Ok(cfg.add_container_bin(&name, binary))
            }
            async fn start(&self, _options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
                let name = stringify!($name).to_ascii_lowercase();
                cfg.start(&name).await?;
                Ok(())
            }
            async fn wait_on(&self, _options: &StartOptions, _cfg: &ComposeTest) -> Result<(), Error> {
                Liveness {}.request_on_bus(ChannelVs::$name, bus()).await?;
                Ok(())
            }
        })+
    };
    ($($name:ident), +) => {
        impl_ctrlp_agents!($($name,)+);
    };
}

pub fn build_error(name: &str, status: Option<i32>) -> Result<(), Error> {
    let make_error = |extra: &str| {
        let error = format!("Failed to build {}: {}", name, extra);
        std::io::Error::new(std::io::ErrorKind::Other, error)
    };
    match status {
        Some(0) => Ok(()),
        Some(code) => {
            let error = format!("exited with code {}", code);
            Err(make_error(&error).into())
        }
        None => Err(make_error("interrupted by signal").into()),
    }
}

impl Components {
    /// Wait for the url endpoint to return success to a GET request with a default timeout
    pub async fn wait_url(url: &str) -> Result<(), Error> {
        Self::wait_url_timeout(url, std::time::Duration::from_secs(20)).await
    }
    /// Wait for the url endpoint to return success to a GET request with a provided timeout
    pub async fn wait_url_timeout(url: &str, timeout: std::time::Duration) -> Result<(), Error> {
        let start_time = std::time::Instant::now();
        let get_timeout = std::time::Duration::from_millis(200);
        loop {
            let request = reqwest::Client::new().get(url).timeout(get_timeout);
            match request.send().await {
                Ok(_) => {
                    return Ok(());
                }
                Err(error) => {
                    if std::time::Instant::now() > (start_time + timeout) {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::AddrNotAvailable,
                            format!("URL '{}' not ready yet: '{:?}'", url, error),
                        )
                        .into());
                    }
                }
            }
            tokio::time::sleep(get_timeout).await;
        }
    }
    /// Start all components and wait up to the provided timeout
    pub async fn start_wait(
        &self,
        cfg: &ComposeTest,
        timeout: std::time::Duration,
    ) -> Result<(), Error> {
        match tokio::time::timeout(timeout, self.start_wait_inner(cfg)).await {
            Ok(result) => result,
            Err(_) => {
                let error = format!("Time out of {:?} expired", timeout);
                Err(std::io::Error::new(std::io::ErrorKind::TimedOut, error).into())
            }
        }
    }
    /// Start all components, in order, but don't wait for them
    pub async fn start(&self, cfg: &ComposeTest) -> Result<(), Error> {
        let mut last_done = None;
        for component in &self.0 {
            if let Some(last_done) = last_done {
                if component.boot_order() == last_done {
                    continue;
                }
            }
            let components = self
                .0
                .iter()
                .filter(|c| c.boot_order() == component.boot_order());
            for component in components {
                component.start(&self.1, cfg).await?;
            }
            last_done = Some(component.boot_order());
        }
        Ok(())
    }
    /// Start all components, in order. Then wait for all components with a wait between each
    /// component to make sure they start orderly
    async fn start_wait_inner(&self, cfg: &ComposeTest) -> Result<(), Error> {
        let mut last_done = None;
        for component in &self.0 {
            if let Some(last_done) = last_done {
                if component.boot_order() == last_done {
                    continue;
                }
            }
            let components = self
                .0
                .iter()
                .filter(|c| c.boot_order() == component.boot_order())
                .collect::<Vec<&Component>>();

            for component in &components {
                component.start(&self.1, cfg).await?;
            }
            self.wait_on_components(&components, cfg).await?;
            last_done = Some(component.boot_order());
        }
        Ok(())
    }
    pub fn shutdown_order(&self) -> Vec<String> {
        let ordered = self
            .0
            .iter()
            .rev()
            // todo: this is wrong, get the actual name!
            .map(|c| c.to_string().to_ascii_lowercase())
            .collect::<Vec<_>>();
        ordered
    }
    pub fn nats_enabled(&self) -> bool {
        !self.1.no_nats
    }
    /// to check whether core agent is being deployed or not
    pub fn core_enabled(&self) -> bool {
        self.0.contains(&Component::Core(Default::default()))
    }
}

#[macro_export]
macro_rules! impl_component {
    ($($name:ident,$order:literal,)+) => {
        /// All the Control Plane Components
        #[derive(Debug, Clone, StructOpt, ToString, EnumVariantNames, Eq, PartialEq)]
        #[structopt(about = "Control Plane Components")]
        pub enum Component {
            $(
                $name($name),
            )+
        }

        /// List of Control Plane Components to deploy
        #[derive(Debug, Clone)]
        pub struct Components(Vec<Component>, StartOptions);
        impl BuilderConfigure for Components {
            fn configure(&self, cfg: Builder) -> Result<Builder, Error> {
                if self.1.build_all {
                    let path = std::path::PathBuf::from(std::env!("CARGO_MANIFEST_DIR"));
                    let status = std::process::Command::new("cargo")
                        .current_dir(path.parent().expect("main workspace"))
                        .args(&["build", "--bins"])
                        .status()?;
                    build_error("all the workspace binaries", status.code())?;
                }
                let mut cfg = cfg;
                for component in &self.0 {
                    cfg = component.configure(&self.1, cfg)?;
                }
                Ok(cfg)
            }
        }

        impl Components {
            pub fn push_generic_components(&mut self, name: &str, component: Component) {
                if !ControlPlaneAgent::VARIANTS.iter().any(|&s| s == name) {
                    self.0.push(component);
                }
            }
            pub fn new(options: StartOptions) -> Components {
                let agents = options.agents.clone();
                let components = agents
                    .iter()
                    .map(Component::from)
                    .collect::<Vec<Component>>();

                let mut components = Components(components, options.clone());
                $(components.push_generic_components(stringify!($name), $name::default().into());)+
                components.0.sort();
                components
            }
            pub async fn wait_on(
                &self,
                cfg: &ComposeTest,
                timeout: std::time::Duration,
            ) -> Result<(), Error> {
                match tokio::time::timeout(timeout, self.wait_on_inner(cfg)).await {
                    Ok(result) => result,
                    Err(_) => {
                        let error = format!("Time out of {:?} expired", timeout);
                        Err(std::io::Error::new(std::io::ErrorKind::TimedOut, error).into())
                    }
                }
            }
            async fn wait_on_components(&self, components: &[&Component], cfg: &ComposeTest) -> Result<(), Error> {
                let mut futures = vec![];
                for component in components {
                    futures.push(async move {
                        component.wait_on(&self.1, cfg).await
                    })
                }
                let result = join_all(futures).await;
                result.iter().for_each(|result| match result {
                    Err(error) => println!("Failed to wait for component: {:?}", error),
                    _ => {}
                });
                if let Some(Err(error)) = result.iter().find(|result| result.is_err()) {
                    Err(std::io::Error::new(std::io::ErrorKind::AddrNotAvailable, error.to_string()).into())
                } else {
                    Ok(())
                }
            }
            async fn wait_on_inner(&self, cfg: &ComposeTest) -> Result<(), Error> {
                self.wait_on_components(&self.0.iter().collect::<Vec<_>>(), cfg).await
            }
        }

        /// Trait to manage a component startup sequence
        #[async_trait]
        pub trait ComponentAction {
            fn configure(&self, options: &StartOptions, cfg: Builder) -> Result<Builder, Error>;
            async fn start(&self, options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error>;
            async fn wait_on(&self, _options: &StartOptions, _cfg: &ComposeTest) -> Result<(), Error> {
                Ok(())
            }
        }

        #[async_trait]
        impl ComponentAction for Component {
            fn configure(&self, options: &StartOptions, cfg: Builder) -> Result<Builder, Error> {
                match self {
                    $(Self::$name(obj) => obj.configure(options, cfg),)+
                }
            }
            async fn start(&self, options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
                match self {
                    $(Self::$name(obj) => obj.start(options, cfg).await,)+
                }
            }
            async fn wait_on(&self, options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
                match self {
                    $(Self::$name(obj) => obj.wait_on(options, cfg).await,)+
                }
            }
        }

        $(impl From<$name> for Component {
            fn from(from: $name) -> Component {
                Component::$name(from)
            }
        })+

        /// Control Plane Component
        $(#[derive(Default, Debug, Clone, StructOpt, Eq, PartialEq)]
        pub struct $name {})+

        impl Component {
            fn boot_order(&self) -> u32 {
                match self {
                    $(Self::$name(_) => $order,)+
                }
            }
        }

        impl PartialOrd for Component {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                self.boot_order().partial_cmp(&other.boot_order())
            }
        }
        impl Ord for Component {
            fn cmp(&self, other: &Self) -> Ordering {
                self.boot_order().cmp(&other.boot_order())
            }
        }
    };
    ($($name:ident, $order:ident), +) => {
        impl_component!($($name,$order)+);
    };
}

// Component Name and bootstrap ordering
// from lower to high
impl_component! {
    Empty,      0,
    // Note: NATS needs to be the first to support usage of this library in cargo tests
    // to make sure that the IP does not change between tests
    Nats,       0,
    Dns,        1,
    Etcd,       1,
    Elastic,    1,
    Kibana,     1,
    Jaeger,     2,
    Core,       3,
    Rest,       3,
    Mayastor,   4,
    JsonGrpc,   5,
    Csi,        5,
}

// Message Bus Control Plane Agents
impl_ctrlp_agents!(Core, JsonGrpc);
