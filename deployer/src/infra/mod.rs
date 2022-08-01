mod core;
mod csi_controller;
mod csi_node;
pub mod dns;
mod elastic;
mod empty;
mod etcd;
mod hanodeagent;
mod io_engine;
mod jaeger;
mod jsongrpc;
mod kibana;
mod rest;

use super::StartOptions;
use async_trait::async_trait;

use composer::{Binary, Builder, BuilderConfigure, ComposeTest, ContainerSpec};
use futures::future::{join_all, try_join_all};
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
    /// Wait for the url endpoint to return success to a GET request with a default timeout.
    pub async fn wait_url(url: &str) -> Result<(), Error> {
        Self::wait_url_timeout(url, std::time::Duration::from_secs(20)).await
    }
    /// Wait for the url endpoint to return success to a GET request with a provided timeout.
    pub async fn wait_url_timeout(url: &str, timeout: std::time::Duration) -> Result<(), Error> {
        Self::wait_url_timeouts(url, timeout, std::time::Duration::from_millis(200)).await
    }
    /// Wait for the url endpoint to return success to a GET request with provided timeouts.
    pub async fn wait_url_timeouts(
        url: &str,
        total: std::time::Duration,
        each: std::time::Duration,
    ) -> Result<(), Error> {
        let start_time = std::time::Instant::now();
        let get_timeout = each;
        let timeout = total;
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

            let start_components = components.map(|component| async move {
                tracing::trace!(component=%component.to_string(), "Starting");
                component.start(&self.1, cfg).await
            });
            try_join_all(start_components).await?;
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

            let wait_components = components
                .iter()
                .map(|c| async move { c.start(&self.1, cfg).await });
            try_join_all(wait_components).await?;
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
                Ok(cfg.with_spec_map(|spec| {
                    let spec = spec.with_direct_bind("/etc/machine-id")
                                   .with_direct_bind("/sys/class/dmi/id/product_uuid");
                    if let Some(uid) = &self.1.cluster_uid {
                        spec.with_env("NOPLATFORM_UUID", uid)
                    } else {
                        spec
                    }
                }))
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
                let _trace = TraceFn::new(self, "configure");
                match self {
                    $(Self::$name(obj) => obj.configure(options, cfg),)+
                }
            }
            async fn start(&self, options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
                let _trace = TraceFn::new(self, "start");
                match self {
                    $(Self::$name(obj) => obj.start(options, cfg).await,)+
                }
            }
            async fn wait_on(&self, options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
                let _trace = TraceFn::new(self, "wait_on");
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

struct TraceFn(String, String);
impl TraceFn {
    fn new(component: &Component, func: &str) -> Self {
        let component = component.to_string();
        tracing::trace!(component=%component, func=%func, "Entering");
        Self(component, func.to_string())
    }
}
impl Drop for TraceFn {
    fn drop(&mut self) {
        tracing::trace!(component=%self.0, func=%self.1, "Exiting");
    }
}

// Component Name and bootstrap ordering
// from lower to high
impl_component! {
    Empty,         0,
    Jaeger,        0,
    Dns,           1,
    Etcd,          1,
    Elastic,       1,
    Kibana,        1,
    Core,          3,
    JsonGrpc,      3,
    Rest,          3,
    IoEngine,      4,
    CsiNode,       5,
    CsiController, 5,
    HANodeAgent,   5,
}

// Message Bus Control Plane Agents
impl_ctrlp_agents!(Core, JsonGrpc, HANodeAgent);
