#![deny(missing_docs)]
//! This library provides a low-level k8s proxy support.
//!
//! The different proxies can be used to communicate with in-cluster pods/services using the
//! kubernetes api-server.
//!
//! If you're looking at a higher-level construct, please take a look at kube-proxy.

mod http_forward;
mod pod_selection;
mod port_forward;

/// Layer 7 proxies.
pub use http_forward::{HttpForward, HttpProxy};
/// Layer 4 proxies.
pub use port_forward::PortForward;

/// The kubernetes api version used throughout the crate.
pub(crate) use k8s_openapi::api::core::v1 as vx;

use anyhow::Context;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::ResourceExt;
use vx::Pod;

/// Different types of target selectors.
#[derive(Clone)]
pub enum TargetSelector {
    /// By pod name.
    PodName(String),
    /// By pod label selector.
    PodLabel(String),
    /// By service label selector.
    ServiceLabel(String),
}
impl TargetSelector {
    /// New `Self` from the given pod label key value.
    pub fn pod_label(key: &str, val: &str) -> Self {
        Self::PodLabel(format!("{key}={val}"))
    }
    /// New `Self` from the given service label key value.
    pub fn svc_label(key: &str, val: &str) -> Self {
        Self::ServiceLabel(format!("{key}={val}"))
    }
}

/// Identify a port explicitly by its number of by name.
#[derive(Clone)]
pub enum Port {
    /// Specified using a number.
    Number(i32),
    /// Specified using a name.
    Name(String),
}
impl From<i32> for Port {
    fn from(port: i32) -> Self {
        Self::Number(port)
    }
}
impl From<&str> for Port {
    fn from(port: &str) -> Self {
        Self::Name(port.to_string())
    }
}
impl From<IntOrString> for Port {
    fn from(port: IntOrString) -> Self {
        match port {
            IntOrString::Int(port) => Self::Number(port),
            IntOrString::String(port) => Self::Name(port),
        }
    }
}
impl Port {
    /// Returns the port name, if set.
    pub(crate) fn name(&self) -> Option<&String> {
        match self {
            Port::Number(_) => None,
            Port::Name(name) => Some(name),
        }
    }
    /// Returns the port number, if set.
    pub(crate) fn number(&self) -> Option<i32> {
        match self {
            Port::Number(number) => Some(*number),
            Port::Name(_) => None,
        }
    }
    /// Returns the port as a string.
    pub(crate) fn any(&self) -> String {
        match self {
            Port::Number(number) => number.to_string(),
            Port::Name(name) => name.clone(),
        }
    }
}

/// A kubernetes target.
#[derive(Clone)]
pub struct Target {
    selector: TargetSelector,
    port: Port,
    namespace: NameSpace,
}

/// A kubernetes namespace.
/// If None, the default is "default".
#[derive(Clone)]
pub(crate) struct NameSpace(Option<String>);
impl NameSpace {
    /// Returns the configured namespace or the default.
    pub(crate) fn name_any(&self) -> String {
        let default = "default".to_string();
        self.0.clone().unwrap_or(default)
    }
}

/// A pod target which is composed of its pod name and port number.
#[derive(Clone)]
pub(crate) struct TargetPod {
    pod_name: String,
    port_number: u16,
}
impl TargetPod {
    fn new(pod_name: String, port_number: i32) -> anyhow::Result<Self> {
        let port_number = u16::try_from(port_number).context("Port not valid")?;
        Ok(Self {
            pod_name,
            port_number,
        })
    }
    /// Convert `Self` into a tuple of `pod_name` and `port_number`.
    pub(crate) fn into_parts(self) -> (String, u16) {
        (self.pod_name, self.port_number)
    }
}

impl Target {
    /// Returns a new `Self` from the given parameters.
    /// # Arguments
    /// * `selector` - target selector
    /// * `port` - target port
    /// * `namespace` - target namespace
    pub fn new<I: Into<Option<T>>, T: Into<String>, P: Into<Port>>(
        selector: TargetSelector,
        port: P,
        namespace: I,
    ) -> Self {
        Self {
            selector,
            port: port.into(),
            namespace: NameSpace(namespace.into().map(Into::into)),
        }
    }

    /// Modify and return `Self` from the given parameters.
    /// # Arguments
    /// * `selector` - target selector
    pub fn with_selector(mut self, selector: TargetSelector) -> Self {
        self.selector = selector;
        self
    }

    /// Modify and return `Self` from the given parameters.
    /// # Arguments
    /// * `port` - target port
    pub fn with_port<P: Into<Port>>(mut self, port: P) -> Self {
        self.port = port.into();
        self
    }

    /// Modify and return `Self` from the given parameters.
    /// # Arguments
    /// * `namespace` - target namespace
    pub fn with_namespace<I: Into<Option<T>>, T: Into<String>>(mut self, namespace: I) -> Self {
        self.namespace = NameSpace(namespace.into().map(Into::into));
        self
    }

    /// Returns the `TargetPod` for the given pod/port or pod/self.port.
    pub(crate) fn find(&self, pod: &Pod, port: Option<Port>) -> anyhow::Result<TargetPod> {
        let port = match &port {
            None => &self.port,
            Some(port) => port,
        };

        TargetPod::new(
            pod.name_any(),
            match port {
                Port::Number(port) => *port,
                Port::Name(name) => {
                    let spec = pod.spec.as_ref().context("Pod Spec is None")?;
                    let containers = &spec.containers;
                    let mut ports = containers.iter().filter_map(|c| c.ports.as_ref()).flatten();
                    let port = ports.find(|p| p.name.as_ref() == Some(name));
                    port.context("Port not found")?.container_port
                }
            },
        )
    }
}
