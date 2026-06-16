#![deny(missing_docs)]
//! This library provides a low-level k8s proxy support.
//!
//! The different proxies can be used to communicate with in-cluster pods/services using the
//! kubernetes api-server.
//!
//! If you're looking at a higher-level construct, please take a look at kube-proxy.

mod error;
mod http_forward;
mod pod_selection;
mod port_forward;

/// Layer 7 proxies.
pub use http_forward::{HttpForward, HttpProxy};
use openapi::tower::client::configuration::TlsMode;
/// Layer 4 proxies.
pub use port_forward::PortForward;

/// The kubernetes api version used throughout the crate.
pub(crate) use k8s_openapi::api::core::v1 as vx;

use anyhow::Context;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::ResourceExt;
use vx::Pod;

/// Default pod annotation prefix for TLS/JWT material.
pub const DEFAULT_REST_TLS_PREFIX: &str = "openebs.io/rest";

/// The purpose of a Kubernetes Secret, which determines the expected key names.
#[derive(Debug, Clone)]
pub(crate) enum SecretPurpose {
    Tls(crate::TlsModeAnno),
    Jwt(crate::AuthModeAnno),
}

/// TLS mode as specified by pod annotation.
#[derive(Default, Clone, Copy, Debug)]
pub(crate) enum TlsModeAnno {
    #[default]
    None,
    Auto,
    ClientVerify,
    ServerVerify,
    Mtls,
}
impl From<&str> for TlsModeAnno {
    fn from(s: &str) -> Self {
        match s {
            "none" => Self::None,
            "auto" => Self::Auto,
            "client-verify" => Self::ClientVerify,
            "server-verify" => Self::ServerVerify,
            "mtls" => Self::Mtls,
            _ => Self::None,
        }
    }
}

/// Authentication mode as specified by pod annotation.
#[derive(Default, Clone, Copy, Debug)]
pub(crate) enum AuthModeAnno {
    #[default]
    None,
    Jwt,
}
impl From<&str> for AuthModeAnno {
    fn from(s: &str) -> Self {
        match s {
            "none" => Self::None,
            "jwt" => Self::Jwt,
            _ => Self::None,
        }
    }
}

/// A reference to a Kubernetes Secret with fixed key names.
#[derive(Clone, Debug)]
pub(crate) struct SecretRef {
    /// Name of the Kubernetes Secret.
    pub(crate) name: String,
    /// Purpose of the Kubernetes Secret (ie tls or auth).
    pub(crate) kind: SecretPurpose,
}
impl SecretRef {
    /// Create a new `SecretRef` with the given secret name.
    pub(crate) fn new_tls(name: impl Into<String>, mode: crate::TlsModeAnno) -> Self {
        Self {
            name: name.into(),
            kind: SecretPurpose::Tls(mode),
        }
    }
    /// Create a new `SecretRef` with the given secret name.
    pub(crate) fn new_jwt(name: impl Into<String>, mode: crate::AuthModeAnno) -> Self {
        Self {
            name: name.into(),
            kind: SecretPurpose::Jwt(mode),
        }
    }
    fn ca_key(&self) -> &str {
        "ca.crt"
    }
    fn cert_key(&self) -> &str {
        "tls.crt"
    }
    fn key_key(&self) -> &str {
        "tls.key"
    }
    fn jwt_key(&self) -> &str {
        "jwt"
    }
}

/// Data fetched from a Kubernetes Secret.
#[derive(Clone, Default, Debug)]
pub(crate) struct SecretData {
    /// CA certificate PEM bytes, if found.
    pub(crate) ca_certificate: Option<Vec<u8>>,
    /// Client certificate PEM bytes, if found.
    pub(crate) client_certificate: Option<Vec<u8>>,
    /// Client private key PEM bytes, if found.
    pub(crate) client_key: Option<Vec<u8>>,
    /// JWT bearer token string, if found.
    pub(crate) jwt: Option<String>,
}

impl From<SecretData> for TlsMode {
    fn from(data: SecretData) -> Self {
        match (
            data.ca_certificate,
            data.client_certificate,
            data.client_key,
        ) {
            (Some(ca_certificate), None, None) => TlsMode::ServerVerify { ca_certificate },
            (None, Some(client_certificate), Some(client_key)) => TlsMode::ClientVerify {
                client_certificate,
                client_key,
            },
            (Some(ca_certificate), Some(client_certificate), Some(client_key)) => TlsMode::Mtls {
                ca_certificate,
                client_certificate,
                client_key,
            },
            _ => TlsMode::Auto,
        }
    }
}

/// The error exposed.
pub use crate::error::Error;

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
#[derive(Debug, Clone)]
pub struct NameSpace(Option<String>);
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
    fn new(pod_name: String, port_number: i32) -> Result<Self, Error> {
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
    ///
    /// TODO: this namespace api is not bad, needs refactoring...
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
    pub(crate) fn find(&self, pod: &Pod, port: Option<Port>) -> Result<TargetPod, Error> {
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
