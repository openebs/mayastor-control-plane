use anyhow::Context;
use futures::{StreamExt, TryStreamExt};
use openapi::tower::client::configuration::{ClientSecurity, TlsMode};
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

use crate::{
    pod_selection::{AnyReady, PodSelection},
    vx::{Pod, Secret, Service},
    Error,
};
use kube::{
    api::{Api, ListParams},
    Client, ResourceExt,
};

/// Used to "proxy" connections to a pod by use of port forwarding.
/// # Example
/// ```ignore
/// let selector = kube_forward::TargetSelector::pod_label("app", "etcd");
/// let target = kube_forward::Target::new(selector, "client", "mayastor");
/// let client = kube::Client::try_default().await?;
/// let pf = kube_forward::PortForward::new(target, 35003, client).await?;
///
/// let (_port, _client_security, handle) = pf.port_forward(true).await?;
/// handle.await?;
/// ```
#[derive(Clone)]
pub struct PortForward {
    target: crate::Target,
    local_port: Option<u16>,
    security_prefix: String,
    pod_api: Api<Pod>,
    svc_api: Api<Service>,
    secret_api: Api<Secret>,
}

impl PortForward {
    /// Return a new `Self`.
    /// # Arguments
    /// * `target` - the target we'll forward to
    /// * `local_port` - specific local port to use, if Some
    pub fn new(target: crate::Target, local_port: impl Into<Option<u16>>, client: Client) -> Self {
        Self::new_with_secrets(target, local_port, client, None)
    }

    /// Return a new `Self` with optional REST TLS/JWT secret names.
    /// Defaults are `rest-tls` and `rest-jwt` when names are not provided.
    pub fn new_with_secrets(
        target: crate::Target,
        local_port: impl Into<Option<u16>>,
        client: Client,
        security_prefix: impl Into<Option<String>>,
    ) -> Self {
        let namespace = target.namespace.name_any();
        let security_prefix = security_prefix
            .into()
            .filter(|name| !name.trim().is_empty())
            .unwrap_or_else(|| crate::DEFAULT_REST_TLS_PREFIX.to_string());

        Self {
            target,
            local_port: local_port.into(),
            security_prefix,
            pod_api: Api::namespaced(client.clone(), &namespace),
            svc_api: Api::namespaced(client.clone(), &namespace),
            secret_api: Api::namespaced(client, &namespace),
        }
    }

    /// The specified local port, or 0.
    /// Port 0 is special as it tells the kernel to give us the next free port.
    fn local_port(&self) -> u16 {
        self.local_port.unwrap_or(0)
    }

    /// Runs the port forwarding proxy until a SIGINT signal is received.
    pub async fn discover_client_security(
        &self,
        client_security: &ClientSecurity,
    ) -> Result<ClientSecurity, Error> {
        if client_security.discover() {
            let (_, client_security) = self
                .finder()
                .find(&self.target, Some(client_security))
                .await?;
            Ok(client_security)
        } else {
            Ok(client_security.clone())
        }
    }

    /// Runs the port forwarding proxy until a SIGINT signal is received.
    pub async fn secure_port_forward(
        self,
        client_security: &ClientSecurity,
    ) -> Result<(u16, ClientSecurity, tokio::task::JoinHandle<()>), Error> {
        let client_security = self.discover_client_security(client_security).await?;

        let (port, task) = self.port_forward().await?;
        Ok((port, client_security, task))
    }

    /// Runs the port forwarding proxy until a SIGINT signal is received.
    pub async fn port_forward(self) -> Result<(u16, tokio::task::JoinHandle<()>), Error> {
        let addr = SocketAddr::from(([127, 0, 0, 1], self.local_port()));

        let bind = TcpListener::bind(addr)
            .await
            .map_err(|source| Error::Io { source })?;
        let port = bind
            .local_addr()
            .map_err(|source| Error::Io { source })?
            .port();
        tracing::trace!(port, "Bound to local port");

        let server = TcpListenerStream::new(bind)
            .take_until(shutdown::Shutdown::wait_sig())
            .try_for_each(move |client_conn| {
                let pf = self.clone();

                async {
                    let client_conn = client_conn;
                    if let Ok(peer_addr) = client_conn.peer_addr() {
                        tracing::trace!(%peer_addr, "new connection");
                    }

                    tokio::spawn(async move {
                        if let Err(error) = pf.forward_connection(client_conn).await {
                            tracing::error!(%error, "failed to forward connection");
                        }
                    });

                    // keep the server running
                    Ok(())
                }
            });

        Ok((
            port,
            tokio::spawn(async {
                if let Err(e) = server.await {
                    tracing::error!(error = &e as &dyn std::error::Error, "server error");
                }
            }),
        ))
    }
    async fn forward_connection(self, mut client_conn: tokio::net::TcpStream) -> Result<(), Error> {
        let (target, _) = self.finder().find(&self.target, None).await?;
        let (pod_name, pod_port) = target.into_parts();

        let mut forwarder = self.pod_api.portforward(&pod_name, &[pod_port]).await?;
        let mut upstream_conn = forwarder
            .take_stream(pod_port)
            .context("port not found in forwarder")?;

        let local_port = self.local_port();

        tracing::debug!(local_port, pod_port, pod_name, "forwarding connections");

        if let Err(error) =
            tokio::io::copy_bidirectional(&mut client_conn, &mut upstream_conn).await
        {
            tracing::trace!(local_port, pod_port, pod_name, ?error, "connection error");
        }

        drop(upstream_conn);
        forwarder.join().await.map_err(|error| Error::AnyHow {
            source: error.into(),
        })?;
        tracing::debug!(local_port, pod_port, pod_name, "connection closed");
        Ok(())
    }
    fn finder(&self) -> TargetPodFinder<'_> {
        TargetPodFinder {
            pod_api: &self.pod_api,
            svc_api: &self.svc_api,
            secret_api: &self.secret_api,
            anno_prefix: &self.security_prefix,
        }
    }
}

/// Finds a `crate::TargetPod`, which is essentially a pod name and port.
/// Note this finds the actual pod mapping and not the service.
#[derive(Clone)]
struct TargetPodFinder<'a> {
    pod_api: &'a Api<Pod>,
    svc_api: &'a Api<Service>,
    secret_api: &'a Api<Secret>,
    anno_prefix: &'a str,
}
impl<'a> TargetPodFinder<'a> {
    /// Finds the name and port of the target pod specified by the selector,
    /// and optionally fetches `ClientSecurity` from the resolved pod/service metadata.
    pub(crate) async fn find(
        &self,
        target: &crate::Target,
        client_security: Option<&ClientSecurity>,
    ) -> Result<(crate::TargetPod, ClientSecurity), Error> {
        let pod_api = self.pod_api;
        let svc_api = self.svc_api;
        let ready_pod = AnyReady {};

        let security = ClientSecurity::default();
        let security = client_security.unwrap_or(&security);
        let fetch_sec = async |pod: &Pod| -> Result<ClientSecurity, Error> {
            if client_security.is_none() || !security.discover() {
                return Ok(security.clone());
            }
            self.fetch_client_security_from_pod(pod, security).await
        };

        match &target.selector {
            crate::TargetSelector::PodName(name) => {
                let pod = pod_api.get(name).await?;
                let pod_target = target.find(&pod, None)?;
                Ok((pod_target, fetch_sec(&pod).await?))
            }
            crate::TargetSelector::PodLabel(selector) => {
                let pods = pod_api.list(&Self::pod_params(selector)).await?;
                let pod = ready_pod.select(&pods.items, selector)?;
                let pod_target = target.find(pod, None)?;
                Ok((pod_target, fetch_sec(pod).await?))
            }
            crate::TargetSelector::ServiceLabel(selector) => {
                let pods = pod_api.list(&Self::pod_params(selector)).await?;
                let pod = ready_pod.select(&pods.items, selector)?;

                let services = svc_api.list(&Self::svc_params(selector)).await?;
                let service = match services.items.into_iter().next() {
                    Some(service) => Ok(service),
                    None => Err(anyhow::anyhow!("Service '{selector}' not found")),
                }?;

                let svc = service.spec.context("Spec is not defined")?;
                let ports = svc.ports.unwrap_or_default();
                let port = ports
                    .into_iter()
                    .find(|p| {
                        p.name.as_ref() == target.port.name()
                            || Some(p.port) == target.port.number()
                    })
                    .context("No port found in pod")?;

                let pod_target = target.find(pod, port.target_port.map(|p| p.into()))?;
                Ok((pod_target, fetch_sec(pod).await?))
            }
        }
    }

    /// Fetches `ClientSecurity` from secrets mounted in the resolved pod.
    async fn fetch_client_security_from_pod(
        &self,
        pod: &Pod,
        security: &ClientSecurity,
    ) -> Result<ClientSecurity, Error> {
        let mut security = security.clone();
        let secret_refs = self.secret_refs_from_pod(pod, &mut security);
        self.fetch_secret_data_from_refs(secret_refs, security)
            .await
    }

    async fn fetch_secret_data_from_refs(
        &self,
        secret_refs: Vec<crate::SecretRef>,
        security: ClientSecurity,
    ) -> Result<ClientSecurity, Error> {
        let mut security = security.clone();

        for secret_ref in secret_refs {
            match secret_ref.kind {
                crate::SecretPurpose::Tls(ref mode) => {
                    if matches!(mode, crate::TlsModeAnno::None) {
                        security.tls = TlsMode::None;
                        continue;
                    }
                    // If the secret is for TLS and the client security doesn't require discovery, skip fetching it.
                    if !security.tls.is_auto() {
                        continue;
                    }
                    let data = self.read_tls_secret(&secret_ref, mode).await?;
                    security.tls = TlsMode::from(data);
                }
                crate::SecretPurpose::Jwt(ref mode) => {
                    if matches!(mode, crate::AuthModeAnno::None) {
                        security.jwt = None;
                        continue;
                    }
                    // If the secret is for JWT and the client security doesn't require discovery, skip fetching it.
                    if security.jwt.is_some() {
                        continue;
                    }
                    let data = self.read_jwt_secret(&secret_ref, mode).await?;
                    security.jwt = data.jwt;
                }
            }
        }
        security.discover = false;

        Ok(security)
    }

    fn tls_key(&self) -> String {
        format!("{}-tls", self.anno_prefix)
    }

    fn auth_key(&self) -> String {
        format!("{}-auth", self.anno_prefix)
    }

    fn secret_refs_from_pod(
        &self,
        pod: &Pod,
        security: &mut ClientSecurity,
    ) -> Vec<crate::SecretRef> {
        let Some(spec) = pod.spec.as_ref() else {
            return vec![];
        };
        let Some(volumes) = spec.volumes.as_ref() else {
            return vec![];
        };

        let pod_anno = pod.annotations();
        let binding = String::new();

        let tls_mode = pod_anno.get(&self.tls_key()).unwrap_or(&binding);
        let (tls_mode, tls_name) = tls_mode.split_once(':').unwrap_or((tls_mode.as_str(), ""));
        let tls_mode = crate::TlsModeAnno::from(tls_mode);

        let jwt_mode = pod_anno.get(&self.auth_key()).unwrap_or(&binding);
        let (jwt_mode, jwt_name) = jwt_mode.split_once(':').unwrap_or((jwt_mode.as_str(), ""));
        let jwt_mode = crate::AuthModeAnno::from(jwt_mode);

        if matches!(tls_mode, crate::TlsModeAnno::None) {
            security.tls = TlsMode::None;
        }
        if matches!(jwt_mode, crate::AuthModeAnno::None) {
            security.jwt = None;
        }

        let mut secret_refs = Vec::new();
        for volume in volumes {
            let Some(secret) = volume.secret.as_ref() else {
                continue;
            };
            let Some(secret_name) = secret.secret_name.as_ref() else {
                continue;
            };

            if !matches!(tls_mode, crate::TlsModeAnno::None) && volume.name == tls_name {
                secret_refs.push(crate::SecretRef::new_tls(secret_name.clone(), tls_mode));
            }
            if !matches!(jwt_mode, crate::AuthModeAnno::None) && volume.name == jwt_name {
                secret_refs.push(crate::SecretRef::new_jwt(secret_name.clone(), jwt_mode));
            }
        }

        secret_refs
    }

    async fn read_tls_secret(
        &self,
        secret_ref: &crate::SecretRef,
        mode: &crate::TlsModeAnno,
    ) -> Result<crate::SecretData, Error> {
        if matches!(mode, crate::TlsModeAnno::None | crate::TlsModeAnno::Auto) {
            return Ok(crate::SecretData::default());
        }

        let secret = self.secret_api.get(&secret_ref.name).await?;
        let mut data = secret.data.unwrap_or_default();
        let get = |key: &str,
                   data: &mut std::collections::BTreeMap<String, k8s_openapi::ByteString>|
         -> Option<Vec<u8>> { data.remove(key).map(|b| b.0) };
        Ok(match mode {
            crate::TlsModeAnno::None | crate::TlsModeAnno::Auto => crate::SecretData::default(),
            crate::TlsModeAnno::ClientVerify => crate::SecretData {
                client_certificate: get(secret_ref.cert_key(), &mut data),
                client_key: get(secret_ref.key_key(), &mut data),
                ..Default::default()
            },
            crate::TlsModeAnno::ServerVerify => crate::SecretData {
                ca_certificate: get(secret_ref.ca_key(), &mut data),
                ..Default::default()
            },
            crate::TlsModeAnno::Mtls => crate::SecretData {
                ca_certificate: get(secret_ref.ca_key(), &mut data),
                client_certificate: get(secret_ref.cert_key(), &mut data),
                client_key: get(secret_ref.key_key(), &mut data),
                ..Default::default()
            },
        })
    }
    async fn read_jwt_secret(
        &self,
        secret_ref: &crate::SecretRef,
        mode: &crate::AuthModeAnno,
    ) -> Result<crate::SecretData, Error> {
        match mode {
            crate::AuthModeAnno::None => return Ok(crate::SecretData::default()),
            crate::AuthModeAnno::Jwt => {}
        }
        let secret = self.secret_api.get(&secret_ref.name).await?;
        let mut data = secret.data.unwrap_or_default();
        let jwt = data
            .remove(secret_ref.jwt_key())
            .and_then(|b| String::from_utf8(b.0).ok());
        Ok(crate::SecretData {
            jwt,
            ..Default::default()
        })
    }
    fn pod_params(selector: &str) -> ListParams {
        ListParams::default()
            .labels(selector)
            .fields("status.phase=Running")
    }
    fn svc_params(selector: &str) -> ListParams {
        ListParams::default().labels(selector)
    }
}
