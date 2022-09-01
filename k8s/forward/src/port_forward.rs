use anyhow::Context;
use futures::{StreamExt, TryStreamExt};
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

use crate::{
    pod_selection::{AnyReady, PodSelection},
    vx::{Pod, Service},
};
use kube::{
    api::{Api, ListParams},
    Client,
};

/// Used to "proxy" connections to a pod by use of port forwarding.
/// # Example
/// ```ignore
/// let selector = kube_forward::TargetSelector::pod_label("app", "etcd");
/// let target = kube_forward::Target::new(selector, "client", "mayastor");
/// let pf = kube_forward::PortForward::new(target, 35003).await?;
///
/// let (_port, handle) = pf.port_forward().await?;
/// handle.await?;
/// ```
#[derive(Clone)]
pub struct PortForward {
    target: crate::Target,
    local_port: Option<u16>,
    pod_api: Api<Pod>,
    svc_api: Api<Service>,
}

impl PortForward {
    /// Return a new `Self`.
    /// # Arguments
    /// * `target` - the target we'll forward to
    /// * `local_port` - specific local port to use, if Some
    pub async fn new(
        target: crate::Target,
        local_port: impl Into<Option<u16>>,
    ) -> anyhow::Result<Self> {
        let client = Client::try_default().await?;
        let namespace = target.namespace.name_any();

        Ok(Self {
            target,
            local_port: local_port.into(),
            pod_api: Api::namespaced(client.clone(), &namespace),
            svc_api: Api::namespaced(client, &namespace),
        })
    }

    /// The specified local port, or 0.
    /// Port 0 is special as it tells the kernel to give us the next free port.
    fn local_port(&self) -> u16 {
        self.local_port.unwrap_or(0)
    }

    /// Runs the port forwarding proxy until a SIGINT signal is received.
    pub async fn port_forward(self) -> anyhow::Result<(u16, tokio::task::JoinHandle<()>)> {
        let addr = SocketAddr::from(([127, 0, 0, 1], self.local_port()));

        let bind = TcpListener::bind(addr).await?;
        let port = bind.local_addr()?.port();
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
                        if let Err(e) = pf.forward_connection(client_conn).await {
                            tracing::error!(
                                error = e.as_ref() as &dyn std::error::Error,
                                "failed to forward connection"
                            );
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
    async fn forward_connection(
        self,
        mut client_conn: tokio::net::TcpStream,
    ) -> anyhow::Result<()> {
        let target = self.finder().find(&self.target).await?;
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
        forwarder.join().await?;
        tracing::debug!(local_port, pod_port, pod_name, "connection closed");
        Ok(())
    }
    fn finder(&self) -> TargetPodFinder {
        TargetPodFinder {
            pod_api: &self.pod_api,
            svc_api: &self.svc_api,
        }
    }
}

/// Finds a `crate::TargetPod`, which is essentially a pod name and port.
/// Note this finds the actual pod mapping and not the service.
#[derive(Clone)]
struct TargetPodFinder<'a> {
    pod_api: &'a Api<Pod>,
    svc_api: &'a Api<Service>,
}
impl<'a> TargetPodFinder<'a> {
    /// Finds the name and port of the target pod specified by the selector.
    /// # Arguments
    /// * `target` - the target to be found
    pub(crate) async fn find(&self, target: &crate::Target) -> anyhow::Result<crate::TargetPod> {
        let pod_api = self.pod_api;
        let svc_api = self.svc_api;
        let ready_pod = AnyReady {};

        match &target.selector {
            crate::TargetSelector::PodName(name) => {
                let pod = pod_api.get(name).await?;
                target.find(&pod, None)
            }
            crate::TargetSelector::PodLabel(selector) => {
                let pods = pod_api.list(&Self::pod_params(selector)).await?;
                let pod = ready_pod.select(&pods.items, selector)?;
                target.find(pod, None)
            }
            crate::TargetSelector::ServiceLabel(selector) => {
                let pods = pod_api.list(&Self::pod_params(selector)).await?;
                let pod = ready_pod.select(&pods.items, selector)?;

                let services = svc_api.list(&Self::svc_params(selector)).await?;
                let service = match services.items.into_iter().next() {
                    Some(service) => Ok(service),
                    None => Err(anyhow::anyhow!("Service '{}' not found", selector)),
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

                target.find(pod, port.target_port.map(|p| p.into()))
            }
        }
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
