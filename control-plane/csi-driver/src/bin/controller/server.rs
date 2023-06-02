use crate::IoEngineApiClient;
use futures::TryFutureExt;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::UnixListener,
};
use tonic::transport::{server::Connected, Server};
use tracing::{debug, error, info};

use std::{
    fs,
    io::ErrorKind,
    ops::Add,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use rpc::csi::{controller_server::ControllerServer, identity_server::IdentityServer};

use crate::{controller::CsiControllerSvc, identity::CsiIdentitySvc};

#[derive(Debug)]
struct UnixStream(tokio::net::UnixStream);

impl Connected for UnixStream {
    type ConnectInfo = UdsConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        UdsConnectInfo {
            peer_addr: self.0.peer_addr().ok().map(Arc::new),
            peer_cred: self.0.peer_cred().ok(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct UdsConnectInfo {
    pub peer_addr: Option<Arc<tokio::net::unix::SocketAddr>>,
    pub peer_cred: Option<tokio::net::unix::UCred>,
}

impl AsyncRead for UnixStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for UnixStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

pub struct CsiServer {}

#[tracing::instrument]
async fn ping_rest_api() {
    info!("Checking REST API endpoint accessibility ...");

    match IoEngineApiClient::get_client().list_nodes().await {
        Err(e) => error!(?e, "REST API endpoint is not accessible"),
        Ok(nodes) => {
            let names: Vec<String> = nodes.into_iter().map(|n| n.id).collect();
            info!(
                "REST API endpoints available, {} IoEngine node(s) reported: {:?}",
                names.len(),
                names,
            );
        }
    }
}

impl CsiServer {
    pub async fn run(csi_socket: String) -> Result<(), String> {
        // Remove existing CSI socket from previous runs.
        match fs::remove_file(&csi_socket) {
            Ok(_) => info!("Removed stale CSI socket {}", csi_socket),
            Err(err) => {
                if err.kind() != ErrorKind::NotFound {
                    return Err(format!(
                        "Error removing stale CSI socket {csi_socket}: {err}"
                    ));
                }
            }
        }

        debug!("CSI RPC server is listening on {}", csi_socket);

        let incoming = {
            let uds = UnixListener::bind(&csi_socket).map_err(|_e| "Failed to bind CSI socket")?;

            // Change permissions on CSI socket to allow non-privileged clients to access it
            // to simplify testing.
            if let Err(e) = fs::set_permissions(
                &csi_socket,
                std::os::unix::fs::PermissionsExt::from_mode(0o777),
            ) {
                error!("Failed to change permissions for CSI socket: {:?}", e);
            } else {
                debug!("Successfully changed file permissions for CSI socket");
            }

            async_stream::stream! {
                loop {
                    let item = uds.accept().map_ok(|(st, _)| UnixStream(st)).await;
                    yield item;
                }
            }
        };

        // Try to detect REST API endpoint to debug the accessibility status.
        ping_rest_api().await;

        let cfg = crate::CsiControllerConfig::get_config();

        Server::builder()
            .timeout(cfg.io_timeout().add(std::time::Duration::from_secs(3)))
            .add_service(IdentityServer::new(CsiIdentitySvc::default()))
            .add_service(ControllerServer::new(CsiControllerSvc::new(cfg)))
            .serve_with_incoming_shutdown(incoming, shutdown::Shutdown::wait())
            .await
            .map_err(|_| "Failed to start gRPC server")?;
        Ok(())
    }
}
