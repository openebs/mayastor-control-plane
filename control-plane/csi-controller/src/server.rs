use crate::MayastorApiClient;
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
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use rpc::csi::{controller_server::ControllerServer, identity_server::IdentityServer};

use crate::{controller::CsiControllerSvc, identity::CsiIdentitySvc};

#[derive(Debug)]
struct UnixStream(pub tokio::net::UnixStream);

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

async fn ping_rest_api() {
    info!("Checking REST API endpoint accessibility ...");

    match MayastorApiClient::get_client().list_nodes().await {
        Err(e) => error!(?e, "REST API endpoint is not accessible"),
        Ok(nodes) => {
            let names: Vec<String> = nodes.into_iter().map(|n| n.id).collect();
            info!(
                "REST API endpoints available, {} Mayastor node(s) reported: {:?}",
                names.len(),
                names,
            );
        }
    };
}

impl CsiServer {
    pub async fn run(csi_socket: String) -> Result<(), String> {
        // Remove existing CSI socket from previous runs.
        match fs::remove_file(&csi_socket) {
            Ok(_) => debug!("Removed stale CSI socket {}", csi_socket),
            Err(err) => {
                if err.kind() != ErrorKind::NotFound {
                    return Err(format!(
                        "Error removing stale CSI socket {}: {}",
                        csi_socket, err
                    ));
                }
            }
        }

        info!("CSI RPC server is listening on {}", csi_socket);

        let incoming = {
            let uds = UnixListener::bind(csi_socket).map_err(|_e| "Failed to bind CSI socket")?;

            async_stream::stream! {
                while let item = uds.accept().map_ok(|(st, _)| UnixStream(st)).await {
                    yield item;
                }
            }
        };

        // Try to detect REST API endpoint to debug the accessibility status.
        ping_rest_api().await;

        Server::builder()
            .add_service(IdentityServer::new(CsiIdentitySvc::default()))
            .add_service(ControllerServer::new(CsiControllerSvc::default()))
            .serve_with_incoming(incoming)
            .await
            .map_err(|_| "Failed to start gRPC server")?;

        Ok(())
    }
}
