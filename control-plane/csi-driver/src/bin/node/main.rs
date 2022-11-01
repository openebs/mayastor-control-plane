//! IoEngine CSI plugin.
//!
//! Implementation of gRPC methods from the CSI spec. This includes mounting
//! of volumes using iscsi/nvmf protocols on the node.

use crate::{identity::Identity, mount::probe_filesystems, node::Node, shutdown_event::Shutdown};
use clap::{App, Arg};
use csi_driver::csi::{identity_server::IdentityServer, node_server::NodeServer};
use futures::TryFutureExt;
use nodeplugin_grpc::NodePluginGrpcServer;
use std::{
    env, fs,
    future::Future,
    io::ErrorKind,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::UnixListener,
};
use tonic::transport::{server::Connected, Server};
use tracing::{debug, error, info};

mod block_vol;
/// Configuration Parameters
pub(crate) mod config;
mod dev;
mod error;
mod filesystem_vol;
mod findmnt;
mod format;
mod identity;
mod match_dev;
mod mount;
mod node;
mod nodeplugin_grpc;
mod nodeplugin_svc;

/// Shutdown event which lets the plugin know it needs to stop processing new events and
/// complete any existing ones before shutting down.
pub(crate) mod shutdown_event;

#[derive(Clone, Debug)]
pub struct UdsConnectInfo {
    pub peer_addr: Option<Arc<tokio::net::unix::SocketAddr>>,
    pub peer_cred: Option<tokio::net::unix::UCred>,
}

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

const GRPC_PORT: u16 = 50051;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    main_().await.map_err(|error| {
        tracing::error!(%error, "Terminated with error");
        error
    })
}
async fn main_() -> anyhow::Result<()> {
    let matches = App::new(utils::package_description!())
        .about("k8s sidecar for IoEngine implementing CSI among others")
        .version(utils::version_info_str!())
        .arg(
            Arg::with_name("csi-socket")
                .short("c")
                .long("csi-socket")
                .value_name("PATH")
                .help("CSI gRPC listen socket (default /var/tmp/csi.sock)")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("node-name")
                .short("n")
                .long("node-name")
                .value_name("NAME")
                .help("Unique node name where this instance runs")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("grpc-endpoint")
                .short("g")
                .long("grpc-endpoint")
                .value_name("NAME")
                .help("ip address where this instance runs, and optionally the gRPC port")
                .default_value("0.0.0.0")
                .required(false)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("v")
                .short("v")
                .multiple(true)
                .help("Sets the verbosity level"),
        )
        .arg(
            Arg::with_name("nvme-core-io-timeout")
                .long("nvme-core-io-timeout")
                .value_name("TIMEOUT")
                .takes_value(true)
                .required(false)
                .help("Sets the global nvme_core module io_timeout, in seconds"),
        )
        .arg(
            Arg::with_name(Box::leak(crate::config::nvme_nr_io_queues().into_boxed_str()))
                .long(&crate::config::nvme_nr_io_queues())
                .value_name("NUMBER")
                .takes_value(true)
                .required(false)
                .help("Sets the nvme-nr-io-queues parameter when connecting to a volume target"),
        )
        .arg(
            Arg::with_name(Box::leak(crate::config::nvme_ctrl_loss_tmo().into_boxed_str()))
                .long(&crate::config::nvme_ctrl_loss_tmo())
                .value_name("NUMBER")
                .takes_value(true)
                .required(false)
                .help("Sets the nvme-ctrl-loss-tmo parameter when connecting to a volume target. (May be overridden through the storage class)"),
        )
        .arg(
            Arg::with_name("node-selector")
                .long("node-selector")
                .multiple(true)
                .number_of_values(1)
                .allow_hyphen_values(true)
                .default_value(Box::leak(csi_driver::csi_node_selector().into_boxed_str()))
                .help(
                    "The node selector label which this plugin will report as part of its topology.\n\
                    Example:\n --node-selector key=value --node-selector key2=value2",
                ),
        )
        .get_matches();

    utils::print_package_info!();

    let endpoint = matches.value_of("grpc-endpoint").unwrap();
    let csi_socket = matches
        .value_of("csi-socket")
        .unwrap_or("/var/tmp/csi.sock");

    let tags = utils::tracing_telemetry::default_tracing_tags(
        utils::raw_version_str(),
        env!("CARGO_PKG_VERSION"),
    );
    utils::tracing_telemetry::init_tracing("csi-node", tags, None);

    if let Some(nvme_io_timeout_secs) = matches.value_of("nvme_core io_timeout") {
        let io_timeout_secs: u32 = nvme_io_timeout_secs.parse().expect(
            "nvme_core io_timeout should be an integer number, representing the timeout in seconds",
        );

        if let Err(error) = dev::nvmf::set_nvmecore_iotimeout(io_timeout_secs) {
            anyhow::bail!("Failed to set nvme_core io_timeout: {}", error);
        }
    }

    // Remove stale CSI socket from previous instance if there is any
    match fs::remove_file(csi_socket) {
        Ok(_) => info!("Removed stale CSI socket {}", csi_socket),
        Err(err) => {
            if err.kind() != ErrorKind::NotFound {
                anyhow::bail!("Error removing stale CSI socket {}: {}", csi_socket, err);
            }
        }
    }

    let sock_addr = if endpoint.contains(':') {
        endpoint.to_string()
    } else {
        format!("{}:{}", endpoint, GRPC_PORT)
    };

    *config::config().nvme_as_mut() = TryFrom::try_from(&matches)?;

    let (csi, grpc) = tokio::join!(
        CsiServer::run(csi_socket, &matches)?,
        NodePluginGrpcServer::run(sock_addr.parse().expect("Invalid gRPC endpoint")),
    );
    vec![csi, grpc].into_iter().collect()
}

struct CsiServer {}

impl CsiServer {
    fn run(
        csi_socket: &str,
        cli_args: &clap::ArgMatches<'_>,
    ) -> anyhow::Result<impl Future<Output = anyhow::Result<()>>> {
        let node_name = cli_args.value_of("node-name").expect("required");
        let node_selector =
            csi_driver::csi_node_selector_parse(cli_args.values_of("node-selector"))?;

        let incoming = {
            let uds = UnixListener::bind(csi_socket).unwrap();
            info!("CSI plugin bound to {}", csi_socket);

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

        let node = Node::new(node_name.into(), node_selector, probe_filesystems());
        Ok(async move {
            Server::builder()
                .add_service(NodeServer::new(node))
                .add_service(IdentityServer::new(Identity {}))
                .serve_with_incoming_shutdown(incoming, Shutdown::wait())
                .await
                .map_err(|error| {
                    error!(?error, "CSI server failed");
                    anyhow::anyhow!("CSI server failed with error: {}", error)
                })
        })
    }
}
