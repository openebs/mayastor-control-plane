//! IoEngine CSI plugin.
//!
//! Implementation of gRPC methods from the CSI spec. This includes mounting
//! of volumes using iscsi/nvmf protocols on the node.

use crate::{
    identity::Identity, mount::probe_filesystems, node::Node,
    nodeplugin_grpc::NodePluginGrpcServer, nodeplugin_nvme::NvmeOperationsSvc,
    shutdown_event::Shutdown,
};
use clap::Arg;
use csi_driver::csi::{identity_server::IdentityServer, node_server::NodeServer};
use futures::TryFutureExt;
use grpc::csi_node_nvme::nvme_operations_server::NvmeOperationsServer;
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

pub(super) async fn main() -> anyhow::Result<()> {
    let matches = clap::Command::new(utils::package_description!())
        .about("k8s sidecar for IoEngine implementing CSI among others")
        .version(utils::version_info_str!())
        .arg(
            Arg::new("csi-socket")
                .short('c')
                .long("csi-socket")
                .value_name("PATH")
                .help("CSI gRPC listen socket (default /var/tmp/csi.sock)")
        )
        .arg(
            Arg::new("node-name")
                .short('n')
                .long("node-name")
                .value_name("NAME")
                .help("Unique node name where this instance runs")
                .required(true)
        )
        .arg(
            Arg::new("grpc-endpoint")
                .short('g')
                .long("grpc-endpoint")
                .value_name("NAME")
                .help("ip address where this instance runs, and optionally the gRPC port")
                .default_value("0.0.0.0")
                .required(false)
        )
        .arg(
            Arg::new("v")
                .short('v')
                .action(clap::ArgAction::Count)
                .help("Sets the verbosity level")
        )
        .arg(
            Arg::new("nvme-core-io-timeout")
                .long("nvme-core-io-timeout")
                .value_name("TIMEOUT")
                .required(false)
                .help("Sets the global nvme_core module io_timeout, in seconds")
        )
        .arg(
            Arg::new(crate::config::nvme_nr_io_queues())
                .long(crate::config::nvme_nr_io_queues())
                .value_name("NUMBER")
                .required(false)
                .help("Sets the nvme-nr-io-queues parameter when connecting to a volume target")
        )
        .arg(
            Arg::new(crate::config::nvme_ctrl_loss_tmo())
                .long(crate::config::nvme_ctrl_loss_tmo())
                .value_name("NUMBER")
                .required(false)
                .help("Sets the nvme-ctrl-loss-tmo parameter when connecting to a volume target. (May be overridden through the storage class)")
        )
        .arg(
            Arg::new(crate::config::nvme_keep_alive_tmo())
                .long(crate::config::nvme_keep_alive_tmo())
                .value_name("NUMBER")
                .required(false)
                .help("Sets the nvme-keep-alive-tmo parameter when connecting to a volume target")
        )
        .arg(
            Arg::new("node-selector")
                .long("node-selector")
                .action(clap::ArgAction::Append)
                .num_args(1)
                .allow_hyphen_values(true)
                .default_value(csi_driver::csi_node_selector())
                .help(
                    "The node selector label which this plugin will report as part of its topology.\n\
                    Example:\n --node-selector key=value --node-selector key2=value2",
                ),
        )
        .get_matches();

    utils::print_package_info!();

    let endpoint = matches.get_one::<String>("grpc-endpoint").unwrap();
    let csi_socket = matches
        .get_one::<String>("csi-socket")
        .map(|s| s.as_str())
        .unwrap_or("/var/tmp/csi.sock");

    let tags = utils::tracing_telemetry::default_tracing_tags(
        utils::raw_version_str(),
        env!("CARGO_PKG_VERSION"),
    );
    utils::tracing_telemetry::init_tracing("csi-node", tags, None);

    if let Some(nvme_io_timeout_secs) = matches.get_one::<String>("nvme-core-io-timeout") {
        let io_timeout_secs: u32 = nvme_io_timeout_secs.parse().expect(
            "nvme_core io_timeout should be an integer number, representing the timeout in seconds",
        );

        if let Err(error) = crate::dev::nvmf::set_nvmecore_iotimeout(io_timeout_secs) {
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
        format!("{endpoint}:{GRPC_PORT}")
    }
    .parse()?;

    *crate::config::config().nvme_as_mut() = TryFrom::try_from(&matches)?;
    tokio::select! {
        result = CsiServer::run(csi_socket, &matches)? => {
            result?;
        }
        result = NodePluginGrpcServer::run(sock_addr) => {
            result?;
        }
    }
    Ok(())
}

struct CsiServer {}

impl CsiServer {
    fn run(
        csi_socket: &str,
        cli_args: &clap::ArgMatches,
    ) -> anyhow::Result<impl Future<Output = anyhow::Result<()>>> {
        let node_name = cli_args.get_one::<String>("node-name").expect("required");
        let node_selector = csi_driver::csi_node_selector_parse(
            cli_args
                .get_many::<String>("node-selector")
                .map(|s| s.map(|s| s.as_str())),
        )?;

        let incoming = {
            let uds = UnixListener::bind(csi_socket).unwrap();
            info!("CSI plugin bound to {}", csi_socket);

            // Change permissions on CSI socket to allow non-privileged clients to access it
            // to simplify testing.
            if let Err(e) = fs::set_permissions(
                csi_socket,
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
                .add_service(NvmeOperationsServer::new(NvmeOperationsSvc {}))
                .serve_with_incoming_shutdown(incoming, Shutdown::wait())
                .await
                .map_err(|error| {
                    use stor_port::transport_api::ErrorChain;
                    error!(error = error.full_string(), "CsiServer failed");
                    error.into()
                })
        })
    }
}
