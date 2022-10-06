use crate::infra::{
    async_trait, Builder, ComponentAction, ComposeTest, CsiNode, Error, StartOptions,
};
use composer::{Binary, ContainerSpec};
use std::convert::TryFrom;
use tokio::{
    net::UnixStream,
    time::{sleep, Duration},
};
use tonic::transport::{Endpoint, Uri};
use tower::service_fn;

use crate::IoEngine;
use rpc::csi::{identity_client::IdentityClient, GetPluginInfoRequest};

const CSI_NODE: &str = "csi-node";

#[async_trait]
impl ComponentAction for CsiNode {
    fn configure(&self, options: &StartOptions, mut cfg: Builder) -> Result<Builder, Error> {
        Ok(if !options.csi_node {
            cfg
        } else {
            if options.build {
                std::process::Command::new("cargo")
                    .args(&["build", "-p", "csi-driver", "--bin", CSI_NODE])
                    .status()?;
            }

            let local_nodes = if options.local_nodes {
                options.io_engines
            } else {
                0
            };

            for i in 0 .. local_nodes {
                cfg = CsiNode::with_local_node(i, options, cfg);
            }

            for i in 0 .. options.app_nodes() {
                cfg = CsiNode::with_app_node(i, cfg);
            }
            cfg
        })
    }
    async fn start(&self, options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
        if options.csi_node {
            let local_nodes = if options.local_nodes {
                options.io_engines
            } else {
                0
            };

            for i in 0 .. local_nodes {
                let container_name = Self::local_container_name(&IoEngine::name(i, options));
                cfg.start(&container_name).await?;
            }

            for i in 0 .. options.app_nodes() {
                cfg.start(&Self::container_name(i)).await?;
            }
        }
        Ok(())
    }

    async fn wait_on(&self, options: &StartOptions, _cfg: &ComposeTest) -> Result<(), Error> {
        if !options.csi_node {
            return Ok(());
        }

        let local_nodes = if options.local_nodes {
            options.io_engines
        } else {
            0
        };

        for i in 0 .. local_nodes {
            CsiNode::wait_local_node(i, options).await?;
        }

        for i in 0 .. options.app_nodes() {
            CsiNode::wait_app_node(i).await?;
        }

        Ok(())
    }
}

impl CsiNode {
    /// The node-name for the csi-node plugin instance.
    pub fn name(i: u32) -> String {
        format!("app-node-{}", i + 1)
    }
    /// The container name for the csi-node plugin instance.
    pub fn container_name(i: u32) -> String {
        format!("{}-{}", CSI_NODE, i + 1)
    }
    fn local_container_name(io_engine: &str) -> String {
        format!("{}-{}", CSI_NODE, io_engine)
    }
    /// The socket path for the csi-node plugin instance.
    pub fn socket(node_name: &str) -> String {
        format!("/var/tmp/csi-{}.sock", node_name)
    }
    fn with_app_node(index: u32, cfg: Builder) -> Builder {
        let container_name = Self::container_name(index);
        let node_name = Self::name(index);
        let socket = Self::socket(&node_name);

        Self::with_node(&container_name, &node_name, &socket, cfg)
    }
    fn with_local_node(index: u32, options: &StartOptions, cfg: Builder) -> Builder {
        let container_name = Self::local_container_name(&IoEngine::name(index, options));
        let node_name = IoEngine::name(index, options);
        let socket = Self::socket(&node_name);

        Self::with_node(&container_name, &node_name, &socket, cfg)
    }
    fn with_node(container_name: &str, node_name: &str, socket: &str, cfg: Builder) -> Builder {
        let binary = Binary::from_dbg(CSI_NODE)
            .with_args(vec!["--nvme-nr-io-queues", "1"])
            .with_args(vec!["--node-name", node_name])
            // Make sure that CSI socket is always under shared directory
            // regardless of what its default value is.
            .with_args(vec!["--csi-socket", socket]);

        let path = format!(
            "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:{}",
            env!("PATH")
        );

        cfg.add_container_spec(
            ContainerSpec::from_binary(container_name, binary)
                .with_bypass_default_mounts(true)
                .with_bind("/var/tmp", "/var/tmp")
                .with_bind("/dev", "/dev:ro")
                .with_bind("/run/udev", "/run/udev:ro")
                .with_env("PATH", path.as_str())
                .with_privileged(Some(true)),
        )
    }
    async fn wait_app_node(index: u32) -> Result<(), Error> {
        let node_name = Self::name(index);
        let socket = Self::socket(&node_name);

        Self::wait_node(&socket).await
    }
    async fn wait_local_node(index: u32, options: &StartOptions) -> Result<(), Error> {
        let node_name = Self::local_container_name(&IoEngine::name(index, options));
        let socket = Self::socket(&node_name);

        Self::wait_node(&socket).await
    }
    async fn wait_node(socket: &str) -> Result<(), Error> {
        // Step 1: Wait till CSI node's gRPC server is registered and is ready
        // to serve API requests.
        let endpoint =
            Endpoint::try_from("http://[::]")?.connect_timeout(Duration::from_millis(150));

        let channel = loop {
            let socket = socket.to_string();
            match endpoint
                .connect_with_connector(service_fn(move |_: Uri| {
                    UnixStream::connect(socket.to_string())
                }))
                .await
            {
                Ok(channel) => break channel,
                Err(_) => sleep(Duration::from_millis(150)).await,
            }
        };

        let mut client = IdentityClient::new(channel);

        // Step 2: Make sure we can perform a successful RPC call.
        loop {
            match client
                .get_plugin_info(GetPluginInfoRequest::default())
                .await
            {
                Ok(_) => break,
                Err(_) => sleep(Duration::from_millis(150)).await,
            }
        }

        Ok(())
    }
}
