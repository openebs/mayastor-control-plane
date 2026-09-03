use crate::infra::{
    async_trait, Builder, ComponentAction, CsiNode, Error, HaNodeAgent, StartOptions,
};
use composer::{Binary, ContainerSpec};
use std::str::FromStr;
use tokio::time::{sleep, Duration};
use tonic::transport::Endpoint;

#[async_trait]
impl ComponentAction for HaNodeAgent {
    fn configure(&self, options: &StartOptions, cfg: Builder) -> Result<Builder, Error> {
        let socket = format!("-g{}:11600", cfg.next_ip_for_name("agent-ha-node")?);
        let mut binary = Binary::from_dbg("agent-ha-node")
            .with_arg(format!("-n{}", CsiNode::name(0)).as_str())
            .with_arg(socket.as_str())
            // Hardcoding the csi-socket file for now as we can launch only one instance
            // of ha node agent. TODO: Map csi-node with ha-node.
            .with_args(vec!["--csi-socket", "/var/tmp/csi-app-node-1.sock"]);
        if !options.no_grpc_tls {
            binary = binary.with_arg("--grpc-auto-tls");
        }
        let mut spec = ContainerSpec::from_binary("agent-ha-node", binary)
            .with_bypass_default_mounts(true)
            .with_bind("/var/tmp", "/var/tmp")
            .with_bind("/run/udev", "/run/udev:ro")
            .with_bind("/dev", "/dev:ro")
            .with_privileged(Some(true))
            .with_portmap("11600", "11600");

        if let Some(env) = &options.agents_env {
            for kv in env {
                spec = spec.with_env(kv.key.as_str(), kv.value.as_str().as_ref());
            }
        }
        if cfg.container_exists("jaeger") {
            let jaeger_config = format!("jaeger.{}", cfg.get_name());
            spec = spec.with_args(vec!["--jaeger", &jaeger_config])
        };
        if options.eventing {
            let nats_server_url = "nats://nats:4222";
            spec = spec.with_args(vec!["--events-url", nats_server_url]);
        };

        Ok(cfg.add_container_spec(spec))
    }

    async fn start(
        &self,
        _options: &StartOptions,
        cfg: &crate::ComposeTestNt,
    ) -> Result<(), Error> {
        cfg.start("agent-ha-node").await?;
        Ok(())
    }

    async fn wait_on(
        &self,
        options: &StartOptions,
        cfg: &crate::ComposeTestNt,
    ) -> Result<(), Error> {
        // Wait till node-agent's gRPC server is ready to server the request
        wait_on(
            &format!("http://{}:11600", cfg.container_ip("agent-ha-node")),
            options.no_grpc_tls,
        )
        .await?;

        wait_on("http://10.1.0.1:11600", options.no_grpc_tls).await?;

        Ok(())
    }
}

async fn wait_on(uri: &str, no_grpc_tls: bool) -> Result<(), Error> {
    // Wait till node-agent's gRPC server is ready to server the request
    loop {
        // The auto-tls connector performs the TLS handshake itself, so the endpoint always
        // uses an http scheme to stop tonic from applying (and rejecting) its own TLS logic.
        let endpoint = Endpoint::from_str(uri)?.connect_timeout(Duration::from_millis(100));
        let connect = match no_grpc_tls {
            true => endpoint.connect().await,
            false => grpc::tls::auto_tls_connect(&endpoint).await,
        };
        match connect {
            Ok(_) => break,
            Err(_) => sleep(Duration::from_millis(25)).await,
        }
    }
    Ok(())
}
