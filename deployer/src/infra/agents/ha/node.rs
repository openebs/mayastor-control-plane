use crate::infra::*;

use tokio::time::{sleep, Duration};
use tonic::transport::Endpoint;

#[async_trait]
impl ComponentAction for HaNodeAgent {
    fn configure(&self, _options: &StartOptions, cfg: Builder) -> Result<Builder, Error> {
        let mut spec = ContainerSpec::from_binary(
            "agent-ha-node",
            Binary::from_dbg("agent-ha-node").with_arg(format!("-n{}", CsiNode::name(0)).as_str()),
        )
        .with_bind("/run/udev", "/run/udev:ro")
        .with_bind("/dev", "/dev:ro")
        .with_privileged(Some(true))
        .with_portmap("11600", "11600");

        if cfg.container_exists("jaeger") {
            let jaeger_config = format!("jaeger.{}:6831", cfg.get_name());
            spec = spec.with_args(vec!["--jaeger", &jaeger_config])
        };

        Ok(cfg.add_container_spec(spec))
    }

    async fn start(&self, _options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
        cfg.start("agent-ha-node").await?;
        Ok(())
    }

    async fn wait_on(&self, _options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
        // Wait till node-agent's gRPC server is ready to server the request
        loop {
            match Endpoint::try_from(format!(
                "https://{}:11600",
                cfg.container_ip("agent-ha-node")
            ))?
            .connect_timeout(Duration::from_millis(100))
            .connect()
            .await
            {
                Ok(_) => break,
                Err(_) => sleep(Duration::from_millis(1000)).await,
            }
        }
        Ok(())
    }
}
