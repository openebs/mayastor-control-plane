use tokio::time::{sleep, Duration};
use tonic::transport::Endpoint;

use crate::infra::*;

#[async_trait]
impl ComponentAction for HaClusterAgent {
    fn configure(&self, _options: &StartOptions, cfg: Builder) -> Result<Builder, Error> {
        let spec = ContainerSpec::from_binary(
            "agent-ha-cluster",
            Binary::from_dbg("agent-ha-cluster").with_args(vec!["-g=0.0.0.0:11500"]),
        )
        .with_portmap("11500", "11500");

        Ok(cfg.add_container_spec(spec))
    }

    async fn start(&self, _options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
        cfg.start("agent-ha-cluster").await?;
        Ok(())
    }

    async fn wait_on(&self, _options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
        // Wait till cluster-agent's gRPC server is ready to server the request
        loop {
            match Endpoint::try_from(format!(
                "https://{}:11500",
                cfg.container_ip("agent-ha-cluster")
            ))?
            .connect_timeout(Duration::from_millis(100))
            .connect()
            .await
            {
                Ok(_) => break,
                Err(error) => {
                    println!("TIMEOUT! {error}");
                    sleep(Duration::from_millis(100)).await
                }
            }
        }
        Ok(())
    }
}
