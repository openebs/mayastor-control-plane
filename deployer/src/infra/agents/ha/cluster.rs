use std::convert::TryFrom;
use tokio::time::{sleep, Duration};
use tonic::transport::Endpoint;

use crate::infra::{
    async_trait, Builder, ComponentAction, ComposeTest, Error, HaClusterAgent, StartOptions,
};
use composer::{Binary, ContainerSpec};

#[async_trait]
impl ComponentAction for HaClusterAgent {
    fn configure(&self, options: &StartOptions, cfg: Builder) -> Result<Builder, Error> {
        let mut spec = ContainerSpec::from_binary(
            "agent-ha-cluster",
            Binary::from_dbg("agent-ha-cluster").with_args(vec!["-g=0.0.0.0:11500"]),
        )
        .with_portmap("11500", "11500");

        let etcd = format!("etcd.{}:2379", options.cluster_label.name());
        spec = spec.with_args(vec!["--store", &etcd]);

        if cfg.container_exists("jaeger") {
            let jaeger_config = format!("jaeger.{}:6831", cfg.get_name());
            spec = spec.with_args(vec!["--jaeger", &jaeger_config])
        };

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
                Err(_) => sleep(Duration::from_millis(100)).await,
            }
        }
        Ok(())
    }
}
