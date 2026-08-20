use std::convert::TryFrom;
use tokio::time::{sleep, Duration};
use tonic::transport::Endpoint;

use crate::infra::{async_trait, Builder, ComponentAction, Error, HaClusterAgent, StartOptions};
use composer::{Binary, ContainerSpec};

#[async_trait]
impl ComponentAction for HaClusterAgent {
    fn configure(&self, options: &StartOptions, cfg: Builder) -> Result<Builder, Error> {
        let mut binary = Binary::from_dbg("agent-ha-cluster").with_args(vec!["-g=[::]:11500"]);
        if !options.no_grpc_tls {
            binary = binary.with_arg("--grpc-auto-tls");
        }
        let mut spec =
            ContainerSpec::from_binary("agent-ha-cluster", binary).with_portmap("11500", "11500");

        if let Some(env) = &options.agents_env {
            for kv in env {
                spec = spec.with_env(kv.key.as_str(), kv.value.as_str().as_ref());
            }
        }
        if let Some(period) = options.cluster_fast_requeue {
            spec = spec.with_args(vec!["--fast-requeue", period.to_string().as_str()]);
        }

        let etcd = format!("etcd.{}:2379", options.cluster_label.name());
        spec = spec.with_args(vec!["--store", &etcd]);

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
        cfg.start("agent-ha-cluster").await?;
        Ok(())
    }

    async fn wait_on(
        &self,
        options: &StartOptions,
        cfg: &crate::ComposeTestNt,
    ) -> Result<(), Error> {
        // Wait till cluster-agent's gRPC server is ready to server the request
        loop {
            // The auto-tls connector performs the TLS handshake itself, so the endpoint always
            // uses an http scheme to stop tonic from applying (and rejecting) its own TLS logic.
            let endpoint = Endpoint::try_from(format!(
                "http://{}:11500",
                cfg.container_ip("agent-ha-cluster")
            ))?
            .connect_timeout(Duration::from_millis(100));
            let connect = match options.no_grpc_tls {
                true => endpoint.connect().await,
                false => grpc::tls::auto_tls_connect(&endpoint).await,
            };
            match connect {
                Ok(_) => break,
                Err(error) => {
                    tracing::error!(?error, "Failed to connect to cluster-agent gRPC server");
                    sleep(Duration::from_millis(25)).await;
                }
            }
        }
        Ok(())
    }
}
