use crate::{
    build_error,
    infra::{async_trait, Builder, ComponentAction, DrAgent, Error, StartOptions},
};
use composer::Binary;
use grpc::operations::dr::client::DrClient;
use std::str::FromStr;

/// Name of the dr agent container, and the name it is reachable under from other containers
/// on the same compose network.
pub(crate) const CONTAINER: &str = "agent-dr";
/// Port the dr agent's gRPC server listens on.
pub(crate) const GRPC_PORT: u16 = 11700;

#[async_trait]
impl ComponentAction for DrAgent {
    fn configure(&self, options: &StartOptions, cfg: Builder) -> Result<Builder, Error> {
        if options.build {
            let status = std::process::Command::new("cargo")
                .args(["build", "-p", "agents", "--bin", CONTAINER])
                .status()?;
            build_error(&format!("the {CONTAINER} agent"), status.code())?;
        }
        let mut binary =
            Binary::from_dbg(CONTAINER).with_args(vec!["-g", &format!("[::]:{GRPC_PORT}")]);

        if let Some(env) = &options.agents_env {
            for kv in env {
                binary = binary.with_env(kv.key.as_str(), kv.value.as_str().as_ref());
            }
        }

        if !options.no_grpc_tls {
            binary = binary.with_arg("--grpc-auto-tls");
        }

        if cfg.container_exists("jaeger") {
            let jaeger = format!("jaeger.{}", cfg.get_name());
            binary = binary.with_args(vec!["--jaeger", &jaeger]);
        }

        if let Some(size) = &options.otel_max_batch_size {
            binary = binary.with_env("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", size);
        }

        Ok(cfg.add_container_bin(CONTAINER, options.fips_env(binary)))
    }

    async fn start(
        &self,
        _options: &StartOptions,
        cfg: &crate::ComposeTestNt,
    ) -> Result<(), Error> {
        cfg.start(CONTAINER).await?;
        Ok(())
    }

    async fn wait_on(
        &self,
        options: &StartOptions,
        cfg: &crate::ComposeTestNt,
    ) -> Result<(), Error> {
        let ip = cfg.container_ip(CONTAINER);
        let scheme = if options.no_grpc_tls { "http" } else { "https" };
        let uri = tonic::transport::Uri::from_str(&format!("{scheme}://{ip}:{GRPC_PORT}"))
            .expect("a well-formed uri");
        let timeout = grpc::context::TimeoutOptions::new()
            .with_req_timeout(std::time::Duration::from_millis(5));
        let dr = DrClient::new(uri, Some(timeout.with_max_retries(Some(10)))).await;
        dr.wait_ready(None).await.map_err(|_| {
            let error = "Failed to wait for the dr service to get ready";
            std::io::Error::new(std::io::ErrorKind::TimedOut, error)
        })?;
        Ok(())
    }
}
