use super::*;
use grpc::operations::jsongrpc::client::JsonGrpcClient;

#[async_trait]
impl ComponentAction for JsonGrpc {
    fn configure(&self, options: &StartOptions, cfg: Builder) -> Result<Builder, Error> {
        let name = "jsongrpc";
        if options.build {
            let status = std::process::Command::new("cargo")
                .args(&["build", "-p", "agents", "--bin", name])
                .status()?;
            build_error(&format!("the {} agent", name), status.code())?;
        }
        let mut binary = Binary::from_dbg(name);

        if let Some(env) = &options.agents_env {
            for kv in env {
                binary = binary.with_env(kv.key.as_str(), kv.value.as_str().as_ref());
            }
        }

        if let Some(size) = &options.otel_max_batch_size {
            binary = binary.with_env("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", size);
        }
        Ok(cfg.add_container_bin(name, binary))
    }
    async fn start(&self, _options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
        cfg.start("jsongrpc").await?;
        Ok(())
    }
    async fn wait_on(&self, _options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
        let ip = cfg.container_ip("jsongrpc");
        let uri = tonic::transport::Uri::from_str(&format!("https://{}:50052", ip)).unwrap();
        let timeout =
            grpc::context::TimeoutOptions::new().with_timeout(std::time::Duration::from_millis(5));
        let json_grpc = JsonGrpcClient::new(uri, Some(timeout.with_max_retries(Some(10)))).await;
        json_grpc.wait_ready(None).await.map_err(|_| {
            let error = "Failed to wait for jsongrpc service to get ready";
            std::io::Error::new(std::io::ErrorKind::TimedOut, error)
        })?;
        Ok(())
    }
}
