use super::*;

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
        if !options.no_nats {
            binary = binary.with_nats("-n");
        }
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
    async fn wait_on(&self, options: &StartOptions, _cfg: &ComposeTest) -> Result<(), Error> {
        if !options.no_nats {
            Liveness {}
                .request_on_bus(ChannelVs::JsonGrpc, bus())
                .await?;
        }
        Ok(())
    }
}
