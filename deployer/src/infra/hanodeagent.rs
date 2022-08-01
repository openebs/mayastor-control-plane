use super::*;

#[async_trait]
impl ComponentAction for HANodeAgent {
    fn configure(&self, _options: &StartOptions, cfg: Builder) -> Result<Builder, Error> {
        let spec = ContainerSpec::from_binary("agent-ha-node", Binary::from_dbg("agent-ha-node"));

        Ok(cfg.add_container_spec(spec))
    }

    async fn start(&self, _options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
        cfg.start("agent-ha-node").await?;
        Ok(())
    }
}
