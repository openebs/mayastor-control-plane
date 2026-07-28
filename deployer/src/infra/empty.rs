use crate::infra::{async_trait, Builder, ComponentAction, Empty, Error, StartOptions};

#[async_trait]
impl ComponentAction for Empty {
    fn configure(&self, _options: &StartOptions, cfg: Builder) -> Result<Builder, Error> {
        Ok(cfg)
    }
    async fn start(
        &self,
        _options: &StartOptions,
        _cfg: &crate::ComposeTestNt,
    ) -> Result<(), Error> {
        Ok(())
    }
    async fn wait_on(
        &self,
        _options: &StartOptions,
        _cfg: &crate::ComposeTestNt,
    ) -> Result<(), Error> {
        Ok(())
    }
}
