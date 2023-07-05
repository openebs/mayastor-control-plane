use crate::infra::{
    async_trait, Builder, ComponentAction, ComposeTest, Empty, Error, StartOptions,
};

#[async_trait]
impl ComponentAction for Empty {
    fn configure(&self, _options: &StartOptions, cfg: Builder) -> Result<Builder, Error> {
        Ok(cfg)
    }
    async fn start(&self, _options: &StartOptions, _cfg: &ComposeTest) -> Result<(), Error> {
        Ok(())
    }

    async fn restart(&self, _options: &StartOptions, _cfg: &ComposeTest) -> Result<(), Error> {
        Ok(())
    }

    async fn wait_on(&self, _options: &StartOptions, _cfg: &ComposeTest) -> Result<(), Error> {
        Ok(())
    }
}
