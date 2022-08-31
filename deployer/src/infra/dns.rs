use crate::infra::{async_trait, Builder, ComponentAction, ComposeTest, Dns, Error, StartOptions};
use composer::ContainerSpec;

#[async_trait]
impl ComponentAction for Dns {
    fn configure(&self, options: &StartOptions, cfg: Builder) -> Result<Builder, Error> {
        Ok(if options.dns {
            cfg.add_container_spec(
                ContainerSpec::from_image("dns", "defreitas/dns-proxy-server")
                    .with_bind("/var/run/docker.sock", "/var/run/docker.sock")
                    .with_bind("/etc/resolv.conf", "/etc/resolv.conf"),
            )
        } else {
            cfg
        })
    }
    async fn start(&self, options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
        if options.dns {
            cfg.start("dns").await?;
        }
        Ok(())
    }
    async fn wait_on(&self, _options: &StartOptions, _cfg: &ComposeTest) -> Result<(), Error> {
        Ok(())
    }
}
