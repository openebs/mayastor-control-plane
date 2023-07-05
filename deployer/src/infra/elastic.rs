use crate::infra::{
    async_trait, Builder, ComponentAction, Components, ComposeTest, Elastic, Error, StartOptions,
};
use composer::ContainerSpec;

#[async_trait]
impl ComponentAction for Elastic {
    fn configure(&self, options: &StartOptions, cfg: Builder) -> Result<Builder, Error> {
        Ok(if !options.elastic {
            cfg
        } else {
            cfg.add_container_spec(
                ContainerSpec::from_image(
                    "elastic",
                    "docker.elastic.co/elasticsearch/elasticsearch:7.14.0",
                )
                .with_alias("elasticsearch")
                .with_env("discovery.type", "single-node")
                .with_env("xpack.security.enabled", "false")
                .with_env("ES_JAVA_OPTS", "-Xms2g -Xmx2g")
                .with_portmap("9200", "9200")
                .with_portmap("9300", "9300"),
            )
        })
    }

    async fn start(&self, options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
        if options.elastic {
            cfg.start("elastic").await?;
        }
        Ok(())
    }

    async fn restart(&self, options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
        if options.elastic {
            cfg.restart("elastic").await?;
        }
        Ok(())
    }

    async fn wait_on(&self, options: &StartOptions, _cfg: &ComposeTest) -> Result<(), Error> {
        if options.elastic {
            Components::wait_url("http://localhost:9200").await?;
        }
        Ok(())
    }
}
