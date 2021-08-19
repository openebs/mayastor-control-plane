use super::*;

#[async_trait]
impl ComponentAction for Jaeger {
    fn configure(&self, options: &StartOptions, cfg: Builder) -> Result<Builder, Error> {
        Ok(if !options.jaeger {
            cfg
        } else {
            let mut image = ContainerSpec::from_image("jaeger", "jaegertracing/all-in-one:latest")
                .with_portmap("16686", "16686")
                .with_portmap("6831/udp", "6831/udp")
                .with_portmap("6832/udp", "6832/udp");

            if cfg.container_exists("elastic") {
                image = image
                    .with_env("SPAN_STORAGE_TYPE", "elasticsearch")
                    .with_env("ES_SERVER_URLS", "http://elasticsearch:9200")
                    .with_env("ES_TAGS_AS_FIELDS_ALL", "true")
                    .with_env("ES_HOST", "elasticsearch")
                    .with_env("ES_PORT", "9200")
            }
            if cfg.container_exists("elastic") && options.wait_timeout.is_none() {
                image = image
                    // use our entrypoint which doesn't crash when elasticsearch is not ready...
                    // instead, wait until $ES_HOST:$ES_PORT is open
                    // the original entrypoint will be automagically exec'd into
                    .with_entrypoints(vec!["sh", "./deployer/misc/jaeger_entrypoint_elastic.sh"])
            }

            cfg.add_container_spec(image)
        })
    }
    async fn start(&self, options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
        if options.jaeger {
            cfg.start("jaeger").await?;
        }
        Ok(())
    }
}
