use super::*;

#[async_trait]
impl ComponentAction for Jaeger {
    fn configure(&self, options: &StartOptions, cfg: Builder) -> Result<Builder, Error> {
        Ok(if !options.jaeger {
            cfg
        } else {
            let mut tags = crate::KeyValues::new(options.tracing_tags.clone());
            if let Ok(run) = std::env::var("BUILD_TAG") {
                tags.add(crate::KeyValue::new("run", run.replacen("jenkins-", "", 1)));
            }
            if let Ok(stage) = std::env::var("STAGE_NAME") {
                tags.add(crate::KeyValue::new("run.stage", stage));
            }
            let own_collector = format!("jaeger.{}", cfg.get_name());
            let mut image = match &options.external_jaeger {
                Some(collector) if collector.starts_with(&own_collector) => {
                    // local debug trick, use collector on the same jaeger container
                    let mut image =
                        ContainerSpec::from_image("jaeger", "jaegertracing/all-in-one:latest")
                            .with_portmap("16686", "16686")
                            .with_portmap("6831/udp", "6831/udp")
                            .with_portmap("6832/udp", "6832/udp");
                    if let Some(args) = tags.into_args() {
                        image = image.with_arg(&format!("--collector.tags={}", args));
                    }
                    if collector.contains(':') {
                        image.with_args(vec!["--reporter.grpc.host-port", collector])
                    } else {
                        image.with_arg(&format!("--reporter.grpc.host-port={}:14250", collector))
                    }
                }
                Some(collector) if !collector.is_empty() => {
                    // add a local jaeger agent which will export to the external jaeger collector
                    let mut image =
                        ContainerSpec::from_image("jaeger", "jaegertracing/jaeger-agent:latest")
                            .with_portmap("6831/udp", "6831/udp")
                            .with_portmap("6832/udp", "6832/udp");
                    if let Some(args) = tags.into_args() {
                        image = image.with_arg(&format!("--agent.tags={}", args));
                    }
                    if collector.contains(':') {
                        image.with_args(vec!["--reporter.grpc.host-port", collector])
                    } else {
                        image.with_arg(&format!("--reporter.grpc.host-port={}:14250", collector))
                    }
                }
                _ => {
                    // the all-in-one container which contains all components in a single container
                    let image =
                        ContainerSpec::from_image("jaeger", "jaegertracing/all-in-one:latest")
                            .with_portmap("16686", "16686")
                            .with_portmap("6831/udp", "6831/udp")
                            .with_portmap("6832/udp", "6832/udp");
                    if let Some(args) = tags.into_args() {
                        image.with_arg(&format!("--collector.tags={}", args))
                    } else {
                        image
                    }
                }
            };

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
