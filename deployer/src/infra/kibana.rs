use crate::infra::{
    async_trait, Builder, ComponentAction, Components, ComposeTest, Error, Kibana, StartOptions,
};
use composer::ContainerSpec;

#[async_trait]
impl ComponentAction for Kibana {
    fn configure(&self, options: &StartOptions, cfg: Builder) -> Result<Builder, Error> {
        Ok(if !options.kibana {
            cfg
        } else {
            let mut spec =
                ContainerSpec::from_image("kibana", "docker.elastic.co/kibana/kibana:7.14.0")
                    .with_portmap("5601", "5601");

            if cfg.container_exists("elastic") {
                spec = spec.with_args(vec!["--elasticsearch", "http://elasticsearch:9200"]);
            }
            cfg.add_container_spec(spec)
        })
    }

    async fn start(&self, options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
        if options.kibana {
            cfg.start("kibana").await?;
        }
        Ok(())
    }

    async fn restart(&self, options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
        if options.kibana {
            cfg.restart("kibana").await?;
        }
        Ok(())
    }

    async fn wait_on(&self, options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
        if options.kibana && (options.jaeger || cfg.container_exists("jaeger").await) {
            loop {
                let form = reqwest::multipart::Form::new().percent_encode_noop().part(
                    "file",
                    reqwest::multipart::Part::text(include_str!("../../misc/kibana_jaeger.ndjson"))
                        .file_name("kibana_jaeger.ndjson"),
                );

                let request = reqwest::Client::new()
                    .post("http://localhost:5601/api/saved_objects/_import")
                    .query(&[("overwrite", "true")])
                    .header("kbn-xsrf", "true")
                    .multipart(form)
                    .timeout(std::time::Duration::from_millis(500));

                match request.send().await {
                    Ok(resp) if resp.status().is_success() => {
                        break;
                    }
                    _ => {}
                }

                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
        } else if options.kibana {
            Components::wait_url("http://localhost:5601/api/status").await?;
        }
        Ok(())
    }
}
