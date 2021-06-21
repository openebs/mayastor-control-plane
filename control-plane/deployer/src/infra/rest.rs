use super::*;

#[async_trait]
impl ComponentAction for Rest {
    fn configure(&self, options: &StartOptions, cfg: Builder) -> Result<Builder, Error> {
        Ok(if options.no_rest {
            cfg
        } else {
            if options.build {
                std::process::Command::new("cargo")
                    .args(&["build", "-p", "rest", "--bin", "rest"])
                    .status()?;
            }
            if !options.jaeger {
                cfg.add_container_spec(
                    ContainerSpec::from_binary(
                        "rest",
                        Binary::from_dbg("rest")
                            .with_nats("-n")
                            .with_arg("--dummy-certificates")
                            .with_arg("--no-auth")
                            .with_args(vec!["--https", "rest:8080"])
                            .with_args(vec!["--http", "rest:8081"]),
                    )
                    .with_portmap("8080", "8080")
                    .with_portmap("8081", "8081"),
                )
            } else {
                let jaeger_config = format!("jaeger.{}:6831", cfg.get_name());
                cfg.add_container_spec(
                    ContainerSpec::from_binary(
                        "rest",
                        Binary::from_dbg("rest")
                            .with_nats("-n")
                            .with_arg("--dummy-certificates")
                            .with_arg("--no-auth")
                            .with_args(vec!["-j", &jaeger_config])
                            .with_args(vec!["--https", "rest:8080"])
                            .with_args(vec!["--http", "rest:8081"]),
                    )
                    .with_portmap("8080", "8080")
                    .with_portmap("8081", "8081"),
                )
            }
        })
    }
    async fn start(&self, options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
        if !options.no_rest {
            cfg.start("rest").await?;
        }
        Ok(())
    }
    async fn wait_on(&self, options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
        if options.no_rest {
            return Ok(());
        }
        // wait till the container is running
        loop {
            if let Some(rest) = cfg.get_cluster_container("rest").await? {
                if rest.state == Some("running".to_string()) {
                    break;
                }
            }
            tokio::time::delay_for(std::time::Duration::from_millis(200)).await;
        }

        let max_tries = 20;
        for i in 0 .. max_tries {
            let request = reqwest::Client::new()
                .get("http://localhost:8081/v0/api/spec")
                .timeout(std::time::Duration::from_secs(1))
                .send();
            match request.await {
                Ok(_) => return Ok(()),
                Err(error) => {
                    if i == max_tries - 1 {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::AddrNotAvailable,
                            error.to_string(),
                        )
                        .into());
                    }
                    tokio::time::delay_for(std::time::Duration::from_millis(100)).await;
                }
            }
        }
        Ok(())
    }
}
