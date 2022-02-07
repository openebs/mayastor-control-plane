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
            let binary = Binary::from_dbg("rest")
                .with_nats("-n")
                .with_arg("--dummy-certificates")
                .with_args(vec!["--https", "rest:8080"])
                .with_args(vec!["--http", "rest:8081"]);

            let binary = if let Some(jwk) = &options.rest_jwk {
                binary.with_arg("--jwk").with_arg(jwk)
            } else {
                binary.with_arg("--no-auth")
            };

            let mut binary = if let Some(timeout) = &options.request_timeout {
                binary
                    .with_arg("--request-timeout")
                    .with_arg(&timeout.to_string())
            } else {
                binary
            };
            if options.no_min_timeouts {
                binary = binary.with_arg("--no-min-timeouts");
            }

            if let Some(env) = &options.rest_env {
                for kv in env {
                    binary = binary.with_env(kv.key.as_str(), kv.value.as_str().as_ref());
                }
            }

            if cfg.container_exists("jaeger") {
                let jaeger_config = format!("jaeger.{}:6831", cfg.get_name());
                binary = binary.with_args(vec!["--jaeger", &jaeger_config])
            };

            cfg.add_container_spec(
                ContainerSpec::from_binary("rest", binary)
                    .with_portmap("8080", "8080")
                    .with_portmap("8081", "8081"),
            )
        })
    }
    async fn start(&self, options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
        if !options.no_rest {
            cfg.start("rest").await?;
        }
        Ok(())
    }
    async fn wait_on(&self, options: &StartOptions, _cfg: &ComposeTest) -> Result<(), Error> {
        if options.no_rest {
            return Ok(());
        }
        Components::wait_url("http://localhost:8081/v0/api/spec").await
    }
}
