use super::*;

#[async_trait]
impl ComponentAction for Core {
    fn configure(&self, options: &StartOptions, cfg: Builder) -> Result<Builder, Error> {
        let name = "core";
        if options.build {
            let status = std::process::Command::new("cargo")
                .args(&["build", "-p", "agents", "--bin", name])
                .status()?;
            build_error(&format!("the {} agent", name), status.code())?;
        }
        let mut binary = Binary::from_dbg(name);

        if let Some(env) = &options.agents_env {
            for kv in env {
                binary = binary.with_env(kv.key.as_str(), kv.value.as_str().as_ref());
            }
        }

        let etcd = format!("etcd.{}:2379", options.cluster_label.name());
        binary = binary.with_args(vec!["--store", &etcd]);
        if let Some(cache_period) = &options.cache_period {
            binary = binary.with_args(vec!["-c", &cache_period.to_string()]);
        }
        if let Some(deadline) = &options.node_deadline {
            binary = binary.with_args(vec!["-d", &deadline.to_string()]);
        }
        if let Some(timeout) = &options.node_conn_timeout {
            binary = binary.with_args(vec!["--connect-timeout", &timeout.to_string()]);
        }
        if let Some(timeout) = &options.request_timeout {
            binary = binary.with_args(vec!["--request-timeout", &timeout.to_string()]);
        }
        if options.no_min_timeouts {
            binary = binary.with_arg("--no-min-timeouts");
        }
        if let Some(timeout) = &options.store_timeout {
            binary = binary.with_args(vec!["--store-timeout", &timeout.to_string()]);
        }
        if let Some(ttl) = &options.store_lease_ttl {
            binary = binary.with_args(vec!["--store-lease-ttl", &ttl.to_string()]);
        }
        if let Some(period) = &options.reconcile_period {
            binary = binary.with_args(vec!["--reconcile-period", &period.to_string()]);
        }
        if let Some(period) = &options.reconcile_idle_period {
            binary = binary.with_args(vec!["--reconcile-idle-period", &period.to_string()]);
        }
        if cfg.container_exists("jaeger") {
            let jaeger_config = format!("jaeger.{}:6831", cfg.get_name());
            binary = binary.with_args(vec!["--jaeger", &jaeger_config]);
        }
        if let Some(size) = &options.otel_max_batch_size {
            binary = binary.with_env("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", size);
        }
        if let Some(max_rebuilds) = &options.max_rebuilds {
            binary = binary.with_args(vec!["--max-rebuilds", &max_rebuilds.to_string()]);
        }
        Ok(cfg.add_container_bin(name, binary))
    }
    async fn start(&self, _options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
        cfg.start("core").await?;
        Ok(())
    }
    async fn wait_on(&self, _options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
        let ip = cfg.container_ip("core");
        let uri = tonic::transport::Uri::from_str(&format!("https://{}:50051", ip)).unwrap();
        let timeout = grpc::context::TimeoutOptions::new()
            .with_req_timeout(std::time::Duration::from_millis(100));
        let core =
            grpc::client::CoreClient::new(uri, Some(timeout.with_max_retries(Some(10)))).await;
        core.wait_ready(None).await.map_err(|_| {
            let error = "Failed to wait for core to get ready";
            std::io::Error::new(std::io::ErrorKind::TimedOut, error)
        })?;

        Ok(())
    }
}
