use super::*;
use common_lib::store::etcd::Etcd as EtcdStore;

#[async_trait]
impl ComponentAction for Etcd {
    fn configure(&self, options: &StartOptions, cfg: Builder) -> Result<Builder, Error> {
        Ok(if !options.no_etcd {
            let container_spec = ContainerSpec::from_binary(
                "etcd",
                Binary::from_path("etcd").with_args(vec![
                    "--data-dir",
                    "/tmp/etcd-data",
                    "--advertise-client-urls",
                    "http://0.0.0.0:2379",
                    "--listen-client-urls",
                    "http://0.0.0.0:2379",
                ]),
            )
            .with_portmap("2379", "2379")
            .with_portmap("2380", "2380");

            #[cfg(target_arch = "aarch64")]
            let container_spec = container_spec.with_env("ETCD_UNSUPPORTED_ARCH", "arm64");
            cfg.add_container_spec(container_spec)
        } else {
            cfg
        })
    }
    async fn start(&self, options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
        if !options.no_etcd {
            cfg.start("etcd").await?;
        }
        Ok(())
    }
    async fn wait_on(&self, options: &StartOptions, _cfg: &ComposeTest) -> Result<(), Error> {
        if !options.no_etcd {
            let _store = EtcdStore::new("0.0.0.0:2379")
                .await
                .expect("Failed to connect to etcd.");
        }
        Ok(())
    }
}
