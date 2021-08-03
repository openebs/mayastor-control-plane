use super::*;
use common_lib::{store::etcd::Etcd as EtcdStore, types::v0::store::definitions::Store};

#[async_trait]
impl ComponentAction for Etcd {
    fn configure(&self, options: &StartOptions, cfg: Builder) -> Result<Builder, Error> {
        Ok(if !options.no_etcd {
            cfg.add_container_spec(
                ContainerSpec::from_binary(
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
                .with_portmap("2380", "2380"),
            )
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
            let mut store = EtcdStore::new("0.0.0.0:2379")
                .await
                .expect("Failed to connect to etcd.");
            assert!(store.online().await);
        }
        Ok(())
    }
}
