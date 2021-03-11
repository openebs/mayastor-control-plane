use super::*;
use store::{etcd::Etcd as EtcdStore, kv_store::Store};

#[async_trait]
impl ComponentAction for Etcd {
    fn configure(
        &self,
        options: &StartOptions,
        cfg: Builder,
    ) -> Result<Builder, Error> {
        Ok(if options.etcd {
            cfg.add_container_spec(
                ContainerSpec::from_binary(
                    "etcd",
                    Binary::from_nix("etcd").with_args(vec![
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
    async fn start(
        &self,
        options: &StartOptions,
        cfg: &ComposeTest,
    ) -> Result<(), Error> {
        if options.etcd {
            cfg.start("etcd").await?;
        }
        Ok(())
    }
    async fn wait_on(
        &self,
        options: &StartOptions,
        _cfg: &ComposeTest,
    ) -> Result<(), Error> {
        if options.etcd {
            let mut store = EtcdStore::new("0.0.0.0:2379")
                .await
                .expect("Failed to connect to etcd.");
            let key = serde_json::json!("wait");
            let value = serde_json::json!("test");
            store
                .put(&key, &value)
                .await
                .expect("Failed to 'put' to etcd");
            store.delete(&key).await.unwrap();
        }
        Ok(())
    }
}
