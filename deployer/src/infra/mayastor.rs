use super::*;
use rpc::mayastor::RpcHandle;
use std::net::{IpAddr, SocketAddr};
use utils::DEFAULT_GRPC_CLIENT_ADDR;

#[async_trait]
impl ComponentAction for Mayastor {
    fn configure(&self, options: &StartOptions, cfg: Builder) -> Result<Builder, Error> {
        let mut cfg = cfg;
        for i in 0 .. options.mayastors {
            let mayastor_socket =
                format!("{}:10124", cfg.next_ip_for_name(&Self::name(i, options))?);
            let name = Self::name(i, options);
            let bin = utils::DATA_PLANE_BINARY;
            let binary = options.mayastor_bin.clone().or_else(|| Self::binary(bin));

            let mut spec = if let Some(binary) = binary {
                ContainerSpec::from_binary(&name, Binary::from_path(&binary))
                    .with_bind_binary_dir(true)
            } else {
                ContainerSpec::from_image(&name, &options.mayastor_image)
            }
            .with_args(vec!["-N", &name])
            .with_args(vec!["-g", &mayastor_socket])
            .with_args(vec!["-R", DEFAULT_GRPC_CLIENT_ADDR])
            .with_bind("/tmp", "/host/tmp");

            if let Some(env) = &options.mayastor_env {
                for kv in env {
                    spec = spec.with_env(kv.key.as_str(), kv.value.as_str().as_ref());
                }
            }

            if !options.mayastor_devices.is_empty() {
                spec = spec.with_privileged(Some(true));
                for device in options.mayastor_devices.iter() {
                    spec = spec.with_bind(device, device);
                }
            }

            if options.developer_delayed {
                spec = spec.with_env("DEVELOPER_DELAYED", "1");
            }

            if !options.no_etcd {
                let etcd = format!("etcd.{}:2379", options.cluster_label.name());
                spec = spec.with_args(vec!["-p", &etcd]);
            }
            cfg = cfg.add_container_spec(spec)
        }
        Ok(cfg)
    }
    async fn start(&self, options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
        let mayastors = (0 .. options.mayastors)
            .into_iter()
            .map(|i| async move { cfg.start(&Self::name(i, options)).await });
        futures::future::try_join_all(mayastors).await?;
        Ok(())
    }
    async fn wait_on(&self, options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
        for i in 0 .. options.mayastors {
            let name = Self::name(i, options);
            let container_ip = cfg.container_ip_as_ref(&name);
            let socket = SocketAddr::new(IpAddr::from(*container_ip), 10124);
            let mut hdl = RpcHandle::connect(&name, socket).await?;
            hdl.mayastor
                .list_nexus(rpc::mayastor::Null {})
                .await
                .unwrap();
        }
        Ok(())
    }
}

impl Mayastor {
    pub fn name(i: u32, _options: &StartOptions) -> String {
        format!("mayastor-{}", i + 1)
    }
    fn binary(path: &str) -> Option<String> {
        match std::env::var_os(&path) {
            None => None,
            Some(val) if val.is_empty() => None,
            Some(val) => Some(val.to_string_lossy().to_string()),
        }
    }
}
