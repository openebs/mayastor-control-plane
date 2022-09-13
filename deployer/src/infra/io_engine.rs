use crate::infra::{
    async_trait, Builder, ComponentAction, ComposeTest, Error, IoEngine, StartOptions,
};
use composer::{Binary, ContainerSpec};
use rpc::io_engine::{IoEngineApiVersion, RpcHandle};
use std::net::{IpAddr, SocketAddr};
use utils::DEFAULT_GRPC_CLIENT_ADDR;

#[async_trait]
impl ComponentAction for IoEngine {
    fn configure(&self, options: &StartOptions, cfg: Builder) -> Result<Builder, Error> {
        let mut cfg = cfg;
        for i in 0 .. options.io_engines {
            let io_engine_socket =
                format!("{}:10124", cfg.next_ip_for_name(&Self::name(i, options))?);
            let name = Self::name(i, options);
            let bin = utils::DATA_PLANE_BINARY;
            let binary = options.io_engine_bin.clone().or_else(|| Self::binary(bin));

            let mut spec = if let Some(binary) = binary {
                ContainerSpec::from_binary(&name, Binary::from_path(&binary))
                    .with_bind_binary_dir(true)
            } else {
                ContainerSpec::from_image(&name, &options.io_engine_image)
                    .with_pull_policy(options.image_pull_policy.clone())
            }
            .with_args(vec!["-N", &name])
            .with_args(vec!["-g", &io_engine_socket])
            .with_args(vec!["-R", DEFAULT_GRPC_CLIENT_ADDR])
            .with_args(vec![
                "--api-versions".to_string(),
                IoEngineApiVersion::vec_to_str(options.io_engine_api_versions.clone()),
            ])
            .with_args(vec![
                "-r",
                format!("/host/tmp/{}.sock", Self::name(i, options)).as_str(),
            ])
            .with_bind("/tmp", "/host/tmp");

            if options.io_engine_isolate {
                spec = spec.with_args(vec!["-l", format!("{}", i).as_str()]);
            }

            if let Some(env) = &options.io_engine_env {
                for kv in env {
                    spec = spec.with_env(kv.key.as_str(), kv.value.as_str().as_ref());
                }
            }

            if !options.io_engine_devices.is_empty() {
                spec = spec.with_privileged(Some(true));
                for device in options.io_engine_devices.iter() {
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
        let io_engines = (0 .. options.io_engines)
            .into_iter()
            .map(|i| async move { cfg.start(&Self::name(i, options)).await });
        futures::future::try_join_all(io_engines).await?;
        Ok(())
    }
    async fn wait_on(&self, options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
        for i in 0 .. options.io_engines {
            let name = Self::name(i, options);
            let container_ip = cfg.container_ip_as_ref(&name);
            let socket = SocketAddr::new(IpAddr::from(*container_ip), 10124);
            let mut hdl =
                RpcHandle::connect(options.latest_io_api_version(), &name, socket).await?;
            hdl.ping().await.unwrap();
        }
        Ok(())
    }
}

impl IoEngine {
    pub fn name(i: u32, _options: &StartOptions) -> String {
        format!("io-engine-{}", i + 1)
    }
    fn binary(path: &str) -> Option<String> {
        match std::env::var_os(&path) {
            None => None,
            Some(val) if val.is_empty() => None,
            Some(val) => Some(val.to_string_lossy().to_string()),
        }
    }
}
