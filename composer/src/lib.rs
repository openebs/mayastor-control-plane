use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddr, TcpStream},
    path::Path,
    thread,
    time::Duration,
};

use bollard::{
    container::{
        Config, CreateContainerOptions, ListContainersOptions, LogsOptions, NetworkingConfig,
        RemoveContainerOptions, RestartContainerOptions, StopContainerOptions,
    },
    errors::Error,
    network::{CreateNetworkOptions, ListNetworksOptions},
    service::{
        ContainerSummaryInner, EndpointIpamConfig, EndpointSettings, HostConfig, Ipam, Mount,
        MountTypeEnum, Network, PortMap,
    },
    Docker,
};
use futures::{StreamExt, TryStreamExt};
use ipnetwork::Ipv4Network;
use tonic::transport::Channel;

use bollard::{
    container::KillContainerOptions, image::CreateImageOptions, models::ContainerInspectResponse,
    network::DisconnectNetworkOptions,
};
use rpc::mayastor::{bdev_rpc_client::BdevRpcClient, mayastor_client::MayastorClient};

pub const TEST_NET_NAME: &str = "mayastor-testing-network";
pub const TEST_LABEL_PREFIX: &str = "io.mayastor.test";
pub const TEST_NET_NETWORK: &str = "10.1.0.0/16";
#[derive(Clone)]
pub struct RpcHandle {
    pub name: String,
    pub endpoint: SocketAddr,
    pub mayastor: MayastorClient<Channel>,
    pub bdev: BdevRpcClient<Channel>,
}

impl RpcHandle {
    /// connect to the containers and construct a handle
    async fn connect(name: String, endpoint: SocketAddr) -> Result<Self, String> {
        let mut attempts = 40;
        loop {
            if TcpStream::connect_timeout(&endpoint, Duration::from_millis(100)).is_ok() {
                break;
            } else {
                thread::sleep(Duration::from_millis(101));
            }
            attempts -= 1;
            if attempts == 0 {
                return Err(format!("Failed to connect to {}/{}", name, endpoint));
            }
        }

        let mayastor = MayastorClient::connect(format!("http://{}", endpoint))
            .await
            .unwrap();
        let bdev = BdevRpcClient::connect(format!("http://{}", endpoint))
            .await
            .unwrap();

        Ok(Self {
            name,
            mayastor,
            bdev,
            endpoint,
        })
    }
}

/// Path to local binary and arguments
#[derive(Default, Clone)]
pub struct Binary {
    path: String,
    arguments: Vec<String>,
    nats_arg: Option<String>,
    env: HashMap<String, String>,
    binds: HashMap<String, String>,
    privileged: Option<bool>,
}

impl Binary {
    /// Setup local binary from target debug and arguments
    pub fn from_dbg(name: &str) -> Self {
        let path = std::path::PathBuf::from(std::env!("CARGO_MANIFEST_DIR"));
        let srcdir = path.parent().unwrap().to_string_lossy();

        Self::new(&format!("{}/target/debug/{}", srcdir, name), vec![])
    }
    /// Setup binary from path
    pub fn from_path(name: &str) -> Self {
        Self::new(name, vec![])
    }
    /// Add single argument
    /// Only one argument can be passed per use. So instead of:
    ///
    /// # Self::from_dbg("hello")
    /// .with_arg("-n nats")
    /// # ;
    ///
    /// usage would be:
    ///
    /// # Self::from_dbg("hello")
    /// .with_arg("-n")
    /// .with_arg("nats")
    /// # ;
    pub fn with_arg(mut self, arg: &str) -> Self {
        self.arguments.push(arg.into());
        self
    }
    /// Add multiple arguments via a vector
    pub fn with_args<S: Into<String>>(mut self, mut args: Vec<S>) -> Self {
        self.arguments.extend(args.drain(..).map(|s| s.into()));
        self
    }
    /// Set the nats endpoint via the provided argument
    pub fn with_nats(mut self, arg: &str) -> Self {
        self.nats_arg = Some(arg.to_string());
        self
    }
    /// Add environment variables for the container
    pub fn with_env(mut self, key: &str, val: &str) -> Self {
        if let Some(old) = self.env.insert(key.into(), val.into()) {
            println!("Replaced key {} val {} with val {}", key, old, val);
        }
        self
    }
    /// use a volume binds between host path and container container
    pub fn with_bind(mut self, host: &str, container: &str) -> Self {
        self.binds.insert(container.to_string(), host.to_string());
        self
    }
    /// run the container as privileged
    pub fn with_privileged(mut self, enable: Option<bool>) -> Self {
        self.privileged = enable;
        self
    }
    /// pick up the nats argument name for a particular binary from nats_arg
    /// and fill up the nats server endpoint using the network name
    fn setup_nats(&mut self, network: &str) {
        if let Some(nats_arg) = self.nats_arg.take() {
            if !nats_arg.is_empty() {
                self.arguments.push(nats_arg);
                self.arguments.push(format!("nats.{}:4222", network));
            }
        }
    }

    fn command(&self) -> String {
        self.path.clone()
    }
    fn commands(&self) -> Vec<String> {
        let mut v = vec![self.path.clone()];
        v.extend(self.arguments.clone());
        v
    }
    /// Run the `which` command to get location of the named binary
    pub fn which(name: &str) -> std::io::Result<String> {
        let output = std::process::Command::new("which").arg(name).output()?;
        if !output.status.success() {
            return Err(std::io::Error::new(std::io::ErrorKind::NotFound, name));
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().into())
    }
    fn new(path: &str, args: Vec<String>) -> Self {
        Self {
            path: Self::which(path).expect("Binary path should exist!"),
            arguments: args,
            ..Default::default()
        }
    }
}

const RUST_LOG_DEFAULT: &str =
    "debug,actix_web=debug,actix=debug,h2=info,hyper=info,tower_buffer=info,tower=info,bollard=info,rustls=info,reqwest=info,composer=info";

/// Specs of the allowed containers include only the binary path
/// (relative to src) and the required arguments
#[derive(Default, Clone)]
pub struct ContainerSpec {
    /// Name of the container
    name: ContainerName,
    /// Base image of the container
    image: Option<String>,
    /// Command to run
    command: Option<String>,
    /// Entrypoint
    entrypoint: Option<Vec<String>>,
    /// command arguments to run
    arguments: Option<Vec<String>>,
    /// local binary
    binary: Option<Binary>,
    /// Port mapping to host ports
    port_map: Option<PortMap>,
    /// Bypass the container image's port mapping
    bypass_image_port_map: bool,
    /// Network alias names
    network_aliases: Option<Vec<String>>,
    /// Use Init container
    init: Option<bool>,
    /// Key-Map of environment variables
    /// Starts with RUST_LOG=debug,h2=info
    env: HashMap<String, String>,
    /// Volume bind dst/source
    binds: HashMap<String, String>,
    /// run the container as privileged
    privileged: Option<bool>,
    /// use default container mounts (like /tmp and /var/tmp)
    bypass_default_mounts: bool,
    /// Bind binary directory instead of binary itself.
    bind_binary_dir: bool,
}

impl ContainerSpec {
    /// Create new ContainerSpec from name and binary
    pub fn from_binary(name: &str, binary: Binary) -> Self {
        let mut env = binary.env.clone();
        if !env.contains_key("RUST_LOG") {
            env.insert("RUST_LOG".to_string(), RUST_LOG_DEFAULT.to_string());
        }
        Self {
            name: name.into(),
            binary: Some(binary),
            init: Some(true),
            env,
            ..Default::default()
        }
    }
    /// Create new ContainerSpec from name and image
    pub fn from_image(name: &str, image: &str) -> Self {
        let mut env = HashMap::new();
        env.insert("RUST_LOG".to_string(), RUST_LOG_DEFAULT.to_string());
        Self {
            name: name.into(),
            init: Some(true),
            image: Some(image.into()),
            env,
            ..Default::default()
        }
    }
    /// Add port mapping from container to host
    pub fn with_portmap(mut self, from: &str, to: &str) -> Self {
        let from = if from.contains('/') {
            from.to_string()
        } else {
            format!("{}/tcp", from)
        };
        let binding = bollard::service::PortBinding {
            host_ip: None,
            host_port: Some(to.into()),
        };
        if let Some(pm) = &mut self.port_map {
            pm.insert(from, Some(vec![binding]));
        } else {
            let mut port_map = bollard::service::PortMap::new();
            port_map.insert(from, Some(vec![binding]));
            self.port_map = Some(port_map);
        }
        self
    }
    /// Bypass the container image's port map
    pub fn with_bypass_port_map(mut self, bypass: bool) -> Self {
        self.bypass_image_port_map = bypass;
        self
    }
    /// Add an alias to the container
    pub fn with_alias(mut self, alias: &str) -> Self {
        match self.network_aliases.as_mut() {
            None => self.network_aliases = Some(vec![alias.into()]),
            Some(aliases) => {
                aliases.push(alias.into());
            }
        };
        self
    }
    /// use or not predefined mounts for the container.
    pub fn with_bypass_default_mounts(mut self, bypass: bool) -> Self {
        self.bypass_default_mounts = bypass;
        self
    }
    /// Add environment key-val, eg for setting the RUST_LOG
    /// If a key already exists, the value is replaced
    pub fn with_env(mut self, key: &str, val: &str) -> Self {
        if let Some(old) = self.env.insert(key.into(), val.into()) {
            println!("Replaced key {} val {} with val {}", key, old, val);
        }
        self
    }
    /// Add single argument
    /// Only one argument can be passed per use. So instead of:
    ///
    /// # Self::from_image("hello", "hello")
    /// .with_arg("-n nats")
    /// # ;
    ///
    /// usage would be:
    ///
    /// # Self::from_image("hello", "hello")
    /// .with_arg("-n")
    /// .with_arg("nats")
    /// # ;
    pub fn with_arg(mut self, arg: &str) -> Self {
        match self.arguments.as_mut() {
            None => self.arguments = Some(vec![arg.into()]),
            Some(args) => {
                args.push(arg.into());
            }
        };
        self
    }
    /// Add multiple arguments via a vector
    pub fn with_args<S: Into<String>>(mut self, args: Vec<S>) -> Self {
        match self.arguments.as_mut() {
            None => self.arguments = Some(args.into_iter().map(Into::into).collect()),
            Some(arguments) => {
                arguments.extend(args.into_iter().map(Into::into));
            }
        };
        self
    }
    /// Use a specific command to execute
    /// Note: this is not the entrypoint!
    pub fn with_cmd(mut self, command: &str) -> Self {
        self.command = Some(command.to_string());
        self
    }
    /// Add a container entrypoint
    pub fn with_entrypoint<S: Into<String>>(mut self, entrypoint: S) -> Self {
        self.entrypoint = Some(vec![entrypoint.into()]);
        self
    }
    /// Add a container entrypoint with arguments
    pub fn with_entrypoints<S: Into<String>>(mut self, entrypoints: Vec<S>) -> Self {
        self.entrypoint = Some(entrypoints.into_iter().map(Into::into).collect());
        self
    }
    /// use a volume binds between host path and container container
    pub fn with_bind(mut self, host: &str, container: &str) -> Self {
        self.binds.insert(container.to_string(), host.to_string());
        self
    }
    /// List of volume binds with each element as host:container
    fn binds(&self) -> Vec<String> {
        let mut vec = vec![];
        self.binds.iter().for_each(|(container, host)| {
            vec.push(format!("{}:{}", host, container));
        });
        if let Some(binary) = &self.binary {
            binary.binds.iter().for_each(|(container, host)| {
                vec.push(format!("{}:{}", host, container));
            });
        }
        vec
    }
    /// run the container as privileged
    pub fn with_privileged(mut self, enable: Option<bool>) -> Self {
        self.privileged = enable;
        self
    }
    /// check if the container is to run as privileged
    pub fn privileged(&self) -> Option<bool> {
        if self.privileged.is_some() {
            self.privileged
        } else if let Some(binary) = &self.binary {
            binary.privileged
        } else {
            None
        }
    }
    /// If host binary is used, enable binding of host binary directory
    /// instead of the path of the binary itself.
    ///
    /// The allows to access files in binary directory within the container.
    /// For example, this is needed when the binary is linked to
    /// a shared library located in the same directory.
    ///
    /// This option is false by default.
    pub fn with_bind_binary_dir(mut self, enable: bool) -> Self {
        self.bind_binary_dir = enable;
        self
    }

    /// Environment variables as a vector with each element as:
    /// "{key}={value}"
    fn environment(&self) -> Vec<String> {
        let mut vec = vec![];
        self.env.iter().for_each(|(k, v)| {
            vec.push(format!("{}={}", k, v));
        });
        vec
    }
    /// Command/entrypoint followed by/and arguments
    fn commands(&self, network: &str) -> Vec<String> {
        let mut commands = vec![];
        if let Some(mut binary) = self.binary.clone() {
            binary.setup_nats(network);
            commands.extend(binary.commands());
        } else if let Some(command) = self.command.clone() {
            commands.push(command);
        }
        commands.extend(self.arguments.clone().unwrap_or_default());
        commands
    }
    /// Container spec's entrypoint
    fn entrypoint(&self, default_image: &Option<String>) -> Vec<String> {
        if let Some(entrypoint) = &self.entrypoint {
            entrypoint.clone()
        } else if self.binary.is_some() && default_image.is_some() && self.init.unwrap_or(true) {
            vec!["tini".to_string(), "--".to_string()]
        } else {
            vec![]
        }
    }
    /// Get the container command, if any
    fn command(&self, network: &str) -> Option<String> {
        if let Some(mut binary) = self.binary.clone() {
            binary.setup_nats(network);
            Some(binary.command())
        } else {
            self.command.clone()
        }
    }
}

#[derive(Clone)]
pub struct Builder {
    /// name of the experiment this name will be used as a network and labels
    /// this way we can "group" all objects within docker to match this test
    /// test. It is highly recommend you use a sane name for this as it will
    /// help you during debugging
    name: String,
    /// containers we want to create, note these are mayastor containers
    /// only
    containers: Vec<(ContainerSpec, Ipv4Addr)>,
    /// existing containers and their (IDs, Ipv4)
    existing_containers: HashMap<ContainerName, (ContainerId, Ipv4Addr)>,
    /// container shutdown order
    shutdown_order: Vec<ContainerName>,
    /// the network used by this experiment
    network: Ipv4Network,
    /// reuse existing containers
    reuse: bool,
    /// prefix for labels set on containers and networks
    ///   $prefix.name = $name will be created automatically
    label_prefix: String,
    /// allow cleaning up on a panic (if clean is true)
    allow_clean_on_panic: bool,
    /// delete the container and network when dropped
    clean: bool,
    /// destroy existing containers on the same network, if any
    prune: bool,
    /// prune matching existing containers
    /// useful when we want to recreate matching containers, and leave everything else alone
    prune_matching: bool,
    /// run all containers on build
    autorun: bool,
    /// base image for image-less containers
    image: Option<String>,
    /// output container logs on panic
    logs_on_panic: bool,
}

impl Default for Builder {
    fn default() -> Self {
        Builder::new()
    }
}

/// trait to allow extensibility using the Builder pattern
pub trait BuilderConfigure {
    fn configure(&self, cfg: Builder) -> Result<Builder, Box<dyn std::error::Error>>;
}

impl Builder {
    /// construct a new builder for `[ComposeTest']
    pub fn new() -> Self {
        Self {
            name: TEST_NET_NAME.to_string(),
            containers: Default::default(),
            existing_containers: Default::default(),
            shutdown_order: vec![],
            network: TEST_NET_NETWORK.parse().expect("Valid network config"),
            reuse: false,
            label_prefix: TEST_LABEL_PREFIX.to_string(),
            allow_clean_on_panic: true,
            clean: true,
            prune: true,
            prune_matching: false,
            autorun: true,
            image: None,
            logs_on_panic: true,
        }
    }

    /// get the name of the experiment
    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    /// load information existing containers by querying the docker network
    pub async fn load_existing_containers(mut self) -> Self {
        tracing::trace!("Loading Existing containers");
        let composer = self.clone().build_only().await.unwrap();
        let containers = composer.list_network_containers(&self.name).await.unwrap();
        for container in containers {
            let networks = container
                .network_settings
                .unwrap_or_default()
                .networks
                .unwrap_or_default();
            if let Some(n) = container.names.unwrap_or_default().first() {
                if let Some(endpoint) = networks.get(&self.name) {
                    if let Some(ip) = endpoint.ip_address.clone() {
                        if let Ok(ip) = ip.parse() {
                            tracing::debug!("Reloading existing container: {}", n);
                            self.existing_containers
                                .insert(n[1 ..].into(), (container.id.unwrap_or_default(), ip));
                        }
                    }
                }
            }
        }
        tracing::trace!("Loaded Existing containers");
        self
    }

    /// configure the `Builder` using the `BuilderConfigure` trait
    pub fn configure(
        self,
        cfg: impl BuilderConfigure,
    ) -> Result<Builder, Box<dyn std::error::Error>> {
        cfg.configure(self)
    }

    /// finds the next unused ip
    pub fn next_ip(&self) -> Result<Ipv4Addr, Error> {
        for ip in 2 ..= 255u32 {
            if let Some(ip) = self.network.nth(ip) {
                if self.existing_containers.values().all(|(_, e)| e != &ip)
                    && self.containers.iter().all(|(_, e)| e != &ip)
                {
                    return Ok(ip);
                }
            }
        }
        Err(bollard::errors::Error::IOError {
            err: std::io::Error::new(std::io::ErrorKind::AddrNotAvailable, "No available ip"),
        })
    }

    /// next ordinal container ip for the given container name
    /// (useful in case we're recreating an existing container and "inheriting" its ip)
    pub fn next_ip_for_name(&self, name: &str) -> Result<Ipv4Addr, Error> {
        if let Some(container) = self.existing_containers.get(name) {
            Ok(container.1)
        } else {
            self.next_ip()
        }
    }

    /// run all containers on build
    pub fn autorun(mut self, run: bool) -> Builder {
        self.autorun = run;
        self
    }

    /// set the network for this test
    pub fn network(mut self, network: &str) -> Result<Builder, Error> {
        self.network = network
            .parse()
            .map_err(|error| bollard::errors::Error::IOError {
                err: std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Invalid network format: {}", error),
                ),
            })?;
        Ok(self)
    }

    /// the name to be used as labels and network name
    pub fn name(mut self, name: &str) -> Builder {
        self.name = name.to_owned();
        self
    }

    /// set different label prefix for the network label
    pub fn label_prefix(mut self, prefix: &str) -> Builder {
        self.label_prefix = prefix.to_owned();
        self
    }

    /// add a mayastor container with a name
    pub fn add_container(self, name: &str) -> Builder {
        self.add_container_spec(ContainerSpec::from_binary(
            name,
            Binary::from_path("mayastor"),
        ))
    }

    /// check if a container exists
    pub fn container_exists(&self, name: &str) -> bool {
        self.containers.iter().any(|(c, _)| c.name == name)
            || self.existing_containers.get(name).is_some()
    }

    /// add a generic container which runs a local binary
    pub fn add_container_spec(mut self, spec: ContainerSpec) -> Builder {
        if let Some(container) = self.existing_containers.get(&spec.name) {
            tracing::debug!("Reusing container: {}", spec.name);
            let next_ip = container.1;
            self.existing_containers.remove(&spec.name);
            self.containers.push((spec, next_ip));
        } else {
            let next_ip = self.next_ip().unwrap();
            tracing::debug!("Adding container: {}, ip: {}", spec.name, next_ip);
            self.containers.push((spec, next_ip));
        }
        self
    }

    /// add a generic container which runs a local binary
    pub fn add_container_bin(self, name: &str, bin: Binary) -> Builder {
        self.add_container_spec(ContainerSpec::from_binary(name, bin))
    }

    /// add a docker container which will be pulled if not present
    pub fn add_container_image(self, name: &str, image: Binary) -> Builder {
        self.add_container_spec(ContainerSpec::from_binary(name, image))
    }

    /// attempt to reuse and restart containers instead of starting new ones
    pub fn with_reuse(mut self, reuse: bool) -> Builder {
        self.reuse = reuse;
        self.prune = !reuse;
        self
    }

    /// set all the prune and reuse flags
    pub fn with_prune_reuse(mut self, prune: bool, prune_matching: bool, reuse: bool) -> Builder {
        self.reuse = reuse;
        self.prune = prune;
        self.prune_matching = prune_matching;
        self
    }

    /// clean on drop?
    pub fn with_clean(mut self, enable: bool) -> Builder {
        self.clean = enable;
        self
    }

    /// allow clean on panic if clean is set
    pub fn with_clean_on_panic(mut self, enable: bool) -> Builder {
        self.allow_clean_on_panic = enable;
        self
    }

    /// prune containers and networks on start
    pub fn with_prune(mut self, enable: bool) -> Builder {
        self.prune = enable;
        self.prune_matching = enable;
        self
    }

    /// output logs on panic
    pub fn with_logs(mut self, enable: bool) -> Builder {
        self.logs_on_panic = enable;
        self
    }

    /// use base image for all binary containers
    pub fn with_base_image<S: Into<Option<String>>>(mut self, image: S) -> Builder {
        self.image = image.into();
        self
    }

    /// setup tracing for the cargo test code with `RUST_LOG` const
    pub fn with_default_tracing(self) -> Self {
        self.with_tracing(RUST_LOG_DEFAULT)
    }

    /// setup tracing for the cargo test code with `filter`
    /// ignore when called multiple times
    pub fn with_tracing(self, filter: &str) -> Self {
        let builder = if let Ok(filter) = tracing_subscriber::EnvFilter::try_from_default_env() {
            tracing_subscriber::fmt().with_env_filter(filter)
        } else {
            tracing_subscriber::fmt().with_env_filter(filter)
        };
        builder.try_init().ok();
        self
    }

    /// with the following shutdown order
    pub fn with_shutdown_order(mut self, shutdown: Vec<String>) -> Builder {
        self.shutdown_order = shutdown;
        self
    }

    /// build the config and start the containers
    pub async fn build(self) -> Result<ComposeTest, Box<dyn std::error::Error>> {
        let autorun = self.autorun;
        let mut compose = self.build_only().await?;
        if autorun {
            compose.start_all().await?;
        }
        Ok(compose)
    }

    fn override_flags(flag: &mut bool, flag_name: &str) {
        let key = format!("COMPOSE_{}", flag_name.to_ascii_uppercase());
        if let Some(val) = std::env::var_os(&key) {
            let clean = match val.to_str().unwrap_or_default() {
                "true" => true,
                "false" => false,
                _ => return,
            };
            if clean != *flag {
                tracing::warn!(
                    "env::{} => Overriding the {} flag to {}",
                    key,
                    flag_name,
                    clean
                );
                *flag = clean;
            }
        }
    }
    /// override clean flags with environment variable
    /// useful for testing without having to change the code
    fn override_debug_flags(&mut self) {
        Self::override_flags(&mut self.clean, "clean");
        Self::override_flags(&mut self.allow_clean_on_panic, "allow_clean_on_panic");
        Self::override_flags(&mut self.logs_on_panic, "logs_on_panic");
        let mut use_alpine = false;
        Self::override_flags(&mut use_alpine, "alpine");
        if use_alpine {
            self.image = Some("alpine:latest".to_string());
        }
    }

    /// build the config but don't start the containers
    async fn build_only(mut self) -> Result<ComposeTest, Box<dyn std::error::Error>> {
        self.override_debug_flags();

        let path = std::path::PathBuf::from(std::env!("CARGO_MANIFEST_DIR"));
        let srcdir = path.parent().unwrap().to_string_lossy().into();
        let docker = Docker::connect_with_unix_defaults()?;

        let mut cfg = HashMap::new();
        cfg.insert(
            "Subnet".to_string(),
            format!("{}/{}", self.network.network(), self.network.prefix()),
        );
        cfg.insert("Gateway".into(), self.network.nth(1).unwrap().to_string());

        let ipam = Ipam {
            driver: Some("default".into()),
            config: Some(vec![cfg]),
            options: None,
        };

        let mut compose = ComposeTest {
            name: self.name.clone(),
            srcdir,
            docker,
            network_id: "".to_string(),
            containers: Default::default(),
            shutdown_order: self.shutdown_order,
            ipam,
            label_prefix: self.label_prefix,
            reuse: self.reuse,
            allow_clean_on_panic: self.allow_clean_on_panic,
            clean: self.clean,
            prune: self.prune,
            prune_matching: self.prune_matching,
            image: self.image,
            logs_on_panic: self.logs_on_panic,
        };

        compose.network_id = compose.network_create().await.map_err(|e| e.to_string())?;

        for (spec, ip) in self.containers {
            compose.create_container(&spec, ip).await?;
        }

        Ok(compose)
    }
}

///
/// Some types to avoid confusion when
///
/// different networks are referred to, internally as networkId in docker
type NetworkId = String;
/// container name
type ContainerName = String;
/// container ID
type ContainerId = String;

#[derive(Clone, Debug)]
pub struct ComposeTest {
    /// used as the network name
    name: String,
    /// the source dir the tests are run in
    srcdir: String,
    /// handle to the docker daemon
    docker: Docker,
    /// the network id is used to attach containers to networks
    network_id: NetworkId,
    /// the name of containers and their (IDs, Ipv4) we have created
    /// perhaps not an ideal data structure, but we can improve it later
    /// if we need to
    containers: HashMap<ContainerName, (ContainerId, Ipv4Addr)>,
    /// container shutdown order
    shutdown_order: Vec<ContainerName>,
    /// the default network configuration we use for our test cases
    ipam: Ipam,
    /// prefix for labels set on containers and networks
    ///   $prefix.name = $name will be created automatically
    label_prefix: String,
    /// reuse existing containers
    reuse: bool,
    /// allow cleaning up on a panic (if clean is set)
    allow_clean_on_panic: bool,
    /// automatically clean up the things we have created for this test
    clean: bool,
    /// remove existing containers on the same network
    prune: bool,
    /// remove matching existing containers
    prune_matching: bool,
    /// base image for image-less containers
    image: Option<String>,
    /// output container logs on panic
    logs_on_panic: bool,
}

impl Drop for ComposeTest {
    /// destroy the containers and network. Notice that we use sync code here
    fn drop(&mut self) {
        if thread::panicking() && self.logs_on_panic {
            self.containers.keys().for_each(|name| {
                tracing::error!("Logs from container '{}':", name);
                let _ = std::process::Command::new("docker")
                    .args(&["logs", name])
                    .status();
            });
        }

        if self.clean && (!thread::panicking() || self.allow_clean_on_panic) {
            self.shutdown_order.iter().for_each(|c| {
                std::process::Command::new("docker")
                    .args(&["kill", "-s", "term", c])
                    .output()
                    .unwrap();
            });

            self.containers.keys().for_each(|c| {
                std::process::Command::new("docker")
                    .args(&["kill", "-s", "term", c])
                    .output()
                    .unwrap();
                std::process::Command::new("docker")
                    .args(&["kill", c])
                    .output()
                    .unwrap();
                std::process::Command::new("docker")
                    .args(&["rm", "-v", c])
                    .output()
                    .unwrap();
            });

            std::process::Command::new("docker")
                .args(&["network", "rm", &self.name])
                .output()
                .unwrap();
        }
    }
}

impl ComposeTest {
    /// Create a new network, with default settings. If a network with the same
    /// name already exists it will be reused. Note that we do not check the
    /// networking IP and/or subnets
    async fn network_create(&mut self) -> Result<NetworkId, Error> {
        let mut net = self.network_list_labeled().await?;
        if !net.is_empty() {
            let first = net.pop().unwrap();
            if Some(self.name.clone()) == first.name {
                // reuse the same network
                self.network_id = first.id.unwrap();
                // but clean up the existing containers
                self.prune_network_containers(&self.name, self.prune)
                    .await?;
                return Ok(self.network_id.clone());
            } else {
                self.network_remove_labeled().await?;
            }
        }

        let name_label = self.label_prefix();
        // we use the same network everywhere
        let create_opts = CreateNetworkOptions {
            name: self.name.as_str(),
            check_duplicate: true,
            driver: "bridge",
            internal: false,
            attachable: true,
            ingress: false,
            ipam: self.ipam.clone(),
            enable_ipv6: false,
            options: vec![("com.docker.network.bridge.name", "mayabridge0")]
                .into_iter()
                .collect(),
            labels: vec![(name_label.as_str(), self.name.as_str())]
                .into_iter()
                .collect(),
        };

        self.docker.create_network(create_opts).await.map(|r| {
            self.network_id = r.id.unwrap();
            self.network_id.clone()
        })
    }

    async fn network_remove_labeled(&self) -> Result<(), Error> {
        self.network_prune_labeled(true).await
    }
    async fn network_prune_labeled(&self, prune: bool) -> Result<(), Error> {
        let our_networks = self.network_list_labeled().await?;
        for network in our_networks {
            let name = &network.name.unwrap();
            self.prune_network_containers(name, prune).await?;
            if prune {
                self.network_remove(name).await?;
            }
        }
        Ok(())
    }

    /// remove all containers from the network
    pub async fn remove_network_containers(&self, network: &str) -> Result<(), Error> {
        self.prune_network_containers(network, true).await
    }
    /// remove all containers from the network
    pub async fn prune_network_containers(&self, network: &str, prune: bool) -> Result<(), Error> {
        let containers = self.list_network_containers(network).await?;
        for k in &containers {
            let name = k.id.clone().unwrap();
            tracing::trace!("Lookup container for removal: {:?}", k.names);
            if prune || (self.prune_matching && self.containers.get(&name).is_some()) {
                self.remove_container(&name).await?;
                while let Ok(_c) = self.docker.inspect_container(&name, None).await {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
        Ok(())
    }

    async fn network_remove(&self, name: &str) -> Result<(), Error> {
        tracing::debug!("Removing network: {}", name);
        // if the network is not found, its not an error, any other error is
        // reported as such. Networks can only be destroyed when all containers
        // attached to it are removed. To get a list of attached
        // containers, use network_list()
        if let Err(e) = self.docker.remove_network(name).await {
            if !matches!(e, Error::DockerResponseNotFoundError { .. }) {
                return Err(e);
            }
        }

        Ok(())
    }

    /// list all the docker networks with our filter
    pub async fn network_list_labeled(&self) -> Result<Vec<Network>, Error> {
        self.docker
            .list_networks(Some(ListNetworksOptions {
                filters: vec![("label", vec![self.label_prefix().as_str()])]
                    .into_iter()
                    .collect(),
            }))
            .await
    }

    async fn list_network_containers(
        &self,
        name: &str,
    ) -> Result<Vec<ContainerSummaryInner>, Error> {
        self.docker
            .list_containers(Some(ListContainersOptions {
                all: true,
                filters: vec![("network", vec![name])].into_iter().collect(),
                ..Default::default()
            }))
            .await
    }

    /// list containers
    pub async fn list_cluster_containers(&self) -> Result<Vec<ContainerSummaryInner>, Error> {
        self.docker
            .list_containers(Some(ListContainersOptions {
                all: true,
                filters: vec![(
                    "label",
                    vec![format!("{}.name={}", self.label_prefix, self.name).as_str()],
                )]
                .into_iter()
                .collect(),
                ..Default::default()
            }))
            .await
    }

    /// get the container with the provided name
    pub async fn get_cluster_container(
        &self,
        name: &str,
    ) -> Result<Option<ContainerSummaryInner>, Error> {
        let (id, _) = self.containers.get(name).unwrap();
        let all = self
            .docker
            .list_containers(Some(ListContainersOptions {
                all: true,
                filters: vec![(
                    "label",
                    vec![format!("{}.name={}", self.label_prefix, self.name).as_str()],
                )]
                .into_iter()
                .collect(),
                ..Default::default()
            }))
            .await?;
        Ok(all.iter().find(|c| c.id.as_ref() == Some(id)).cloned())
    }

    /// remove a container from the configuration
    async fn remove_container(&self, name: &str) -> Result<(), Error> {
        tracing::debug!("Removing container: {}", name);
        self.docker
            .remove_container(
                name,
                Some(RemoveContainerOptions {
                    v: true,
                    force: true,
                    link: false,
                }),
            )
            .await?;

        Ok(())
    }

    /// remove all containers and its network
    async fn remove_all(&self) -> Result<(), Error> {
        for k in &self.containers {
            self.stop(k.0).await?;
            self.remove_container(k.0).await?;
            while let Ok(_c) = self.docker.inspect_container(k.0, None).await {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
        self.network_remove(&self.name).await?;
        Ok(())
    }

    /// we need to construct several objects to create a setup that meets our
    /// liking:
    ///
    /// (1) hostconfig: that configures the host side of the container, i.e what
    /// features/settings from the host perspective do we want too setup
    /// for the container. (2) endpoints: this allows us to plugin in the
    /// container into our network configuration (3) config: the actual
    /// config which includes the above objects
    async fn create_container(
        &mut self,
        spec: &ContainerSpec,
        ipv4: Ipv4Addr,
    ) -> Result<(), Error> {
        tracing::debug!("Creating container: {}", spec.name);

        if self.prune || self.prune_matching {
            tracing::debug!("Killing/Removing container: {}", spec.name);
            let _ = self
                .docker
                .remove_container(
                    &spec.name,
                    Some(RemoveContainerOptions {
                        v: false,
                        force: true,
                        link: false,
                    }),
                )
                .await;
        }

        // pull the image, if missing
        self.pull_missing_image(&spec.image).await;

        let mut binds = vec![
            format!("{}:{}", self.srcdir, self.srcdir),
            "/dev/hugepages:/dev/hugepages:rw".into(),
        ];

        binds.extend(spec.binds());
        if let Some(bin) = &spec.binary {
            binds.push("/nix:/nix:ro".into());
            if !bin.path.starts_with("/nix") || !bin.path.starts_with(&self.srcdir) {
                let path = if spec.bind_binary_dir {
                    Path::new(&bin.path).parent().unwrap()
                } else {
                    Path::new(&bin.path)
                };
                let path = path.to_str().unwrap();
                binds.push(format!("{}:{}", path, path));
            }
            if (spec.image.is_some() || self.image.is_some()) && spec.init.unwrap_or(true) {
                if let Ok(tini) = Binary::which("tini") {
                    binds.push(format!("{}:{}", tini, "/sbin/tini"));
                }
            }
        }

        let mounts = if !spec.bypass_default_mounts {
            vec![
                // DPDK needs to have a /tmp
                Mount {
                    target: Some("/tmp".into()),
                    typ: Some(MountTypeEnum::TMPFS),
                    ..Default::default()
                },
                // mayastor needs to have a /var/tmp
                Mount {
                    target: Some("/var/tmp".into()),
                    typ: Some(MountTypeEnum::TMPFS),
                    ..Default::default()
                },
            ]
        } else {
            vec![]
        };

        let mut host_config = HostConfig {
            binds: Some(binds),
            mounts: Some(mounts),
            cap_add: Some(vec![
                "SYS_ADMIN".to_string(),
                "IPC_LOCK".into(),
                "SYS_NICE".into(),
            ]),
            privileged: spec.privileged(),
            security_opt: Some(vec!["seccomp=unconfined".into()]),
            init: spec.init,
            port_bindings: spec.port_map.clone(),
            ..Default::default()
        };

        let mut endpoints_config = HashMap::new();
        endpoints_config.insert(
            self.name.as_str(),
            EndpointSettings {
                network_id: Some(self.network_id.to_string()),
                ipam_config: Some(EndpointIpamConfig {
                    ipv4_address: Some(ipv4.to_string()),
                    ..Default::default()
                }),
                aliases: spec.network_aliases.clone(),
                ..Default::default()
            },
        );

        let mut env = spec.environment();
        env.push(format!("MY_POD_IP={}", ipv4));

        // figure out which ports to expose based on the port mapping
        let mut exposed_ports = HashMap::new();
        if let Some(map) = spec.port_map.as_ref() {
            map.iter().for_each(|binding| {
                exposed_ports.insert(binding.0.as_str(), HashMap::new());
            })
        }

        let name = spec.name.as_str();
        let mut cmd = spec.commands(&self.name);
        let image = spec
            .image
            .as_ref()
            .map_or_else(|| self.image.as_deref(), |s| Some(s.as_str()));

        let mut entrypoint = spec.entrypoint(&self.image);

        if let Some(image) = image {
            // merge our host config with the container image's default host config parameters
            let image = self.docker.inspect_image(image).await.unwrap();
            let config = image.config.unwrap_or_default();
            let mut img_cmd = config.cmd.unwrap_or_default();
            if !img_cmd.is_empty() && spec.command(&self.network_id).is_none() {
                // keep the default commands first, this way we can use the spec's cmd
                // as additional arguments
                img_cmd.extend(cmd);
                cmd = img_cmd;
            }
            let mut img_entrypoint = config.entrypoint.unwrap_or_default();
            if !img_entrypoint.is_empty() && entrypoint.is_empty() {
                // use the entrypoint from the container image
                entrypoint = img_entrypoint;
            } else if !entrypoint.is_empty() && spec.command(&self.network_id).is_none() {
                // extend the container image entrypoint with the command
                img_entrypoint.extend(cmd);
                // the new command now is the entrypoint plus the commands
                cmd = img_entrypoint;
            }
            host_config.init = None;
            match host_config.port_bindings.as_mut() {
                _ if !spec.bypass_image_port_map => {}
                None => {
                    let mut port_map = bollard::service::PortMap::new();
                    for (port, _) in config.exposed_ports.unwrap_or_default() {
                        port_map.insert(
                            port.clone(),
                            Some(vec![bollard::service::PortBinding {
                                host_ip: None,
                                host_port: port.split('/').next().map(ToString::to_string),
                            }]),
                        );
                    }
                    if !port_map.is_empty() {
                        host_config.port_bindings = Some(port_map);
                    }
                }
                Some(port_map) => {
                    for (port, _) in config.exposed_ports.unwrap_or_default() {
                        port_map.insert(
                            port.clone(),
                            Some(vec![bollard::service::PortBinding {
                                host_ip: None,
                                host_port: port.split('/').next().map(ToString::to_string),
                            }]),
                        );
                    }
                }
            }
        }

        let name_label = format!("{}.name", self.label_prefix);
        let config = Config {
            cmd: Some(cmd.iter().map(|s| s.as_str()).collect::<Vec<_>>()),
            env: Some(env.iter().map(|s| s.as_str()).collect()),
            entrypoint: Some(entrypoint.iter().map(|s| s.as_str()).collect::<Vec<_>>()),
            image,
            hostname: Some(name),
            host_config: Some(host_config),
            networking_config: Some(NetworkingConfig { endpoints_config }),
            working_dir: Some(self.srcdir.as_str()),
            volumes: Some(
                vec![
                    ("/dev/hugepages", HashMap::new()),
                    ("/nix", HashMap::new()),
                    (self.srcdir.as_str(), HashMap::new()),
                ]
                .into_iter()
                .collect(),
            ),
            labels: Some(
                vec![(name_label.as_str(), self.name.as_str())]
                    .into_iter()
                    .collect(),
            ),
            exposed_ports: Some(exposed_ports),
            ..Default::default()
        };

        let container = self
            .docker
            .create_container(Some(CreateContainerOptions { name }), config)
            .await
            .unwrap();

        self.containers
            .insert(name.to_string(), (container.id, ipv4));

        Ok(())
    }

    /// Pulls the docker image, if one is specified and is not present locally
    async fn pull_missing_image(&self, image: &Option<String>) {
        if let Some(image) = image {
            let image = if !image.contains(':') {
                format!("{}:latest", image)
            } else {
                image.clone()
            };
            if !self.image_exists(&image).await {
                self.pull_image(&image).await;
            }
        }
    }

    /// Check if image exists locally
    async fn image_exists(&self, image: &str) -> bool {
        let images = self.docker.list_images::<String>(None).await.unwrap();
        images
            .iter()
            .any(|i| i.repo_tags.iter().any(|t| t == image))
    }

    /// Pulls the docker image
    async fn pull_image(&self, image: &str) {
        let mut stream = self
            .docker
            .create_image(
                Some(CreateImageOptions {
                    from_image: image,
                    ..Default::default()
                }),
                None,
                None,
            )
            .into_future()
            .await;

        while let Some(result) = stream.0.as_ref() {
            let info = result.as_ref().unwrap();
            tracing::trace!("{:?}", &info);
            stream = stream.1.into_future().await;
        }
    }

    /// start the container
    pub async fn start(&self, name: &str) -> Result<(), Error> {
        tracing::trace!("Start container: {}", name);
        let id = self
            .containers
            .get(name)
            .ok_or(bollard::errors::Error::IOError {
                err: std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Can't start container {} as it was not configured", name),
                ),
            })?;
        if !self.reuse || self.prune_matching {
            tracing::debug!("Starting container: {}, ip: {}", name, id.1);
            self.docker
                .start_container::<&str>(id.0.as_str(), None)
                .await?;
        }

        Ok(())
    }

    /// stop the container
    pub async fn stop(&self, name: &str) -> Result<(), Error> {
        let id = self.containers.get(name).unwrap();
        self.stop_id(id.0.as_str()).await
    }

    /// stop the container by its id
    pub async fn stop_id(&self, id: &str) -> Result<(), Error> {
        if let Err(e) = self
            .docker
            .stop_container(id, Some(StopContainerOptions { t: 3 }))
            .await
        {
            // where already stopped
            if !matches!(e, Error::DockerResponseNotModifiedError { .. }) {
                return Err(e);
            }
        }

        Ok(())
    }

    /// kill the container
    pub async fn kill(&self, name: &str) -> Result<(), Error> {
        let id = self.containers.get(name).unwrap();
        self.kill_id(id.0.as_str()).await
    }

    /// kill the container by its id
    pub async fn kill_id(&self, id: &str) -> Result<(), Error> {
        if let Err(e) = self
            .docker
            .kill_container(id, Some(KillContainerOptions { signal: "SIGKILL" }))
            .await
        {
            // where already killed
            if !matches!(e, Error::DockerResponseNotModifiedError { .. }) {
                return Err(e);
            }
        }

        Ok(())
    }

    /// restart the container
    pub async fn restart(&self, name: &str) -> Result<(), Error> {
        let (id, _) = self.containers.get(name).unwrap();
        self.restart_id(id.as_str()).await
    }

    /// restart the container id
    pub async fn restart_id(&self, id: &str) -> Result<(), Error> {
        if let Err(e) = self
            .docker
            .restart_container(id, Some(RestartContainerOptions { t: 3 }))
            .await
        {
            // where already stopped
            if !matches!(e, Error::DockerResponseNotModifiedError { .. }) {
                return Err(e);
            }
        }

        Ok(())
    }

    /// disconnect container from the network
    pub async fn disconnect(&self, container_name: &str) -> Result<(), Error> {
        let id = self.containers.get(container_name).unwrap();
        self.docker
            .disconnect_network(
                &self.network_id,
                DisconnectNetworkOptions {
                    container: id.0.as_str(),
                    force: false,
                },
            )
            .await
    }

    /// get the logs from the container. It would be nice to make it implicit
    /// that is, when you make a rpc call, whatever logs where created due to
    /// that are returned
    pub async fn logs(&self, name: &str) -> Result<(), Error> {
        let logs = self
            .docker
            .logs(
                name,
                Some(LogsOptions {
                    follow: false,
                    stdout: true,
                    stderr: true,
                    since: 0, // TODO log lines since last call?
                    until: 0,
                    timestamps: false,
                    tail: "all",
                }),
            )
            .try_collect::<Vec<_>>()
            .await?;

        logs.iter().for_each(|l| print!("{}:{}", name, l));
        Ok(())
    }

    /// get the logs from all of the containers. It would be nice to make it
    /// implicit that is, when you make a rpc call, whatever logs where
    /// created due to that are returned
    pub async fn logs_all(&self) -> Result<(), Error> {
        for container in &self.containers {
            let _ = self.logs(container.0).await;
        }
        Ok(())
    }

    /// start all the containers
    async fn start_all(&mut self) -> Result<(), Error> {
        for k in &self.containers {
            self.start(k.0).await?;
        }

        Ok(())
    }

    /// start the containers
    pub async fn start_containers(&self, containers: Vec<&str>) -> Result<(), Error> {
        for k in containers {
            self.start(k).await?;
        }
        Ok(())
    }

    /// get container ip
    pub fn container_ip(&self, name: &str) -> String {
        let (_id, ip) = self.containers.get(name).unwrap();
        ip.to_string()
    }

    /// check if a container exists
    pub async fn container_exists(&self, name: &str) -> bool {
        let containers = self
            .list_network_containers(&self.name)
            .await
            .unwrap_or_default();

        self.containers.get(name).is_some()
            || containers.iter().any(|c| {
                c.names
                    .clone()
                    .unwrap_or_default()
                    .contains(&name.to_string())
            })
    }

    /// stop all the containers part of the network
    /// returns the last error, if any or Ok
    pub async fn stop_network_containers(&self) -> Result<(), Error> {
        let mut result = Ok(());
        let containers = self.list_network_containers(&self.name).await?;
        for container in containers {
            if let Some(id) = container.id {
                if let Err(e) = self.stop_id(&id).await {
                    println!("Failed to stop container id {:?}", id);
                    result = Err(e);
                }
            }
        }
        result
    }

    /// restart all the containers part of the network
    /// returns the last error, if any or Ok
    pub async fn restart_network_containers(&self) -> Result<(), Error> {
        let mut result = Ok(());
        let containers = self.list_network_containers(&self.name).await?;
        for container in containers {
            if let Some(id) = container.id {
                if let Err(e) = self.restart_id(&id).await {
                    println!("Failed to restart container id {:?}", id);
                    result = Err(e);
                }
            }
        }
        result
    }

    /// inspect the given container
    pub async fn inspect(&self, name: &str) -> Result<ContainerInspectResponse, Error> {
        self.docker.inspect_container(name, None).await
    }

    /// pause the container; unfortunately, when the API returns it does not
    /// mean that the container indeed is frozen completely, in the sense
    /// that it's not to be assumed that right after a call -- the container
    /// stops responding.
    pub async fn pause(&self, name: &str) -> Result<(), Error> {
        let id = self.containers.get(name).unwrap();
        self.docker.pause_container(id.0.as_str()).await?;

        Ok(())
    }

    /// un_pause the container
    pub async fn thaw(&self, name: &str) -> Result<(), Error> {
        let id = self.containers.get(name).unwrap();
        self.docker.unpause_container(id.0.as_str()).await
    }

    /// return grpc handles to the containers
    pub async fn grpc_handles(&self) -> Result<Vec<RpcHandle>, String> {
        let mut handles = Vec::new();
        for v in &self.containers {
            handles.push(
                RpcHandle::connect(
                    v.0.clone(),
                    format!("{}:10124", v.1 .1).parse::<SocketAddr>().unwrap(),
                )
                .await?,
            );
        }

        Ok(handles)
    }

    /// return grpc handle to the container
    pub async fn grpc_handle(&self, name: &str) -> Result<RpcHandle, String> {
        match self.containers.iter().find(|&c| c.0 == name) {
            Some(container) => Ok(RpcHandle::connect(
                container.0.clone(),
                format!("{}:10124", container.1 .1)
                    .parse::<SocketAddr>()
                    .unwrap(),
            )
            .await?),
            None => Err(format!("Container {} not found!", name)),
        }
    }

    /// explicitly remove all containers
    pub async fn down(&self) {
        self.remove_all().await.unwrap();
    }

    pub fn label_prefix(&self) -> String {
        format!("{}.name", self.label_prefix)
    }
}
