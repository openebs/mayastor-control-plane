pub mod rest_client;

use composer::{Builder, ComposeTest};
use deployer_lib::{
    default_agents,
    infra::{Components, Error, Mayastor},
    StartOptions,
};
use opentelemetry::{global, sdk::propagation::TraceContextPropagator};

use common_lib::{mbus_api, mbus_api::TimeoutOptions, types::v0::message_bus};
use openapi::apis::Uuid;

use common_lib::{
    mbus_api::ReplyError,
    opentelemetry::default_tracing_tags,
    types::v0::{
        message_bus::CreatePool,
        store::{
            definitions::ObjectKey,
            registry::{ControlPlaneService, StoreLeaseLockKey},
        },
    },
};
pub use etcd_client;
use etcd_client::DeleteOptions;
use grpc::{
    client::CoreClient,
    grpc_opts::Context,
    operations::{
        pool::traits::PoolOperations, replica::traits::ReplicaOperations,
        volume::traits::VolumeOperations,
    },
};
use rpc::mayastor::RpcHandle;
use std::{
    collections::HashMap,
    convert::{TryFrom, TryInto},
    net::SocketAddr,
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use structopt::StructOpt;
use tonic::transport::Uri;
use tracing_subscriber::{filter::Directive, layer::SubscriberExt, EnvFilter, Registry};

const RUST_LOG_QUIET_DEFAULTS: &str =
    "h2=info,hyper=info,tower_buffer=info,tower=info,rustls=info,reqwest=info,tokio_util=info,async_io=info,polling=info,tonic=info,want=info,mio=info,bollard=info,composer=info";

#[tokio::test]
#[ignore]
async fn smoke_test() {
    // make sure the cluster can bootstrap properly
    let _cluster = ClusterBuilder::builder()
        .build()
        .await
        .expect("Should bootstrap the cluster!");
}

/// Default options to create a cluster
pub fn default_options() -> StartOptions {
    // using from_iter as Default::default would not set the default_value from structopt
    let options: StartOptions = StartOptions::from_iter(&[""]);
    options
        .with_agents(default_agents().split(',').collect())
        .with_jaeger(true)
        .with_mayastors(1)
        .with_show_info(true)
        .with_build_all(true)
        .with_env_tags(vec!["CARGO_PKG_NAME"])
}

/// Cluster with the composer, the rest client and the jaeger pipeline
#[allow(unused)]
pub struct Cluster {
    composer: ComposeTest,
    rest_client: rest_client::RestClient,
    grpc_client: Option<CoreClient>,
    trace_guard: Arc<tracing::subscriber::DefaultGuard>,
    builder: ClusterBuilder,
}

impl Cluster {
    /// compose utility
    pub fn composer(&self) -> &ComposeTest {
        &self.composer
    }

    /// grpc client for connection
    pub fn grpc_client(&self) -> &CoreClient {
        self.grpc_client.as_ref().unwrap()
    }

    pub async fn volume_service_liveness(
        &self,
        client: &dyn VolumeOperations,
        timeout_opts: Option<TimeoutOptions>,
    ) -> Result<bool, ReplyError> {
        let timeout_opts = if timeout_opts.clone().is_none() {
            Some(
                TimeoutOptions::new()
                    .with_timeout(Duration::from_millis(500))
                    .with_max_retries(5),
            )
        } else {
            timeout_opts
        };
        for x in 1 .. timeout_opts.clone().unwrap().max_retires().unwrap() {
            match client.probe(Some(Context::new(timeout_opts.clone()))).await {
                Ok(resp) => return Ok(resp),
                Err(_) => {
                    tracing::info!("Volume Service not available, Retrying ....{}", x);
                    tokio::time::sleep(timeout_opts.clone().unwrap().base_timeout()).await;
                }
            }
        }
        Err(ReplyError::invalid_reply_error(
            "Max tries exceeded, volume service not up".to_string(),
        ))
    }

    /// return grpc handle to the container
    pub async fn grpc_handle(&self, name: &str) -> Result<RpcHandle, String> {
        match self.composer.containers().iter().find(|&c| c.0 == name) {
            Some(container) => Ok(RpcHandle::connect(
                container.0,
                format!("{}:10124", container.1 .1)
                    .parse::<SocketAddr>()
                    .unwrap(),
            )
            .await?),
            None => Err(format!("Container {} not found!", name)),
        }
    }

    /// restart the core agent
    pub async fn restart_core(&self) {
        self.remove_store_lock(ControlPlaneService::CoreAgent).await;
        self.composer.restart("core").await.unwrap();
    }

    /// remove etcd store lock for `name` instance
    pub async fn remove_store_lock(&self, name: ControlPlaneService) {
        let mut store = etcd_client::Client::connect(["0.0.0.0:2379"], None)
            .await
            .expect("Failed to connect to etcd.");
        store
            .delete(
                StoreLeaseLockKey::new(&name).key(),
                Some(DeleteOptions::new().with_prefix()),
            )
            .await
            .unwrap();
    }

    /// node id for `index`
    pub fn node(&self, index: u32) -> message_bus::NodeId {
        Mayastor::name(index, &self.builder.opts).into()
    }

    /// node ip for `index`
    pub fn node_ip(&self, index: u32) -> String {
        let name = self.node(index);
        self.composer.container_ip(name.as_str())
    }

    /// pool id for `pool` index on `node` index
    pub fn pool(&self, node: u32, pool: u32) -> message_bus::PoolId {
        format!("{}-pool-{}", self.node(node), pool + 1).into()
    }

    /// replica id with index for `pool` index and `replica` index
    pub fn replica(node: u32, pool: usize, replica: u32) -> message_bus::ReplicaId {
        if replica > 254 || pool > 254 || node > 254 {
            panic!("too large");
        }
        let mut uuid = message_bus::ReplicaId::default().to_string();
        // we can't use a uuid with all zeroes, as spdk seems to ignore it and generate new one
        let replica = replica + 1;
        let _ = uuid.drain(24 .. uuid.len());
        format!(
            "{}{:02x}{:02x}{:08x}",
            uuid, node as u8, pool as u8, replica
        )
        .try_into()
        .unwrap()
    }

    /// openapi rest client v0
    pub fn rest_v00(&self) -> common_lib::types::v0::openapi::tower::client::direct::ApiClient {
        self.rest_client.v0()
    }

    /// New cluster
    async fn new(
        trace: bool,
        env_filter: Option<EnvFilter>,
        timeout_rest: std::time::Duration,
        bus_timeout: TimeoutOptions,
        bearer_token: Option<String>,
        components: Components,
        composer: ComposeTest,
    ) -> Result<Cluster, Error> {
        let rest_client = rest_client::RestClient::new_timeout(
            "http://localhost:8081",
            trace,
            bearer_token,
            timeout_rest,
        )
        .unwrap();

        components
            .start_wait(&composer, std::time::Duration::from_secs(30))
            .await?;

        let unknown_module = "unknown".to_string();
        let mut test_module = None;
        if let Ok(mcp_root) = std::env::var("MCP_SRC") {
            backtrace::trace(|frame| {
                backtrace::resolve_frame(frame, |symbol| {
                    if let Some(name) = symbol.name() {
                        if let Some(filename) = symbol.filename() {
                            if filename.starts_with(&mcp_root) && !filename.ends_with(file!()) {
                                let name = name.to_string();
                                let name = match name.split('{').collect::<Vec<_>>().first() {
                                    Some(name) => {
                                        let name = name.to_string();
                                        name.trim_end_matches("::").to_string()
                                    }
                                    None => unknown_module.clone(),
                                };
                                test_module = Some(name);
                            }
                        }
                    }
                });
                test_module.is_none()
            });
        }

        let subscriber = Registry::default()
            // todo: add env filter as an optional layer
            .with(env_filter.unwrap())
            .with(tracing_subscriber::fmt::layer());

        let mut tracing_tags = vec![];
        let trace_guard = Arc::new(match trace {
            true => {
                tracing_tags.append(&mut default_tracing_tags(
                    utils::git_version(),
                    env!("CARGO_PKG_VERSION"),
                ));
                tracing_tags.dedup();

                global::set_text_map_propagator(TraceContextPropagator::new());
                let tracer = opentelemetry_jaeger::new_pipeline()
                    .with_service_name("cluster-client")
                    .with_tags(tracing_tags)
                    .install_simple()
                    .expect("Should be able to initialise the exporter");
                let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
                tracing::subscriber::set_default(subscriber.with(telemetry))
            }
            false => tracing::subscriber::set_default(subscriber),
        });

        let grpc_client = if components.core_enabled() {
            Some(
                CoreClient::new(
                    Uri::try_from(grpc_addr(composer.container_ip("core"))).unwrap(),
                    bus_timeout.clone(),
                )
                .await,
            )
        } else {
            None
        };

        let cluster = Cluster {
            composer,
            rest_client,
            grpc_client,
            trace_guard,
            builder: ClusterBuilder::builder(),
        };

        if components.nats_enabled() {
            // the deployer uses a "fake" message bus so now it's time to
            // connect to the "real" message bus
            cluster.connect_to_bus_timeout("nats", bus_timeout).await;
        }

        Ok(cluster)
    }

    /// connect to message bus helper for the cargo test code with bus timeouts
    async fn connect_to_bus_timeout(&self, name: &str, bus_timeout: TimeoutOptions) {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            mbus_api::message_bus_init_options(None, self.composer.container_ip(name), bus_timeout)
                .await
        })
        .await
        .unwrap();
    }
}

fn option_str<F: ToString>(input: Option<F>) -> String {
    match input {
        Some(input) => input.to_string(),
        None => "?".into(),
    }
}

/// Run future and compare result with what's expected
/// Expected result should be in the form Result<TestValue,TestValue>
/// where TestValue is a useful value which will be added to the returned error
/// string Eg, testing the replica share protocol:
/// test_result(Ok(Nvmf), async move { ... })
/// test_result(Err(NBD), async move { ... })
pub async fn test_result<F, O, E, T>(
    expected: &Result<O, E>,
    future: F,
) -> Result<(), anyhow::Error>
where
    F: std::future::Future<Output = Result<T, common_lib::mbus_api::Error>>,
    E: std::fmt::Debug,
    O: std::fmt::Debug,
{
    match future.await {
        Ok(_) if expected.is_ok() => Ok(()),
        Err(error) if expected.is_err() => match error {
            common_lib::mbus_api::Error::ReplyWithError { .. } => Ok(()),
            _ => {
                // not the error we were waiting for
                Err(anyhow::anyhow!("Invalid response: {:?}", error))
            }
        },
        Err(error) => Err(anyhow::anyhow!(
            "Expected '{:#?}' but failed with '{:?}'!",
            expected,
            error
        )),
        Ok(_) => Err(anyhow::anyhow!("Expected '{:#?}' but succeeded!", expected)),
    }
}

/// Run future and compare result with what's expected, for grpc
pub async fn test_result_grpc<F, O, E, T>(
    expected: &Result<O, E>,
    future: F,
) -> Result<(), ReplyError>
where
    F: std::future::Future<Output = Result<T, ReplyError>>,
    E: std::fmt::Debug,
    O: std::fmt::Debug,
    T: std::fmt::Debug,
{
    match future.await {
        Ok(_) if expected.is_ok() => Ok(()),
        Err(error) if expected.is_err() => {
            let ReplyError { .. } = error;
            Ok(())
        }
        Err(error) => Err(ReplyError::invalid_reply_error(format!(
            "Expected '{:#?}' but failed with '{:?}'!",
            expected, error
        ))),
        Ok(r) => Err(ReplyError::invalid_reply_error(format!(
            "Expected '{:#?} {:#?}' but succeeded!",
            expected, r
        ))),
    }
}

#[macro_export]
macro_rules! result_either {
    ($test:expr) => {
        match $test {
            Ok(v) => v,
            Err(v) => v,
        }
    };
}

#[derive(Clone)]
enum PoolDisk {
    Malloc(u64),
    Uri(String),
    Tmp(TmpDiskFile),
}

/// Temporary "disk" file, which gets deleted on drop
#[derive(Clone)]
pub struct TmpDiskFile {
    inner: std::sync::Arc<TmpDiskFileInner>,
}

struct TmpDiskFileInner {
    path: String,
    uri: String,
}

impl TmpDiskFile {
    /// Creates a new file on `path` with `size`.
    /// The file is deleted on drop.
    pub fn new(name: &str, size: u64) -> Self {
        Self {
            inner: std::sync::Arc::new(TmpDiskFileInner::new(name, size)),
        }
    }
    /// Disk URI to be used by mayastor
    pub fn uri(&self) -> &str {
        self.inner.uri()
    }
}
impl TmpDiskFileInner {
    fn new(name: &str, size: u64) -> Self {
        let path = format!("/tmp/mayastor-{}", name);
        let file = std::fs::File::create(&path).expect("to create the tmp file");
        file.set_len(size).expect("to truncate the tmp file");
        Self {
            // mayastor is setup with a bind mount from /tmp to /host/tmp
            uri: format!(
                "aio:///host{}?blk_size=512&uuid={}",
                path,
                message_bus::PoolId::new()
            ),
            path,
        }
    }
    fn uri(&self) -> &str {
        &self.uri
    }
}

impl Drop for TmpDiskFileInner {
    fn drop(&mut self) {
        std::fs::remove_file(&self.path).expect("to unlink the tmp file");
    }
}

/// Builder for the Cluster
pub struct ClusterBuilder {
    opts: StartOptions,
    pools: HashMap<u32, Vec<PoolDisk>>,
    replicas: Replica,
    trace: bool,
    env_filter: Option<EnvFilter>,
    bearer_token: Option<String>,
    rest_timeout: std::time::Duration,
    bus_timeout: TimeoutOptions,
}

#[derive(Default)]
struct Replica {
    count: u32,
    size: u64,
    share: message_bus::Protocol,
}

/// default timeout options for every bus request
fn bus_timeout_opts() -> TimeoutOptions {
    TimeoutOptions::default()
        .with_timeout(Duration::from_secs(5))
        .with_timeout_backoff(Duration::from_millis(500))
        .with_max_retries(2)
}

impl ClusterBuilder {
    /// Cluster Builder with default options
    #[must_use]
    pub fn builder() -> Self {
        ClusterBuilder {
            opts: default_options(),
            pools: Default::default(),
            replicas: Default::default(),
            trace: true,
            env_filter: None,
            bearer_token: None,
            rest_timeout: std::time::Duration::from_secs(5),
            bus_timeout: bus_timeout_opts(),
        }
        .with_default_tracing()
    }
    /// Update the start options
    #[must_use]
    pub fn with_options<F>(mut self, set: F) -> Self
    where
        F: Fn(StartOptions) -> StartOptions,
    {
        self.opts = set(self.opts);
        self
    }
    /// Enable/Disable the default tokio tracing setup.
    #[must_use]
    pub fn with_default_tracing(self) -> Self {
        self.with_tracing_filter(
            Self::rust_log_add_quiet_defaults(
                EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug")),
            )
            .to_string()
            .as_str(),
        )
    }
    /// Silence common_lib and testlib traces by setting them to WARN.
    #[must_use]
    pub fn with_silence_test_traces(mut self) -> Self {
        self.env_filter = self.env_filter.map(|f| {
            f.add_directive(Directive::from_str("common_lib=warn").unwrap())
                .add_directive(Directive::from_str("testlib=warn").unwrap())
        });
        self
    }
    fn rust_log_add_quiet_defaults(
        current: tracing_subscriber::EnvFilter,
    ) -> tracing_subscriber::EnvFilter {
        let main = match current.to_string().as_str() {
            "debug" => "debug",
            "trace" => "trace",
            _ => return current,
        };
        let logs = format!("{},{}", main, RUST_LOG_QUIET_DEFAULTS);
        tracing_subscriber::EnvFilter::new(logs)
    }
    /// Enable/Disable jaeger tracing.
    #[must_use]
    pub fn with_jaeger_tracing(mut self, enabled: bool) -> Self {
        self.trace = enabled;
        self
    }
    /// Use the provided filter for tracing.
    #[must_use]
    pub fn with_tracing_filter<'a>(mut self, filter: impl Into<Option<&'a str>>) -> Self {
        self.env_filter = filter.into().map(tracing_subscriber::EnvFilter::new);
        self
    }
    /// Rest request timeout
    #[must_use]
    pub fn with_rest_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.rest_timeout = timeout;
        self
    }
    /// Add `count` malloc pools (100MiB size) to each node
    #[must_use]
    pub fn with_pools(mut self, count: u32) -> Self {
        for _ in 0 .. count {
            for node in 0 .. self.opts.mayastors {
                if let Some(pools) = self.pools.get_mut(&node) {
                    pools.push(PoolDisk::Malloc(100 * 1024 * 1024));
                } else {
                    self.pools
                        .insert(node, vec![PoolDisk::Malloc(100 * 1024 * 1024)]);
                }
            }
        }
        self
    }
    /// Add pool URI with `disk` to the node `index`
    #[must_use]
    pub fn with_pool(mut self, index: u32, disk: &str) -> Self {
        if let Some(pools) = self.pools.get_mut(&index) {
            pools.push(PoolDisk::Uri(disk.to_string()));
        } else {
            self.pools
                .insert(index, vec![PoolDisk::Uri(disk.to_string())]);
        }
        self
    }
    /// Add a tmpfs img pool with `disk` to each mayastor node with the specified `size`
    #[must_use]
    pub fn with_tmpfs_pool(mut self, size: u64) -> Self {
        for node in 0 .. self.opts.mayastors {
            let disk = TmpDiskFile::new(&Uuid::new_v4().to_string(), size);
            if let Some(pools) = self.pools.get_mut(&node) {
                pools.push(PoolDisk::Tmp(disk));
            } else {
                self.pools.insert(node, vec![PoolDisk::Tmp(disk)]);
            }
        }
        self
    }
    /// Specify `count` replicas to add to each node per pool
    #[must_use]
    pub fn with_replicas(mut self, count: u32, size: u64, share: message_bus::Protocol) -> Self {
        self.replicas = Replica { count, size, share };
        self
    }
    /// Specify `count` mayastors for the cluster
    #[must_use]
    pub fn with_mayastors(mut self, count: u32) -> Self {
        self.opts = self.opts.with_mayastors(count);
        self
    }
    /// Specify which agents to use
    #[must_use]
    pub fn with_agents(mut self, agents: Vec<&str>) -> Self {
        self.opts = self.opts.with_agents(agents);
        self
    }
    /// Specify the node deadline for the node agent
    /// eg: 2s
    #[must_use]
    pub fn with_node_deadline(mut self, deadline: &str) -> Self {
        self.opts = self.opts.with_node_deadline(deadline);
        self
    }
    /// The period at which the registry updates its cache of all
    /// resources from all nodes
    #[must_use]
    pub fn with_cache_period(mut self, period: &str) -> Self {
        self.opts = self.opts.with_cache_period(period);
        self
    }

    /// With reconcile periods:
    /// `busy` for when there's work that needs to be retried on the next poll
    /// `idle` when there's no work pending
    #[must_use]
    pub fn with_reconcile_period(mut self, busy: Duration, idle: Duration) -> Self {
        self.opts = self.opts.with_reconcile_period(busy, idle);
        self
    }
    /// With store operation timeout
    #[must_use]
    pub fn with_store_timeout(mut self, timeout: Duration) -> Self {
        self.opts = self.opts.with_store_timeout(timeout);
        self
    }
    /// With store lease ttl
    #[must_use]
    pub fn with_store_lease_ttl(mut self, ttl: Duration) -> Self {
        self.opts = self.opts.with_store_lease_ttl(ttl);
        self
    }
    /// Specify the node connect and request timeouts
    #[must_use]
    pub fn with_req_timeouts(mut self, connect: Duration, request: Duration) -> Self {
        self.opts = self.opts.with_req_timeouts(true, connect, request);
        self
    }
    /// Specify the node connect and request timeouts and whether to use minimum timeouts or not
    #[must_use]
    pub fn with_req_timeouts_min(
        mut self,
        no_min: bool,
        connect: Duration,
        request: Duration,
    ) -> Self {
        self.opts = self.opts.with_req_timeouts(no_min, connect, request);
        self
    }
    /// Specify the message bus timeout options
    #[must_use]
    pub fn with_bus_timeouts(mut self, timeout: TimeoutOptions) -> Self {
        self.bus_timeout = timeout;
        self
    }
    /// Specify whether rest is enabled or not
    #[must_use]
    pub fn with_rest(mut self, enabled: bool) -> Self {
        self.opts = self.opts.with_rest(enabled, None);
        self
    }
    /// Specify whether nats is enabled or not
    #[must_use]
    pub fn with_nats(mut self, enabled: bool) -> Self {
        self.opts = self.opts.with_nats(enabled);
        self
    }
    /// Specify whether jaeger is enabled or not
    #[must_use]
    pub fn with_jaeger(mut self, enabled: bool) -> Self {
        self.opts = self.opts.with_jaeger(enabled);
        self
    }
    /// Specify whether rest is enabled or not and wether to use authentication or not
    #[must_use]
    pub fn with_rest_auth(mut self, enabled: bool, jwk: Option<String>) -> Self {
        self.opts = self.opts.with_rest(enabled, jwk);
        self
    }
    /// Specify whether the components should be cargo built or not
    #[must_use]
    pub fn with_build(mut self, enabled: bool) -> Self {
        self.opts = self.opts.with_build(enabled);
        self
    }
    /// Specify whether the workspace binaries should be cargo built or not
    #[must_use]
    pub fn with_build_all(mut self, enabled: bool) -> Self {
        self.opts = self.opts.with_build_all(enabled);
        self
    }
    /// Build into the resulting Cluster using a composer closure, eg:
    /// .compose_build(|c| c.with_logs(false))
    pub async fn compose_build<F>(mut self, set: F) -> Result<Cluster, Error>
    where
        F: Fn(Builder) -> Builder,
    {
        let (components, composer) = self.build_prepare()?;
        let composer = set(composer);
        let mut cluster = self.new_cluster(components, composer).await?;
        cluster.builder = self;
        Ok(cluster)
    }
    /// Build into the resulting Cluster
    pub async fn build(mut self) -> Result<Cluster, Error> {
        let (components, composer) = self.build_prepare()?;
        let mut cluster = self.new_cluster(components, composer).await?;
        cluster.builder = self;
        Ok(cluster)
    }
    fn build_prepare(&self) -> Result<(Components, Builder), Error> {
        // Ensure that the composer is initialised with the correct root path.
        composer::initialize(std::path::Path::new(std::env!("MCP_SRC")).to_str().unwrap());
        let components = Components::new(self.opts.clone());
        let composer = Builder::new()
            .name(&self.opts.cluster_label.name())
            .configure(components.clone())?
            .with_base_image(self.opts.base_image.clone())
            .autorun(false)
            .with_clean(true)
            // test script will clean up containers if ran on CI/CD
            .with_clean_on_panic(false)
            .with_logs(true);
        Ok((components, composer))
    }
    async fn new_cluster(
        &mut self,
        components: Components,
        compose_builder: Builder,
    ) -> Result<Cluster, Error> {
        let compose_builder = compose_builder.with_shutdown_order(components.shutdown_order());
        let composer = compose_builder.build().await?;

        let cluster = Cluster::new(
            self.trace,
            self.env_filter.take(),
            self.rest_timeout,
            self.bus_timeout.clone(),
            self.bearer_token.clone(),
            components,
            composer,
        )
        .await?;

        if self.opts.show_info {
            for container in cluster.composer.list_cluster_containers().await? {
                let networks = container.network_settings.unwrap().networks.unwrap();
                let ip = networks
                    .get(&self.opts.cluster_label.name())
                    .unwrap()
                    .ip_address
                    .clone();
                tracing::debug!(
                    "{:?} [{}] {}",
                    container.names.clone().unwrap_or_default(),
                    ip.clone().unwrap_or_default(),
                    option_str(container.command.clone())
                );
            }
        }

        for pool in &self.pools() {
            let pool_client = cluster.grpc_client().pool();
            let replica_client = cluster.grpc_client().replica();
            pool_client
                .create(
                    &CreatePool {
                        node: pool.node.clone().into(),
                        id: pool.id(),
                        disks: vec![pool.disk()],
                        labels: None,
                    },
                    None,
                )
                .await
                .unwrap();

            for replica in &pool.replicas {
                replica_client.create(replica, None).await.unwrap();
            }
        }

        Ok(cluster)
    }
    fn pools(&self) -> Vec<Pool> {
        let mut pools = vec![];

        for (node, i_pools) in &self.pools {
            for (pool_index, pool) in i_pools.iter().enumerate() {
                let mut pool = Pool {
                    node: Mayastor::name(*node, &self.opts),
                    disk: pool.clone(),
                    index: (pool_index + 1) as u32,
                    replicas: vec![],
                };
                for replica_index in 0 .. self.replicas.count {
                    let rep_id = Cluster::replica(*node, pool_index, replica_index);
                    pool.replicas.push(message_bus::CreateReplica {
                        node: pool.node.clone().into(),
                        name: None,
                        uuid: rep_id,
                        pool: pool.id(),
                        size: self.replicas.size,
                        thin: false,
                        share: self.replicas.share,
                        managed: false,
                        owners: Default::default(),
                    });
                }
                pools.push(pool);
            }
        }
        pools
    }
}

struct Pool {
    node: String,
    disk: PoolDisk,
    index: u32,
    replicas: Vec<message_bus::CreateReplica>,
}

impl Pool {
    fn id(&self) -> message_bus::PoolId {
        format!("{}-pool-{}", self.node, self.index).into()
    }
    fn disk(&self) -> message_bus::PoolDeviceUri {
        match &self.disk {
            PoolDisk::Malloc(size) => {
                let size = size / (1024 * 1024);
                format!(
                    "malloc:///disk{}?size_mb={}&uuid={}",
                    self.index,
                    size,
                    message_bus::PoolId::new()
                )
                .into()
            }
            PoolDisk::Uri(uri) => uri.into(),
            PoolDisk::Tmp(disk) => disk.uri().into(),
        }
    }
}

fn grpc_addr(ip: String) -> String {
    format!("https://{}:50051", ip)
}
