use crate::{
    controller::{
        registry::Registry,
        resources::{
            operations::ResourceDrain,
            operations_helper::{OperationSequenceGuard, ResourceSpecsLocked},
        },
    },
    CliArgs,
};
use agents::errors::SvcError;
use clap::Parser;
use deployer_cluster::{Cluster, ClusterBuilder};
use std::{future::Future, sync::Arc, time::Duration};
use stor_port::{
    pstor::{etcd::Etcd, Error as StoreError, StorableObject, StoreObj},
    types::v0::{
        store::{
            app_node::{AppNodeSpec, AppNodeSpecKey},
            node::{NodeLabels, NodeSpec, NodeSpecKey},
        },
        transport::{Deregister, Register, RegisterAppNode},
    },
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::Mutex,
    task::JoinHandle,
    time::timeout,
};

/// A real registry and etcd, with a gate only on the registry's incoming replies.
struct StoreReplyGate {
    registry: Registry,
    observer: Etcd,
    replies: Arc<Mutex<()>>,
    relay: JoinHandle<()>,
    _cluster: Cluster,
}

impl StoreReplyGate {
    async fn start() -> Self {
        let cluster = ClusterBuilder::builder()
            .with_rest(false)
            .with_jaeger(false)
            .with_io_engines(0)
            .with_agents(vec![])
            .build()
            .await
            .unwrap();
        let backend = "127.0.0.1:2379";
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let replies = Arc::new(Mutex::new(()));
        let reply_gate = replies.clone();
        let relay = tokio::spawn(async move {
            let (mut client, _) = listener.accept().await.unwrap();
            let mut server = TcpStream::connect(backend).await.unwrap();
            let (mut from_client, mut to_client) = client.split();
            let (from_server, mut to_server) = server.split();
            tokio::select! {
                result = tokio::io::copy(&mut from_client, &mut to_server) => { result.unwrap(); }
                () = async {
                    let mut replies = BufReader::new(from_server);
                    while !replies.fill_buf().await.unwrap().is_empty() {
                        let _open = reply_gate.lock().await;
                        to_client.write_all(replies.buffer()).await.unwrap();
                        replies.consume(replies.buffer().len());
                    }
                } => {}
            }
        });
        let args = CliArgs::parse_from([
            "core",
            "--store",
            &endpoint,
            "--store-timeout",
            "1s",
            "--no-volume-health",
        ]);
        let registry = crate::new_registry(args, Default::default()).await.unwrap();
        let observer = Etcd::new(backend).await.unwrap();
        Self {
            registry,
            observer,
            replies,
            relay,
            _cluster: cluster,
        }
    }

    /// Observe what etcd committed while its reply to the timed-out request is still blocked.
    async fn committed_after_timeout<O: StorableObject, T>(
        &self,
        key: &O::Key,
        request: impl Future<Output = Result<T, SvcError>>,
    ) -> O {
        let _held = self.replies.lock().await;
        assert!(matches!(
            request.await,
            Err(SvcError::Store {
                source: StoreError::Timeout { .. }
            })
        ));
        self.observer.clone().get_obj::<O>(key).await.unwrap().0
    }
}

impl Drop for StoreReplyGate {
    fn drop(&mut self) {
        self.relay.abort();
    }
}

#[tokio::test]
async fn retry_after_lost_store_reply_persists_original_endpoint() {
    let mut gate = StoreReplyGate::start().await;
    let original = RegisterAppNode::new(
        "csi-node-1".into(),
        "10.0.0.1:50052".parse().unwrap(),
        None,
        None,
    );
    let changed = RegisterAppNode {
        endpoint: "10.0.0.2:50052".parse().unwrap(),
        ..original.clone()
    };
    let key = AppNodeSpecKey::from(&original.id);
    gate.registry.register_app_node(&original).await.unwrap();

    let committed: AppNodeSpec = gate
        .committed_after_timeout(&key, gate.registry.register_app_node(&changed))
        .await;
    assert_eq!(committed.endpoint, changed.endpoint);

    gate.registry.register_app_node(&original).await.unwrap();
    let (persisted, revision) = gate.observer.get_obj::<AppNodeSpec>(&key).await.unwrap();
    assert_eq!(persisted.endpoint, original.endpoint);

    gate.registry.register_app_node(&original).await.unwrap();
    let (_, heartbeat_revision) = gate.observer.get_obj::<AppNodeSpec>(&key).await.unwrap();
    assert_eq!(
        heartbeat_revision, revision,
        "unchanged heartbeat wrote to etcd"
    );
}

fn io_engine_registration() -> Register {
    Register {
        id: "io-engine-1".into(),
        grpc_endpoint: "10.0.0.1:10124".parse().unwrap(),
        api_versions: None,
        instance_uuid: None,
        node_nqn: None,
        features: None,
        bugfixes: None,
        version: None,
        transport_caps: None,
        nvmf_target: None,
    }
}

#[tokio::test]
async fn register_old_endpoint_after_committed_write_times_out() {
    let mut gate = StoreReplyGate::start().await;
    let registry = &gate.registry;
    let specs = registry.specs();
    let original = io_engine_registration();
    let updated = Register {
        grpc_endpoint: "10.0.0.2:10124".parse().unwrap(),
        ..original.clone()
    };
    let key = NodeSpecKey::from(&original.id);
    specs.register_node(registry, &original).await.unwrap();

    let committed: NodeSpec = gate
        .committed_after_timeout(&key, specs.register_node(registry, &updated))
        .await;
    assert_eq!(committed.endpoint(), updated.grpc_endpoint);
    assert_eq!(
        specs.node(&original.id).unwrap().endpoint(),
        original.grpc_endpoint
    );

    specs.register_node(registry, &original).await.unwrap();
    let (restored, revision) = gate.observer.get_obj::<NodeSpec>(&key).await.unwrap();
    assert_eq!(restored.endpoint(), original.grpc_endpoint);
    assert!(!specs.register_node(registry, &original).await.unwrap());
    assert_eq!(
        gate.observer.get_obj::<NodeSpec>(&key).await.unwrap().1,
        revision
    );
}

#[tokio::test]
async fn deregister_during_drain() {
    let gate = StoreReplyGate::start().await;
    let registry = &gate.registry;
    let specs = registry.specs();
    let node = io_engine_registration();
    specs.register_node(registry, &node).await.unwrap();

    let mut drain = specs.guarded_node(&node.id).await.unwrap();
    drain.drain(registry, "maintenance".into()).await.unwrap();
    let request = Deregister {
        id: node.id.clone(),
    };
    let deregister = specs.deregister_node(registry, &request);
    tokio::pin!(deregister);
    timeout(Duration::from_secs(6), &mut deregister)
        .await
        .expect_err("deregistration must wait for the drain to release the guard");

    drain.set_drained(registry).await.unwrap();
    drop(drain);
    timeout(Duration::from_secs(5), deregister)
        .await
        .unwrap()
        .unwrap();
    let (stored, _) = registry
        .load_obj::<NodeSpec>(&NodeSpecKey::from(&node.id))
        .await
        .unwrap();
    assert!(stored.is_drained() && stored.has_drain_label("maintenance"));
    assert!(stored.is_shutdown() && specs.node(&node.id).unwrap().is_shutdown());
}

#[tokio::test]
async fn waiting_operation_rejects_node_recreated_during_purge() {
    let specs = ResourceSpecsLocked::new();
    let node = NodeSpec::new(
        "node-1".into(),
        "127.0.0.1:10124".parse().unwrap(),
        NodeLabels::new(),
        None,
        None,
        None,
        None,
        None,
        false,
    );
    let original = specs.write().nodes.insert(node.clone());
    let purge = original.operation_guard().unwrap();
    let waiting = specs.guarded_node(node.id());
    tokio::pin!(waiting);
    assert!(futures::poll!(&mut waiting).is_pending());

    specs.remove_node(node.id());
    specs.write().nodes.insert(node.clone());
    drop(purge);

    assert!(matches!(waiting.await, Err(SvcError::NodeNotFound { .. })));
    assert!(specs.guarded_node(node.id()).await.is_ok());
}
