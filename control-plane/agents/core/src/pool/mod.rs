mod registry;
pub mod service;
pub mod specs;

use std::{convert::TryInto, marker::PhantomData};

use super::{core::registry::Registry, handler, impl_request_handler};
use async_trait::async_trait;
use common::{errors::SvcError, Service};
use mbus_api::{
    v0::{
        ChannelVs,
        CreatePool,
        CreateReplica,
        DestroyPool,
        DestroyReplica,
        GetPools,
        GetReplicas,
        ShareReplica,
        UnshareReplica,
    },
    Message,
    MessageId,
    ReceivedMessage,
};

pub(crate) fn configure(builder: Service) -> Service {
    let registry = builder.get_shared_state::<Registry>().clone();
    builder
        .with_channel(ChannelVs::Pool)
        .with_default_liveness()
        .with_shared_state(service::Service::new(registry))
        .with_subscription(handler!(GetPools))
        .with_subscription(handler!(CreatePool))
        .with_subscription(handler!(DestroyPool))
        .with_subscription(handler!(GetReplicas))
        .with_subscription(handler!(CreateReplica))
        .with_subscription(handler!(DestroyReplica))
        .with_subscription(handler!(ShareReplica))
        .with_subscription(handler!(UnshareReplica))
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::v0::GetSpecs;
    use mbus_api::{
        v0::{GetNodes, Protocol, Replica, ReplicaShareProtocol, ReplicaState},
        TimeoutOptions,
    };
    use std::time::Duration;
    use store::types::v0::replica::ReplicaSpec;
    use testlib::{v0::ReplicaId, ClusterBuilder};

    #[actix_rt::test]
    async fn pool() {
        let cluster = ClusterBuilder::builder()
            .with_rest(false)
            .with_agents(vec!["core"])
            .build()
            .await
            .unwrap();
        let mayastor = cluster.node(0);

        let nodes = GetNodes {}.request().await.unwrap();
        tracing::info!("Nodes: {:?}", nodes);

        CreatePool {
            node: mayastor.clone(),
            id: "pooloop".into(),
            disks: vec!["malloc:///disk0?size_mb=100".into()],
        }
        .request()
        .await
        .unwrap();

        let pools = GetPools::default().request().await.unwrap();
        tracing::info!("Pools: {:?}", pools);

        let replica = CreateReplica {
            node: mayastor.clone(),
            uuid: "replica1".into(),
            pool: "pooloop".into(),
            size: 12582912, /* actual size will be a multiple of 4MB so just
                             * create it like so */
            thin: true,
            share: Protocol::Off,
            ..Default::default()
        }
        .request()
        .await
        .unwrap();

        let replicas = GetReplicas::default().request().await.unwrap();
        tracing::info!("Replicas: {:?}", replicas);

        assert_eq!(
            replica,
            Replica {
                node: mayastor.clone(),
                uuid: "replica1".into(),
                pool: "pooloop".into(),
                thin: false,
                size: 12582912,
                share: Protocol::Off,
                uri: "bdev:///replica1".into(),
                state: ReplicaState::Online
            }
        );

        let uri = ShareReplica {
            node: mayastor.clone(),
            uuid: "replica1".into(),
            pool: "pooloop".into(),
            protocol: ReplicaShareProtocol::Nvmf,
        }
        .request()
        .await
        .unwrap();

        let mut replica_updated = replica;
        replica_updated.uri = uri;
        replica_updated.share = Protocol::Nvmf;
        let replica = GetReplicas::default().request().await.unwrap();
        let replica = replica.0.first().unwrap();
        assert_eq!(replica, &replica_updated);

        DestroyReplica {
            node: mayastor.clone(),
            uuid: "replica1".into(),
            pool: "pooloop".into(),
        }
        .request()
        .await
        .unwrap();

        assert!(GetReplicas::default().request().await.unwrap().0.is_empty());

        DestroyPool {
            node: mayastor.clone(),
            id: "pooloop".into(),
        }
        .request()
        .await
        .unwrap();

        assert!(GetPools::default().request().await.unwrap().0.is_empty());
    }

    /// default timeout options for every bus request
    fn bus_timeout_opts() -> TimeoutOptions {
        TimeoutOptions::default()
            .with_max_retries(0)
            .with_timeout(Duration::from_millis(250))
    }

    /// Get the replica spec
    async fn replica_spec(replica: &Replica) -> Option<ReplicaSpec> {
        GetSpecs {}
            .request()
            .await
            .unwrap()
            .replicas
            .iter()
            .find(|r| r.uuid == replica.uuid)
            .cloned()
    }

    #[actix_rt::test]
    async fn replica_transaction() {
        let cluster = ClusterBuilder::builder()
            .with_rest(false)
            .with_pools(1)
            .with_agents(vec!["core"])
            .with_node_timeouts(Duration::from_millis(250), Duration::from_millis(500))
            .with_bus_timeouts(bus_timeout_opts())
            .build()
            .await
            .unwrap();
        let mayastor = cluster.node(0);

        let nodes = GetNodes {}.request().await.unwrap();
        tracing::info!("Nodes: {:?}", nodes);

        let pools = GetPools::default().request().await.unwrap();
        tracing::info!("Pools: {:?}", pools);

        let replica = CreateReplica {
            node: mayastor.clone(),
            uuid: ReplicaId::new(),
            pool: cluster.pool(0, 0),
            size: 12582912,
            thin: false,
            share: Protocol::Off,
            ..Default::default()
        }
        .request()
        .await
        .unwrap();

        async fn check_operation(replica: &Replica, protocol: Protocol) {
            // operation in progress
            assert!(replica_spec(&replica).await.unwrap().operation.is_some());
            tokio::time::delay_for(std::time::Duration::from_millis(500)).await;
            // operation is completed
            assert!(replica_spec(&replica).await.unwrap().operation.is_none());
            assert_eq!(replica_spec(&replica).await.unwrap().share, protocol);
        }

        // pause mayastor
        cluster.composer().pause(mayastor.as_str()).await.unwrap();

        ShareReplica::from(&replica)
            .request_ext(bus_timeout_opts())
            .await
            .expect_err("mayastor down");

        check_operation(&replica, Protocol::Off).await;

        // unpause mayastor
        cluster.composer().thaw(mayastor.as_str()).await.unwrap();

        // now it should be shared successfully
        let uri = ShareReplica::from(&replica).request().await.unwrap();
        println!("Share uri: {}", uri);

        cluster.composer().pause(mayastor.as_str()).await.unwrap();

        UnshareReplica::from(&replica)
            .request_ext(bus_timeout_opts())
            .await
            .expect_err("mayastor down");

        check_operation(&replica, Protocol::Nvmf).await;

        cluster.composer().thaw(mayastor.as_str()).await.unwrap();

        UnshareReplica::from(&replica).request().await.unwrap();

        assert_eq!(replica_spec(&replica).await.unwrap().share, Protocol::Off);
    }

    #[actix_rt::test]
    async fn replica_transaction_store() {
        let store_timeout = Duration::from_millis(250);
        let reconcile_period = Duration::from_millis(250);
        let cluster = ClusterBuilder::builder()
            .with_rest(false)
            .with_pools(1)
            .with_agents(vec!["core"])
            .with_node_timeouts(Duration::from_millis(500), Duration::from_millis(500))
            .with_reconcile_period(reconcile_period, reconcile_period)
            .with_store_timeout(store_timeout)
            .with_bus_timeouts(bus_timeout_opts())
            .build()
            .await
            .unwrap();
        let mayastor = cluster.node(0);

        let replica = CreateReplica {
            node: mayastor.clone(),
            uuid: ReplicaId::new(),
            pool: cluster.pool(0, 0),
            size: 12582912,
            thin: false,
            share: Protocol::Off,
            ..Default::default()
        }
        .request()
        .await
        .unwrap();

        // pause mayastor
        cluster.composer().pause(mayastor.as_str()).await.unwrap();

        ShareReplica::from(&replica)
            .request_ext(bus_timeout_opts())
            .await
            .expect_err("mayastor down");

        // ensure the share will succeed but etcd store will fail
        // by pausing etcd and releasing mayastor
        cluster.composer().pause("etcd").await.unwrap();
        cluster.composer().thaw(mayastor.as_str()).await.unwrap();

        // hopefully we have enough time before the store times outs
        let spec = replica_spec(&replica).await.unwrap();
        assert!(spec.operation.unwrap().result.is_none());

        // let the store write time out
        tokio::time::delay_for(store_timeout * 2).await;

        // and now we have a result but the operation is still pending until
        // we can sync the spec
        let spec = replica_spec(&replica).await.unwrap();
        assert!(spec.operation.unwrap().result.is_some());

        // thaw etcd allowing the worker thread to sync the "dirty" spec
        cluster.composer().thaw("etcd").await.unwrap();

        // wait for the reconciler to do its thing
        tokio::time::delay_for(reconcile_period * 2).await;

        // and now we've sync and the pending operation is no more
        let spec = replica_spec(&replica).await.unwrap();
        assert!(spec.operation.is_none() && spec.share == Protocol::Nvmf);

        ShareReplica::from(&replica)
            .request_ext(bus_timeout_opts())
            .await
            .expect_err("already shared via nvmf");
    }
}
