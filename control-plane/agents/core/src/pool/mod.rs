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
    use mbus_api::v0::{
        GetNodes,
        Protocol,
        Replica,
        ReplicaShareProtocol,
        ReplicaState,
    };
    use testlib::ClusterBuilder;

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
}
