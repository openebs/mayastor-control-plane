pub(crate) mod registry;
mod service;
pub mod specs;

use std::{convert::TryInto, marker::PhantomData};

use super::{core::registry::Registry, handler, impl_request_handler};
use async_trait::async_trait;
use common::errors::SvcError;
use mbus_api::{v0::*, *};

pub(crate) fn configure(builder: common::Service) -> common::Service {
    let registry = builder.get_shared_state::<Registry>().clone();
    builder
        .with_channel(ChannelVs::Volume)
        .with_default_liveness()
        .with_shared_state(service::Service::new(registry))
        .with_subscription(handler!(GetVolumes))
        .with_subscription(handler!(CreateVolume))
        .with_subscription(handler!(DestroyVolume))
        .with_channel(ChannelVs::Nexus)
        .with_subscription(handler!(GetNexuses))
        .with_subscription(handler!(CreateNexus))
        .with_subscription(handler!(DestroyNexus))
        .with_subscription(handler!(ShareNexus))
        .with_subscription(handler!(UnshareNexus))
        .with_subscription(handler!(AddNexusChild))
        .with_subscription(handler!(RemoveNexusChild))
}

#[cfg(test)]
mod tests {
    use super::*;
    use testlib::ClusterBuilder;

    #[actix_rt::test]
    async fn volume() {
        let cluster = ClusterBuilder::builder()
            .with_rest(false)
            .with_agents(vec!["core"])
            .with_mayastors(2)
            .build()
            .await
            .unwrap();

        let mayastor = cluster.node(0).to_string();
        let mayastor2 = cluster.node(1).to_string();

        let nodes = GetNodes {}.request().await.unwrap();
        tracing::info!("Nodes: {:?}", nodes);

        prepare_pools(&mayastor, &mayastor2).await;
        test_nexus(&mayastor, &mayastor2).await;
        test_volume().await;

        assert!(GetNexuses::default().request().await.unwrap().0.is_empty());
    }

    async fn prepare_pools(mayastor: &str, mayastor2: &str) {
        CreatePool {
            node: mayastor.into(),
            id: "pooloop".into(),
            disks: vec!["malloc:///disk0?size_mb=100".into()],
        }
        .request()
        .await
        .unwrap();

        CreatePool {
            node: mayastor2.into(),
            id: "pooloop2".into(),
            disks: vec!["malloc:///disk0?size_mb=100".into()],
        }
        .request()
        .await
        .unwrap();

        let pools = GetPools::default().request().await.unwrap();
        tracing::info!("Pools: {:?}", pools);
    }

    async fn test_nexus(mayastor: &str, mayastor2: &str) {
        let replica = CreateReplica {
            node: mayastor2.into(),
            uuid: "replica".into(),
            pool: "pooloop2".into(),
            size: 12582912, /* actual size will be a multiple of 4MB so just
                             * create it like so */
            thin: true,
            share: Protocol::Nvmf,
            ..Default::default()
        }
        .request()
        .await
        .unwrap();

        let local = "malloc:///local?size_mb=12".into();

        let nexus = CreateNexus {
            node: mayastor.into(),
            uuid: "f086f12c-1728-449e-be32-9415051090d6".into(),
            size: 5242880,
            children: vec![replica.uri.into(), local],
            ..Default::default()
        }
        .request()
        .await
        .unwrap();

        let nexuses = GetNexuses::default().request().await.unwrap().0;
        tracing::info!("Nexuses: {:?}", nexuses);
        assert_eq!(Some(&nexus), nexuses.first());

        ShareNexus {
            node: mayastor.into(),
            uuid: "f086f12c-1728-449e-be32-9415051090d6".into(),
            key: None,
            protocol: NexusShareProtocol::Nvmf,
        }
        .request()
        .await
        .unwrap();

        DestroyNexus {
            node: mayastor.into(),
            uuid: "f086f12c-1728-449e-be32-9415051090d6".into(),
        }
        .request()
        .await
        .unwrap();

        DestroyReplica {
            node: replica.node,
            pool: replica.pool,
            uuid: replica.uuid,
        }
        .request()
        .await
        .unwrap();

        assert!(GetNexuses::default().request().await.unwrap().0.is_empty());
    }

    async fn test_volume() {
        let volume = CreateVolume {
            uuid: "359b7e1a-b724-443b-98b4-e6d97fabbb40".into(),
            size: 5242880,
            nexuses: 1,
            replicas: 2,
            allowed_nodes: vec![],
            preferred_nodes: vec![],
            preferred_nexus_nodes: vec![],
        };

        let volume = volume.request().await.unwrap();
        let volumes = GetVolumes::default().request().await.unwrap().0;
        tracing::info!("Volumes: {:?}", volumes);

        assert_eq!(Some(&volume), volumes.first());

        DestroyVolume {
            uuid: "359b7e1a-b724-443b-98b4-e6d97fabbb40".into(),
        }
        .request()
        .await
        .unwrap();

        assert!(GetVolumes::default().request().await.unwrap().0.is_empty());
        assert!(GetNexuses::default().request().await.unwrap().0.is_empty());
        assert!(GetReplicas::default().request().await.unwrap().0.is_empty());
    }
}
