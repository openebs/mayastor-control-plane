use deployer_cluster::{Cluster, ClusterBuilder};
use events_api::{
    event::{EventAction, EventCategory, EventMessage},
    mbus_nats::{message_bus_init, BusSubscription},
    Bus,
};
use grpc::operations::{pool::traits::PoolOperations, volume::traits::VolumeOperations};
use std::time::Duration;
use stor_port::types::v0::transport::{
    CreatePool, CreateVolume, DestroyPool, DestroyVolume, VolumeId,
};
use tokio::time::timeout;

#[tokio::test]
async fn events() {
    let cluster = ClusterBuilder::builder()
        .with_rest(false)
        .with_agents(vec!["core"])
        .with_eventing(true)
        .build()
        .await
        .unwrap();

    let mut events_mbus_subscription =
        timeout(Duration::from_millis(10), mbus_init("localhost:4222"))
            .await
            .unwrap();

    let pool_id = "pool1";
    let volume_uuid: VolumeId = "1e3cf927-80c2-47a8-adf0-95c486bdd7b7".try_into().unwrap();

    pool_creation_event_test(&cluster, &mut events_mbus_subscription, pool_id).await;
    volume_creation_event_test(&cluster, &mut events_mbus_subscription, &volume_uuid).await;
    volume_deletion_event_test(&cluster, &mut events_mbus_subscription, &volume_uuid).await;
    pool_deletion_event_test(&cluster, &mut events_mbus_subscription, pool_id).await;
}

async fn pool_creation_event_test(
    cluster: &Cluster,
    sub: &mut BusSubscription<EventMessage>,
    pool_id: &str,
) {
    let io_engine = cluster.node(0);
    let pool_client = cluster.grpc_client().pool();

    let _pool = pool_client
        .create(
            &CreatePool {
                node: io_engine.clone(),
                id: pool_id.into(),
                disks: vec!["malloc:///disk0?size_mb=100".into()],
                labels: None,
            },
            None,
        )
        .await
        .unwrap();

    let pool_creation_message = timeout(Duration::from_millis(10), sub.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pool_creation_message.category(), EventCategory::Pool);
    assert_eq!(pool_creation_message.action(), EventAction::Create);
}

async fn pool_deletion_event_test(
    cluster: &Cluster,
    sub: &mut BusSubscription<EventMessage>,
    pool_id: &str,
) {
    let io_engine = cluster.node(0);
    let pool_client = cluster.grpc_client().pool();

    pool_client
        .destroy(
            &DestroyPool {
                node: io_engine.clone(),
                id: pool_id.into(),
            },
            None,
        )
        .await
        .unwrap();

    let pool_deletion_message = timeout(Duration::from_millis(10), sub.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pool_deletion_message.category(), EventCategory::Pool);
    assert_eq!(pool_deletion_message.action(), EventAction::Delete);
}

async fn volume_creation_event_test(
    cluster: &Cluster,
    sub: &mut BusSubscription<EventMessage>,
    volume_uuid: &VolumeId,
) {
    let volume_client = cluster.grpc_client().volume();

    let _volume = volume_client
        .create(
            &CreateVolume {
                uuid: volume_uuid.clone(),
                size: 60 * 1024 * 1024,
                replicas: 1,
                thin: false,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    let vol_creation_message = timeout(Duration::from_millis(10), sub.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(vol_creation_message.category(), EventCategory::Volume);
    assert_eq!(vol_creation_message.action(), EventAction::Create);
}

async fn volume_deletion_event_test(
    cluster: &Cluster,
    sub: &mut BusSubscription<EventMessage>,
    volume_uuid: &VolumeId,
) {
    let volume_client = cluster.grpc_client().volume();

    volume_client
        .destroy(
            &DestroyVolume {
                uuid: volume_uuid.clone(),
            },
            None,
        )
        .await
        .expect("Should be able to destroy the volume");

    let vol_deletion_message = timeout(Duration::from_millis(10), sub.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(vol_deletion_message.category(), EventCategory::Volume);
    assert_eq!(vol_deletion_message.action(), EventAction::Delete);
}

async fn mbus_init(mbus_url: &str) -> BusSubscription<EventMessage> {
    let mut bus = message_bus_init(mbus_url, Some(1)).await;
    bus.subscribe::<EventMessage>().await.unwrap()
}
