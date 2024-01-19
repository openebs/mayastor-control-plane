#![cfg(test)]

use deployer_cluster::ClusterBuilder;
use grpc::operations::volume::traits::VolumeOperations;
use std::time::Duration;
use stor_port::{
    transport_api::ReplyErrorKind,
    types::v0::transport::{CreateVolume, DestroyVolume},
};
use uuid::Uuid;

const SIZE: u64 = 5242880;
const EXCESS: u64 = 1000000;

#[tokio::test]
async fn volume_create_with_capacity_limit() {
    let cache_period = Duration::from_millis(250);
    let reconcile_period = Duration::from_millis(3000);
    let cluster = ClusterBuilder::builder()
        .with_rest(false)
        .with_io_engines(1)
        .with_pool(0, "malloc:///p1?size_mb=300")
        .with_cache_period(&humantime::Duration::from(cache_period).to_string())
        .with_reconcile_period(reconcile_period, reconcile_period)
        .build()
        .await
        .unwrap();

    let volume_client = cluster.grpc_client().volume();

    // test with no capacity limit
    grpc_create_volume_with_limit(&volume_client, SIZE, None, None).await;

    // test exceeding the capacity limit
    grpc_create_volume_with_limit(
        &volume_client,
        SIZE,
        Some(SIZE - EXCESS), // capacity smaller than volume
        Some(ReplyErrorKind::CapacityLimitExceeded {}),
    )
    .await;

    // test at the capacity limit
    grpc_create_volume_with_limit(&volume_client, SIZE, Some(SIZE), None).await;

    // test below the capacity limit
    grpc_create_volume_with_limit(&volume_client, SIZE, Some(SIZE + EXCESS), None).await;
}

async fn grpc_create_volume_with_limit(
    volume_client: &dyn VolumeOperations,
    size: u64,
    capacity: Option<u64>,
    expected_error: Option<ReplyErrorKind>,
) {
    let vol_uuid = Uuid::new_v4();

    let volume = volume_client
        .create(
            &CreateVolume {
                uuid: vol_uuid.try_into().unwrap(),
                size,
                replicas: 1,
                cluster_capacity_limit: capacity,
                ..Default::default()
            },
            None,
        )
        .await;
    match volume {
        Ok(_) => {
            volume_client
                .destroy(
                    &DestroyVolume {
                        uuid: vol_uuid.try_into().unwrap(),
                    },
                    None,
                )
                .await
                .unwrap();
            assert!(expected_error.is_none());
        }
        Err(e) => {
            assert_eq!(expected_error, Some(e.kind)); // wrong error
        }
    }
}
