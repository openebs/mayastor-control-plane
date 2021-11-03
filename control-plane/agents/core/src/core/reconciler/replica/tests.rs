use common_lib::{
    store::etcd::Etcd,
    types::v0::{
        message_bus::{NexusId, ReplicaId, ReplicaOwners, VolumeId},
        openapi::models::CreateReplicaBody,
        store::{
            definitions::Store,
            replica::{ReplicaSpec, ReplicaSpecKey},
        },
    },
};
use std::{thread::sleep, time::Duration};
use testlib::ClusterBuilder;

#[tokio::test]
async fn test_disown_missing_owners() {
    let cluster = ClusterBuilder::builder()
        .with_rest(true)
        .with_agents(vec!["core"])
        .with_mayastors(1)
        .with_pools(1)
        .with_cache_period("1s")
        .with_reconcile_period(Duration::from_secs(1), Duration::from_secs(1))
        .build()
        .await
        .unwrap();

    // Create a replica. This will save the replica spec to the persistent store.
    let replica_id = ReplicaId::new();
    cluster
        .rest_v00()
        .replicas_api()
        .put_pool_replica(
            "mayastor-1-pool-1",
            &replica_id,
            CreateReplicaBody {
                share: None,
                size: 5242880,
                thin: false,
            },
        )
        .await
        .expect("Failed to create replica.");

    // Check the replica exists.
    let num_replicas = cluster
        .rest_v00()
        .replicas_api()
        .get_replicas()
        .await
        .expect("Failed to get replicas.")
        .len();
    assert_eq!(num_replicas, 1);

    // Modify the replica spec in the store so that the replica has a volume and nexus owner;
    // neither of which exist.
    let mut etcd = Etcd::new("0.0.0.0:2379").await.unwrap();
    let mut replica: ReplicaSpec = etcd
        .get_obj(&ReplicaSpecKey::from(&replica_id))
        .await
        .unwrap();
    replica.managed = true;
    replica.owners = ReplicaOwners::new(Some(VolumeId::new()), vec![NexusId::new()]);

    // Persist the modified replica spec to the store
    etcd.put_obj(&replica)
        .await
        .expect("Failed to store modified replica.");

    // Restart the core agent so that it reloads the modified replica spec from the persistent
    // store.
    cluster.restart_core().await;

    // Allow time for the core agent to restart.
    sleep(Duration::from_secs(2));

    // The replica should be removed because none of its parents exist.
    let num_replicas = cluster
        .rest_v00()
        .replicas_api()
        .get_replicas()
        .await
        .expect("Failed to get replicas.")
        .len();
    assert_eq!(num_replicas, 0);
}
