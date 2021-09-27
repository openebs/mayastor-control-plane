#![feature(allow_fail)]
use testlib::*;

#[actix_rt::test]
async fn create_pool_malloc() {
    let cluster = ClusterBuilder::builder().build().await.unwrap();
    cluster
        .rest_v00()
        .pools_api()
        .put_node_pool(
            cluster.node(0).as_str(),
            cluster.pool(0, 0).as_str(),
            models::CreatePoolBody::new(vec!["malloc:///disk?size_mb=100"]),
        )
        .await
        .unwrap();
}

#[actix_rt::test]
async fn create_pool_with_missing_disk() {
    let cluster = ClusterBuilder::builder().build().await.unwrap();

    cluster
        .rest_v00()
        .pools_api()
        .put_node_pool(
            cluster.node(0).as_str(),
            cluster.pool(0, 0).as_str(),
            models::CreatePoolBody::new(vec!["/dev/c/3po"]),
        )
        .await
        .expect_err("Device should not exist");
}

#[actix_rt::test]
async fn create_pool_with_existing_disk() {
    let cluster = ClusterBuilder::builder().build().await.unwrap();

    cluster
        .rest_v00()
        .pools_api()
        .put_node_pool(
            cluster.node(0).as_str(),
            cluster.pool(0, 0).as_str(),
            models::CreatePoolBody::new(vec!["malloc:///disk?size_mb=100"]),
        )
        .await
        .unwrap();

    cluster
        .rest_v00()
        .pools_api()
        .put_node_pool(
            cluster.node(0).as_str(),
            cluster.pool(0, 0).as_str(),
            models::CreatePoolBody::new(vec!["malloc:///disk?size_mb=100"]),
        )
        .await
        .expect_err("Disk should be used by another pool");

    cluster
        .rest_v00()
        .pools_api()
        .del_pool(cluster.pool(0, 0).as_str())
        .await
        .unwrap();

    cluster
        .rest_v00()
        .pools_api()
        .put_node_pool(
            cluster.node(0).as_str(),
            cluster.pool(0, 0).as_str(),
            models::CreatePoolBody::new(vec!["malloc:///disk?size_mb=100"]),
        )
        .await
        .expect("Should now be able to create the new pool");
}

#[actix_rt::test]
async fn create_pool_idempotent() {
    let cluster = ClusterBuilder::builder().build().await.unwrap();

    v0::CreatePool {
        node: cluster.node(0),
        id: cluster.pool(0, 0),
        disks: vec!["malloc:///disk?size_mb=100".into()],
        labels: None,
    }
    .request()
    .await
    .unwrap();

    v0::CreatePool {
        node: cluster.node(0),
        id: cluster.pool(0, 0),
        disks: vec!["malloc:///disk?size_mb=100".into()],
        labels: None,
    }
    .request()
    .await
    .expect_err("already exists");
}

/// FIXME: CAS-710
#[actix_rt::test]
#[allow_fail]
async fn create_pool_idempotent_same_disk_different_query() {
    let cluster = ClusterBuilder::builder()
        // don't log whilst we have the allow_fail
        .compose_build(|c| c.with_logs(false))
        .await
        .unwrap();

    v0::CreatePool {
        node: cluster.node(0),
        id: cluster.pool(0, 0),
        disks: vec!["malloc:///disk?size_mb=100&blk_size=512".into()],
        labels: None,
    }
    .request()
    .await
    .unwrap();

    v0::CreatePool {
        node: cluster.node(0),
        id: cluster.pool(0, 0),
        disks: vec!["malloc:///disk?size_mb=200&blk_size=4096".into()],
        labels: None,
    }
    .request()
    .await
    .expect_err("Different query not allowed!");
}

#[actix_rt::test]
async fn create_pool_idempotent_different_nvmf_host() {
    let cluster = ClusterBuilder::builder()
        .with_options(|opts| opts.with_mayastors(3))
        .build()
        .await
        .unwrap();

    v0::CreatePool {
        node: cluster.node(1),
        id: cluster.pool(1, 0),
        disks: vec!["malloc:///disk?size_mb=100".into()],
        labels: None,
    }
    .request()
    .await
    .unwrap();

    v0::CreatePool {
        node: cluster.node(2),
        id: cluster.pool(2, 0),
        disks: vec!["malloc:///disk?size_mb=100".into()],
        labels: None,
    }
    .request()
    .await
    .unwrap();

    v0::CreatePool {
        node: cluster.node(2),
        id: cluster.pool(2, 0),
        disks: vec!["malloc:///disk?size_mb=100".into()],
        labels: None,
    }
    .request()
    .await
    .expect_err("Pool Already exists!");

    v0::CreatePool {
        node: cluster.node(2),
        id: cluster.pool(2, 0),
        disks: vec!["malloc:///disk?size_mb=100".into()],
        labels: None,
    }
    .request()
    .await
    .expect_err("Pool disk already used by another pool!");
}
