#[cfg(test)]
use crate::resources::utils::{print_table, CreateRows, GetHeaderRow, OutputFormat};
use common_lib::{mbus_api::Message, types::v0::message_bus::CreateVolume};
use gag::BufferRedirect;
use once_cell::sync::OnceCell;
use serde::ser;
use std::{convert::TryInto, io::Read};
use testlib::{Cluster, ClusterBuilder, Uuid};

static CLUSTER: OnceCell<Cluster> = OnceCell::new();
const VOLUME_UUID: &str = "1e3cf927-80c2-47a8-adf0-95c486bdd7b7";

async fn setup() {
    let cluster = ClusterBuilder::builder()
        .with_rest(true)
        .with_agents(vec!["core"])
        .with_mayastors(1)
        .with_pools(1)
        .build()
        .await
        .unwrap();

    CreateVolume {
        uuid: VOLUME_UUID.try_into().unwrap(),
        size: 5242880,
        replicas: 1,
        ..Default::default()
    }
    .request()
    .await
    .unwrap();

    CLUSTER
        .set(cluster)
        .ok()
        .expect("Expect to be initialised only once");
}

#[tokio::test]
async fn get_volumes() {
    if CLUSTER.get().is_none() {
        setup().await;
    }
    let volumes = CLUSTER
        .get()
        .unwrap()
        .rest_v00()
        .volumes_api()
        .get_volumes()
        .await
        .unwrap();
    compare(format!(" ID                                    REPLICAS  TARGET-NODE  ACCESSIBILITY  STATUS  SIZE \n {}  1         <none>       <none>         Online  5242880 \n", VOLUME_UUID), volumes);
}

#[tokio::test]
async fn get_volume() {
    if CLUSTER.get().is_none() {
        setup().await;
    }
    let volume = CLUSTER
        .get()
        .unwrap()
        .rest_v00()
        .volumes_api()
        .get_volume(&Uuid::parse_str(VOLUME_UUID).unwrap())
        .await
        .unwrap();
    compare(format!(" ID                                    REPLICAS  TARGET-NODE  ACCESSIBILITY  STATUS  SIZE \n {}  1         <none>       <none>         Online  5242880 \n", VOLUME_UUID), volume);
}

#[tokio::test]
async fn get_pools() {
    if CLUSTER.get().is_none() {
        setup().await;
    }
    let pools = CLUSTER
        .get()
        .unwrap()
        .rest_v00()
        .pools_api()
        .get_pools()
        .await
        .unwrap();
    compare(format!(" ID                 TOTAL CAPACITY  USED CAPACITY  DISKS                                                                  NODE        STATUS  MANAGED \n mayastor-1-pool-1  100663296       8388608        {}  mayastor-1  Online  true \n", pools[0].state.as_ref().unwrap().disks[0].clone()), pools);
}

#[tokio::test]
async fn get_pool() {
    if CLUSTER.get().is_none() {
        setup().await;
    }
    let pool = CLUSTER
        .get()
        .unwrap()
        .rest_v00()
        .pools_api()
        .get_pool(CLUSTER.get().unwrap().pool(0, 0).as_str())
        .await
        .unwrap();
    compare(format!(" ID                 TOTAL CAPACITY  USED CAPACITY  DISKS                                                                  NODE        STATUS  MANAGED \n mayastor-1-pool-1  100663296       8388608        {}  mayastor-1  Online  true \n", pool.state.as_ref().unwrap().disks[0].clone()), pool);
}

#[tokio::test]
async fn get_nodes() {
    if CLUSTER.get().is_none() {
        setup().await;
    }
    let nodes = CLUSTER
        .get()
        .unwrap()
        .rest_v00()
        .nodes_api()
        .get_nodes()
        .await
        .unwrap();
    compare(
        format!(
            " ID          GRPC ENDPOINT   STATUS \n mayastor-1  {}  Online \n",
            nodes[0].state.as_ref().unwrap().grpc_endpoint
        ),
        nodes,
    );
}

#[tokio::test]
async fn get_node() {
    if CLUSTER.get().is_none() {
        setup().await;
    }
    let node = CLUSTER
        .get()
        .unwrap()
        .rest_v00()
        .nodes_api()
        .get_node(CLUSTER.get().unwrap().node(0).as_str())
        .await
        .unwrap();
    compare(
        format!(
            " ID          GRPC ENDPOINT   STATUS \n mayastor-1  {}  Online \n",
            node.state.as_ref().unwrap().grpc_endpoint
        ),
        node,
    );
}

#[tokio::test]
async fn get_replica_topology() {
    if CLUSTER.get().is_none() {
        setup().await;
    }
    let replica_topo = CLUSTER
        .get()
        .unwrap()
        .rest_v00()
        .volumes_api()
        .get_volume(&Uuid::parse_str(VOLUME_UUID).unwrap())
        .await
        .unwrap()
        .state
        .replica_topology;
    let replica_ids: Vec<String> = replica_topo.clone().into_keys().collect();
    compare(format!(" ID                                    NODE        POOL               STATUS \n {}  mayastor-1  mayastor-1-pool-1  Online \n", replica_ids[0]), replica_topo);
}

// Compares the print_table output redirected to buffer with the expected string
fn compare<T>(expected_output: String, obj: T)
where
    T: ser::Serialize,
    T: CreateRows,
    T: GetHeaderRow,
{
    let mut buf = BufferRedirect::stdout().unwrap();
    print_table(&OutputFormat::NoFormat, obj);
    let mut actual_output = String::new();
    buf.read_to_string(&mut actual_output).unwrap();
    assert_eq!(&actual_output[..], expected_output);
}
