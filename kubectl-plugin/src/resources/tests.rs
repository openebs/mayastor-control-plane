#[cfg(test)]
use crate::resources::utils::{print_table, CreateRows, GetHeaderRow, OutputFormat};
use gag::BufferRedirect;
use once_cell::sync::OnceCell;
use openapi::models::CreateVolumeBody;
use serde::ser;
use std::io::Read;
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

    cluster
        .rest_v00()
        .volumes_api()
        .put_volume(
            &Uuid::parse_str(VOLUME_UUID).unwrap(),
            CreateVolumeBody {
                policy: Default::default(),
                replicas: 1,
                size: 5242880,
                topology: None,
                labels: None,
            },
        )
        .await
        .unwrap();

    CLUSTER
        .set(cluster)
        .ok()
        .expect("Expect to be initialised only once");
}

async fn cluster() -> &'static Cluster {
    if CLUSTER.get().is_none() {
        setup().await;
    }
    CLUSTER.get().unwrap()
}

#[tokio::test]
async fn get_volumes() {
    let volumes = cluster()
        .await
        .rest_v00()
        .volumes_api()
        .get_volumes()
        .await
        .unwrap();
    let volume_state = volumes[0].state.clone();
    let volume_spec = volumes[0].spec.clone();
    compare(
        format!(" ID                                    REPLICAS  TARGET-NODE  ACCESSIBILITY  STATUS  SIZE \n {}  {}         {}       {}         {}  {} \n",
                volume_state.uuid, volume_spec.num_replicas, "<none>", "<none>", volume_state.status.to_string(), volume_state.size),
        volumes,
    );
}

#[tokio::test]
async fn get_volume() {
    let volume = cluster()
        .await
        .rest_v00()
        .volumes_api()
        .get_volume(&Uuid::parse_str(VOLUME_UUID).unwrap())
        .await
        .unwrap();
    let volume_state = volume.state.clone();
    let volume_spec = volume.spec.clone();
    compare(
        format!(" ID                                    REPLICAS  TARGET-NODE  ACCESSIBILITY  STATUS  SIZE \n {}  {}         {}       {}         {}  {} \n",
                volume_state.uuid, volume_spec.num_replicas, "<none>", "<none>", volume_state.status.to_string(), volume_state.size),
        volume,
    );
}

#[tokio::test]
async fn get_pools() {
    let pools = cluster()
        .await
        .rest_v00()
        .pools_api()
        .get_pools()
        .await
        .unwrap();
    let pool_state = pools[0].state.as_ref().unwrap().clone();
    let disks = pool_state.disks.join(", ");
    compare(
        format!(" ID                 TOTAL CAPACITY  USED CAPACITY  DISKS                                                                  NODE        STATUS  MANAGED \n {}  {}       {}        {}  {}  {}  {} \n",
                pool_state.id, pool_state.capacity, pool_state.used, disks, pool_state.node, pool_state.status.to_string(),true),
        pools
    );
}

#[tokio::test]
async fn get_pool() {
    let pool = cluster()
        .await
        .rest_v00()
        .pools_api()
        .get_pool(cluster().await.pool(0, 0).as_str())
        .await
        .unwrap();
    let pool_state = pool.state.as_ref().unwrap().clone();
    let disks = pool_state.disks.join(", ");
    compare(
        format!(" ID                 TOTAL CAPACITY  USED CAPACITY  DISKS                                                                  NODE        STATUS  MANAGED \n {}  {}       {}        {}  {}  {}  {} \n",
                pool_state.id, pool_state.capacity, pool_state.used, disks, pool_state.node, pool_state.status.to_string(),true),
        pool
    );
}

#[tokio::test]
async fn get_nodes() {
    let nodes = cluster()
        .await
        .rest_v00()
        .nodes_api()
        .get_nodes()
        .await
        .unwrap();
    let node_state = nodes[0].state.as_ref().unwrap().clone();
    compare(
        format!(
            " ID          GRPC ENDPOINT   STATUS \n {}  {}  {} \n",
            node_state.id,
            node_state.grpc_endpoint,
            node_state.status.to_string()
        ),
        nodes,
    );
}

#[tokio::test]
async fn get_node() {
    let node = cluster()
        .await
        .rest_v00()
        .nodes_api()
        .get_node(cluster().await.node(0).as_str())
        .await
        .unwrap();
    let node_state = node.state.as_ref().unwrap().clone();
    compare(
        format!(
            " ID          GRPC ENDPOINT   STATUS \n {}  {}  {} \n",
            node_state.id,
            node_state.grpc_endpoint,
            node_state.status.to_string()
        ),
        node,
    );
}

#[tokio::test]
async fn get_replica_topology() {
    let replica_topo = cluster()
        .await
        .rest_v00()
        .volumes_api()
        .get_volume(&Uuid::parse_str(VOLUME_UUID).unwrap())
        .await
        .unwrap()
        .state
        .replica_topology;
    let replica_ids: Vec<String> = replica_topo.clone().into_keys().collect();
    let replica = replica_topo.get(&*replica_ids[0]).unwrap();
    compare(format!(" ID                                    NODE        POOL               STATUS \n {}  {}  {}  {} \n",
                    replica_ids[0], replica.node.as_ref().unwrap(), replica.pool.as_ref().unwrap(), replica.state.to_string()),
            replica_topo
    );
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
