use composer::{Binary, Builder, ContainerSpec};
use oneshot::Receiver;
use pstor::{etcd::Etcd, Store, StoreKv, WatchEvent};
use serde::{Deserialize, Serialize};
use std::{
    io,
    net::{SocketAddr, TcpStream},
    str::FromStr,
    time::Duration,
};
use tokio::task::JoinHandle;

static ETCD_ENDPOINT: &str = "0.0.0.0:2379";

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct TestStruct {
    name: String,
    value: u64,
    msg: String,
}

#[tokio::test]
async fn etcd() {
    let _test = Builder::new()
        .name("etcd")
        .add_container_spec(
            ContainerSpec::from_binary(
                "etcd",
                Binary::from_path("etcd").with_args(vec![
                    "--data-dir",
                    "/tmp/etcd-data",
                    "--advertise-client-urls",
                    "http://0.0.0.0:2379",
                    "--listen-client-urls",
                    "http://0.0.0.0:2379",
                ]),
            )
            .with_portmap("2379", "2379")
            .with_portmap("2380", "2380"),
        )
        .build()
        .await
        .unwrap();

    assert!(wait_for_etcd_ready(ETCD_ENDPOINT).is_ok(), "etcd not ready");

    let mut store = Etcd::new(ETCD_ENDPOINT)
        .await
        .expect("Failed to connect to etcd.");

    let key = serde_json::json!("key");
    let mut data = TestStruct {
        name: "John Doe".to_string(),
        value: 100,
        msg: "Hello etcd".to_string(),
    };

    // Add an entry to the store, read it back and make sure it is correct.
    store
        .put_kv(&key.to_string(), &serde_json::json!(&data))
        .await
        .expect("Failed to 'put' to etcd");
    let v = store.get_kv(&key).await.expect("Failed to 'get' from etcd");
    let result: TestStruct = serde_json::from_value(v).expect("Failed to deserialise value");
    assert_eq!(data, result);

    // Start a watch which should send a message when the subsequent 'put'
    // event occurs.
    let (put_hdl, r) = spawn_watch(&key, &mut store).await;

    // Modify entry.
    data.value = 200;
    store
        .put_kv(&key.to_string(), &serde_json::json!(&data))
        .await
        .expect("Failed to 'put' to etcd");

    // Wait up to 1 second for the watch to see the put event.
    let msg = r
        .recv_timeout(Duration::from_secs(1))
        .expect("Timed out waiting for message");
    let result: TestStruct = match msg {
        WatchEvent::Put(_k, v) => serde_json::from_value(v).expect("Failed to deserialise value"),
        _ => panic!("Expected a 'put' event"),
    };
    assert_eq!(result, data);

    // Start a watch which should send a message when the subsequent 'delete'
    // event occurs.
    let (del_hdl, r) = spawn_watch(&key, &mut store).await;
    store.delete_kv(&key).await.unwrap();

    // Wait up to 1 second for the watch to see the delete event.
    let msg = r
        .recv_timeout(Duration::from_secs(1))
        .expect("Timed out waiting for message");
    match msg {
        WatchEvent::Delete => {
            // The entry is deleted. Let's check that a subsequent 'get' fails.
            store
                .get_kv(&key)
                .await
                .expect_err("Entry should have been deleted");
        }
        _ => panic!("Expected a 'delete' event"),
    };

    put_hdl.await.unwrap();
    del_hdl.await.unwrap();
}

/// Spawn a watch thread which watches for a single change to the entry with
/// the given key.
async fn spawn_watch<W: Store>(
    key: &serde_json::Value,
    store: &mut W,
) -> (JoinHandle<()>, Receiver<WatchEvent>) {
    let (s, r) = oneshot::channel();
    let mut watch = store.watch_kv(&key).await.expect("Failed to watch");
    let hdl = tokio::spawn(async move {
        match watch.recv().await.unwrap() {
            Ok(event) => {
                s.send(event).unwrap();
            }
            Err(_) => {
                panic!("Failed to receive event");
            }
        }
    });
    (hdl, r)
}

/// Wait to establish a connection to etcd.
/// Returns 'Ok' if connected otherwise 'Err' is returned.
fn wait_for_etcd_ready(endpoint: &str) -> io::Result<TcpStream> {
    let sa = SocketAddr::from_str(endpoint).unwrap();
    TcpStream::connect_timeout(&sa, Duration::from_secs(3))
}
