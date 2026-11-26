mod pair;
mod volume;

use deployer_cluster::Cluster;
use dr_store::{S3Config, S3Keys, Store};

/// The cluster's object store, as the test reaches it from the host: what the agent wrote
/// is read back from here, not from the agent.
fn bucket(cluster: &Cluster) -> Store {
    let s3 = cluster.s3();
    Store::s3(&S3Config {
        endpoint: s3.endpoint,
        region: s3.region,
        bucket: s3.bucket,
        keys: Some(S3Keys {
            access_key_id: s3.access_key_id,
            secret_access_key: s3.secret_access_key,
        }),
        ca: None,
    })
    .expect("the cluster's store")
}
