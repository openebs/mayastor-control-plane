use serde_json::Value;
use stor_port::{
    pstor::{key_prefix_obj, product_v1_key_prefix_obj, StorableObjectType, Store, API_VERSION},
    types::v0::{
        store::{definitions::StoreError, pool::PoolSpec, volume::VolumeSpec},
        transport::PoolTopology,
    },
};
use tracing::{info, warn};
use utils::{CREATED_BY_KEY, DSP_OPERATOR};

/// The value to mark the creation source of a pool to be msp operator in labels.
pub const MSP_OPERATOR: &str = "msp-operator";

/// Migrate the values from legacy keys to latest keys.
pub(crate) async fn migrate_product_v1_to_v2<S: Store>(
    store: &mut S,
    spec_type: StorableObjectType,
    etcd_max_page_size: i64,
) -> Result<(), StoreError> {
    info!("Migrating {spec_type:?} from v1 to v2 key space");
    let prefix = &product_v1_key_prefix_obj(spec_type);
    let store_entries = store
        .get_values_paged_all(prefix, etcd_max_page_size)
        .await?;
    for (k, v) in store_entries {
        let id = k
            .split('/')
            .last()
            .ok_or_else(|| StoreError::InvalidKey { key: k.to_string() })?;

        let new_value = match spec_type {
            StorableObjectType::VolumeSpec => migrate_volume_labels(v)?,
            StorableObjectType::PoolSpec => migrate_pool_labels(v)?,
            _ => v,
        };

        store
            .put_kv(
                &format!("{}/{}", key_prefix_obj(spec_type, API_VERSION), id),
                &new_value,
            )
            .await?;
    }

    store.delete_values_prefix(prefix).await?;
    warn!("Removed {spec_type:?} for v1 key space from store");

    Ok(())
}

/// Migrate the labels put by product v1 csi-controller to latest.
fn migrate_volume_labels(mut value: Value) -> Result<Value, StoreError> {
    let mut spec: VolumeSpec =
        serde_json::from_value(value.take()).map_err(|error| StoreError::DeserialiseValue {
            value: value.to_string(),
            source: error,
        })?;
    if let Some(ref mut topology) = &mut spec.topology {
        if let Some(ref mut pool_topology) = topology.pool {
            match pool_topology {
                PoolTopology::Labelled(labels) => {
                    if let Some(value) = labels.inclusion.get_mut(CREATED_BY_KEY) {
                        if value == MSP_OPERATOR {
                            labels
                                .inclusion
                                .insert(CREATED_BY_KEY.to_string(), DSP_OPERATOR.to_string());
                        }
                    }
                }
            }
        }
    }
    serde_json::to_value(&spec).map_err(|error| StoreError::SerialiseValue { source: error })
}

/// Migrate the labels put by product v1 operator to latest.
fn migrate_pool_labels(mut value: Value) -> Result<Value, StoreError> {
    let mut spec: PoolSpec =
        serde_json::from_value(value.take()).map_err(|error| StoreError::DeserialiseValue {
            value: value.to_string(),
            source: error,
        })?;
    if let Some(ref mut labels) = &mut spec.labels {
        if let Some(value) = labels.get_mut(CREATED_BY_KEY) {
            if value == MSP_OPERATOR {
                labels.insert(CREATED_BY_KEY.to_string(), DSP_OPERATOR.to_string());
            }
        }
    }
    serde_json::to_value(&spec).map_err(|error| StoreError::SerialiseValue { source: error })
}
