//! This module encapsulates the 1.x and 2.x product version specific etcd
//! key space management. That involves the key definitions, detection of presence of
//! 1.x prefixes and migration between 1.x and 2.x etcd key spaces.

use crate::{api::Store, common::StorableObjectType, products::v2::API_VERSION};
use tracing::{info, warn};

pub mod v1;
pub mod v2;

/// Migrate the values from legacy keys to latest keys.
pub async fn migrate_product_v1_to_v2<S: Store>(
    store: &mut S,
    spec_type: StorableObjectType,
) -> Result<(), crate::Error> {
    info!("Migrating {spec_type:?} from v1 to v2 key space");
    let prefix = &v1::key_prefix_obj(spec_type);
    let store_entries = store.get_values_prefix(prefix).await?;
    for (k, v) in &store_entries {
        // Bail if we are not able extract the resource id. This can leave
        // incomplete migration entries in etcd, if failed midway.
        let id = k
            .split('/')
            .last()
            .ok_or_else(|| crate::Error::InvalidKey { key: k.to_string() })?;

        store
            .put_kv(
                &format!("{}/{}", v2::key_prefix_obj(spec_type, API_VERSION), id),
                v,
            )
            .await?;
    }

    store.delete_values_prefix(prefix).await?;
    warn!("Removed {spec_type:?} for v1 key space from store");

    Ok(())
}
