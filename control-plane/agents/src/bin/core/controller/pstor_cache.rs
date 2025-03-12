use agents::errors::SvcError;
use snafu::ResultExt;
use std::collections::BTreeMap;
use stor_port::{
    pstor,
    pstor::{error::DeserialiseValue, Error, ObjectKey, StorableObject, StoreWatchReceiver},
};

/// A persistent store cache, which fetches all entries and them allows reading
/// them via the `pstor::StoreObj` interface.
#[derive(Clone)]
pub(crate) struct PStorCache {
    entries: BTreeMap<String, serde_json::Value>,
}

impl PStorCache {
    /// Create a new `PStorCache`.
    pub(crate) async fn new(
        pstor: &mut impl pstor::StoreKv,
        page_size: i64,
        prefix: &str,
    ) -> Result<Self, SvcError> {
        let entries = pstor
            .get_values_paged_all(prefix, page_size)
            .await?
            .into_iter()
            .collect::<BTreeMap<_, _>>();

        Ok(Self { entries })
    }
}

#[async_trait::async_trait]
impl pstor::StoreObj for PStorCache {
    async fn put_obj<O: StorableObject>(&mut self, _object: &O) -> Result<(), Error> {
        unimplemented!()
    }

    async fn get_obj<O: StorableObject>(&mut self, key: &O::Key) -> Result<O, Error> {
        let key = key.key();
        match self.entries.get(&key) {
            Some(kv) => Ok(
                serde_json::from_value(kv.clone()).context(DeserialiseValue {
                    value: kv.to_string(),
                })?,
            ),
            None => Err(Error::MissingEntry { key }),
        }
    }

    async fn watch_obj<K: ObjectKey>(&mut self, _key: &K) -> Result<StoreWatchReceiver, Error> {
        unimplemented!()
    }
}
