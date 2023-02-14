//! The persistent stor is an interface to a datastore which suits the projects needs.
//! We may have various implementations, depending on the platform we're running on.

/// Error exposed by the pstor.
pub mod error;
/// Export error module.
pub use error::Error;

/// The stor interface.
mod api;
/// Export pstor module.
pub use api::{
    ObjectKey, StorableObject, Store, StoreKey, StoreKv, StoreObj, StoreWatchReceiver, WatchEvent,
};

/// A particular implementation of the persistent store, using ETCd.
pub mod etcd;
mod etcd_keep_alive;

mod hack;
pub use hack::{build_key_prefix, generate_key, key_prefix, key_prefix_obj, StorableObjectType};
