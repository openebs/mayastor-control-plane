//! The object store a disaster recovery relationship is mediated through.
//!
//! [`DrStore`] is the interface everything above is written against, so that the store
//! can be a bucket now and the peer `agent-dr` later without those callers changing.
//! [`Store`] is the implementation over `object_store`, which brings s3, gcs, azure and
//! an in-memory backend, plus the one verb it has no form of - a conditional delete,
//! which the design requires and which two of the three stores it measured get wrong.
//!
//! [`Prefix`] and [`Layout`] are where the keys come from - the one place that knows the
//! shape of the bucket and of a relationship's subtree in it, since that is the one thing
//! a populated bucket cannot be migrated away from. A [`Checksum`] is what names a chunk
//! in them.
//!
//! [`Sides`] publishes and reads what each side says about itself - its [`DrConfig`] and
//! its [`ClusterState`] - which are the only channel between the two clusters.

#![deny(missing_docs)]

mod checksum;
mod error;
mod layout;
mod side;
mod store;

pub use checksum::{Checksum, ChecksumError};
pub use error::{Error, Operation, Result};
pub use layout::{Layout, Prefix};
pub use side::{
    ChunkRef, ClusterState, DrConfig, HashAlgorithm, PitId, Role, Side, Sides, UploadFormat,
    VERSION,
};
pub use store::{DrStore, S3Config, Store};
