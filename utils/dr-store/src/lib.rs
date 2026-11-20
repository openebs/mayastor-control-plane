//! The object store a disaster recovery relationship is mediated through.
//!
//! [`DrStore`] is the interface everything above is written against, so that the store
//! can be a bucket now and the peer `agent-dr` later without those callers changing.
//! [`Store`] is the implementation over `object_store`, which brings s3, gcs, azure and
//! an in-memory backend, plus the one verb it has no form of - a conditional delete,
//! which the design requires and which two of the three stores it measured get wrong.
//!
//! [`Layout`] is where the keys come from - the one place that knows the shape of a
//! relationship's subtree, since that is the one thing a populated bucket cannot be
//! migrated away from.

#![deny(missing_docs)]

mod error;
mod layout;
mod store;

pub use error::{Error, Operation, Result};
pub use layout::{cluster_cfg, Layout};
pub use store::{DrStore, S3Config, Store};
