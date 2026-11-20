//! The object store a disaster recovery relationship is mediated through.
//!
//! [`Layout`] is where its keys come from - the one place that knows the shape of a
//! relationship's subtree, since that is the one thing a populated bucket cannot be
//! migrated away from.

#![deny(missing_docs)]

mod layout;

pub use layout::{cluster_cfg, Layout};
