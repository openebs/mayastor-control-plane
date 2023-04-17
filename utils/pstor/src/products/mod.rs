//! This module encapsulates the 1.x and 2.x product version specific etcd
//! key space management. That involves the key definitions, detection of presence of
//! 1.x prefixes and migration between 1.x and 2.x etcd key spaces.

pub mod v1;
pub mod v2;
