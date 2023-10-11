//! This module has the definitions and the client specific operations for the
//! DiskPool CRs. This module is works with the current operator logic.

/// DiskPool client operations.
pub(crate) mod client;
/// The DiskPool custom resource definition.
pub(crate) mod v1alpha1;
pub(crate) mod v1beta1;
