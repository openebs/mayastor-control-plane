//! The MayastorPool operator has been deprecated and now the DiskPool operator
//! is the reponsible for all CRUD operations on DiskPool CRs in a K8s cluster.
//! This module has the definitions and the client specific operations that would
//! be need for the migration of the MayastorPool CRs in the cluster to the DiskPool
//! CRs on the startup of the operator.

/// MayastorPool client operations.
pub(crate) mod client;
/// MayastorPool CRD definition.
pub(crate) mod crd;
