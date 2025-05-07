/// This module has the definitions for the diskpool crd.
pub mod diskpool {
    /// The DiskPool custom resource definition.
    pub mod crd {
        include!("pool/diskpool/crd/v1beta3.rs");
    }
    pub mod quantity {
        include!("pool/diskpool/crd/quantity.rs");
    }
}
