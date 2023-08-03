/// This module has the definitions for the diskpool crd.
pub mod diskpool {
    /// The DiskPool custom resource definition.
    pub mod crd {
        include!("pool/diskpool/v1beta1.rs");
    }
}
