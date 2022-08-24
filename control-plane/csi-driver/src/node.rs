/// Our control plane's node plugin service, which runs within the node-plugin csi driver container.
pub mod internal {
    #![allow(clippy::derive_partial_eq_without_eq)]
    #![allow(clippy::upper_case_acronyms)]
    tonic::include_proto!("node.service");
}
