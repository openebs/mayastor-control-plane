/// The CSI plugin's name.
pub const CSI_PLUGIN_NAME: &str = "io.openebs.csi-mayastor";

/// The node plugin exported components.
pub mod node;

/// The csi driver components.
pub use rpc::csi;
