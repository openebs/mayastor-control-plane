/// The CSI plugin's name.
pub const CSI_PLUGIN_NAME: &str = "io.openebs.csi-mayastor";

/// The topology label used to identify a node as a csi-node.
pub fn csi_node_selector() -> String {
    format!("{}={}", CSI_NODE_TOPOLOGY_KEY, CSI_NODE_TOPOLOGY_VAL)
}

type Selector = std::collections::HashMap<String, String>;
/// Parses node selector labels from clap args into its KeyValue pair.
pub fn csi_node_selector_parse<'a, I: Iterator<Item = &'a str>>(
    values: Option<I>,
) -> anyhow::Result<Selector> {
    match values {
        Some(values) => {
            let values = values.collect::<Vec<_>>();
            let selector = values
                .iter()
                .map(|source| match source.split_once('=') {
                    None => Err(anyhow::anyhow!(
                        "node-selector labels must be in the format: 'Key=Value'"
                    )),
                    Some((key, value)) => Ok((key.to_string(), value.to_string())),
                })
                .collect::<anyhow::Result<Selector>>()?;
            anyhow::ensure!(
                values.len() == selector.len(),
                "Node selector labels must be unique"
            );
            Ok(selector)
        }
        None => Ok(Selector::from([(
            CSI_NODE_TOPOLOGY_KEY.to_string(),
            CSI_NODE_TOPOLOGY_VAL.to_string(),
        )])),
    }
}
/// The topology key added by the csi-node plugin.
pub const CSI_NODE_TOPOLOGY_KEY: &str = "openebs.io/csi-node";
/// The topology value added by the csi-node plugin.
pub const CSI_NODE_TOPOLOGY_VAL: &str = "mayastor";

/// The topology label used to uniquely identify a node.
/// The csi-driver (node,controller) and the io-engine must pick the same value.
/// The nodename key assigned to each node.
pub const NODE_NAME_TOPOLOGY_KEY: &str = "openebs.io/nodename";

/// Volume Parameters parsed from context.
pub use context::{CreateParams, Parameters, PublishParams};

/// The node plugin exported components.
pub mod node;

/// The csi driver components.
pub use rpc::csi;

/// The volume contexts.
pub mod context;
