/// The CSI plugin's name.
pub use utils::csi_plugin_name;

/// The topology label used to identify a node as a csi-node.
pub fn csi_node_selector() -> String {
    format!("{}={}", csi_node_topology_key(), csi_node_topology_val())
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
            csi_node_topology_key().to_string(),
            csi_node_topology_val().to_string(),
        )])),
    }
}
/// The topology key added by the csi-node plugin.
pub fn csi_node_topology_key() -> String {
    use utils::constants::PRODUCT_DOMAIN_NAME;
    format!("{PRODUCT_DOMAIN_NAME}/csi-node")
}
/// The topology value added by the csi-node plugin.
pub fn csi_node_topology_val() -> &'static str {
    utils::constants::PRODUCT_NAME
}

/// The topology label used to uniquely identify a node.
/// The csi-driver (node,controller) and the io-engine must pick the same value.
/// The nodename key assigned to each node.
pub fn node_name_topology_key() -> String {
    use utils::constants::PRODUCT_DOMAIN_NAME;
    format!("{PRODUCT_DOMAIN_NAME}/nodename")
}

/// The volume's share uri contains the hostnqns for all allowed hosts.
/// This function parses the uri and retains the hostnqn only for `node_id`.
/// # Warning
/// In case hostnqn are present in the uri but this node's nodenqn is not, then
/// we error out with invalid_argument.
pub fn parse_host_uri(node_id: &str, uri: &str) -> Result<String, tonic::Status> {
    let mut url =
        url::Url::parse(uri).map_err(|error| tonic::Status::internal(format!("{uri}: {error}")))?;
    let node_nqn: String =
        stor_port::types::v0::transport::NvmeNqn::from_nodename(node_id).to_string();

    let mut matched_ours = false;
    let mut host_nqns = false;
    let queries = url
        .query_pairs()
        .filter(|(name, value)| {
            if name != "hostnqn" {
                return true;
            }
            host_nqns = true;
            if value == &node_nqn {
                matched_ours = true;
                true
            } else {
                false
            }
        })
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect::<Vec<(_, _)>>();

    if host_nqns && !matched_ours {
        let msg = format!("{uri} has hostnqn's but not mine ({node_nqn})");
        return Err(tonic::Status::invalid_argument(msg));
    }

    url.query_pairs_mut().clear().extend_pairs(queries);

    // we have to decode because otherwise the nqn would be encoded, ex:
    // nqn.2019-05.io.openebs%3Anode-name%3Aksworker-1
    Ok(percent_encoding::percent_decode_str(url.as_ref())
        .decode_utf8_lossy()
        .to_string())
}

#[test]
fn test_parse_host_uri() {
    let error = parse_host_uri("node-1", "nvmf://10.0.0.213:8420/nqn.2019-05.io.openebs:0a9bbaff-7b08-4160-b5d3-1c0a3a4539ae?hostnqn=nqn.2019-05.io.openebs:node-name:ksworker-1&hostnqn=nqn.2019-05.io.openebs:node-name:ksworker-2").unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    let uri = parse_host_uri("ksworker-1", "nvmf://10.0.0.213:8420/nqn.2019-05.io.openebs:0a9bbaff-7b08-4160-b5d3-1c0a3a4539ae?hostnqn=nqn.2019-05.io.openebs:node-name:ksworker-1&hostnqn=nqn.2019-05.io.openebs:node-name:ksworker-2").unwrap();
    assert_eq!(uri, "nvmf://10.0.0.213:8420/nqn.2019-05.io.openebs:0a9bbaff-7b08-4160-b5d3-1c0a3a4539ae?hostnqn=nqn.2019-05.io.openebs:node-name:ksworker-1");
    let uri = parse_host_uri("ksworker-1", "nvmf://10.0.0.213:8420/nqn.2019-05.io.openebs:0a9bbaff-7b08-4160-b5d3-1c0a3a4539ae?hostnqn=nqn.2019-05.io.openebs:node-name:ksworker-1&hostnqn=nqn.2019-05.io.openebs:node-name:ksworker-2&a=1").unwrap();
    assert_eq!(
        uri,
        "nvmf://10.0.0.213:8420/nqn.2019-05.io.openebs:0a9bbaff-7b08-4160-b5d3-1c0a3a4539ae?hostnqn=nqn.2019-05.io.openebs:node-name:ksworker-1&a=1"
    );
    let uri = parse_host_uri("ksworker-1", "nvmf://10.0.0.213:8420/nqn.2019-05.io.openebs:0a9bbaff-7b08-4160-b5d3-1c0a3a4539ae?a=1&hostnqn=nqn.2019-05.io.openebs:node-name:ksworker-1&hostnqn=nqn.2019-05.io.openebs:node-name:ksworker-2").unwrap();
    assert_eq!(uri, "nvmf://10.0.0.213:8420/nqn.2019-05.io.openebs:0a9bbaff-7b08-4160-b5d3-1c0a3a4539ae?a=1&hostnqn=nqn.2019-05.io.openebs:node-name:ksworker-1");
}

/// Volume Parameters parsed from context.
pub use context::{CreateParams, Parameters, PublishParams};

/// The node plugin exported components.
pub mod node;

/// The csi driver components.
pub use rpc::csi;

/// The volume contexts.
pub mod context;
/// Filesystem Operations.
pub mod filesystem;
/// Volume concurrency limiter.
pub mod limiter;
/// Contains tools to advertise the same set of capabilities across different
/// CSI microservices.
pub mod plugin_capabilities;
/// Request Tracing.
pub mod trace;
