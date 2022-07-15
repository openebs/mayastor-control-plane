use crate::{
    blockdevice, blockdevice::GetBlockDevicesRequest, context::Context, node,
    node::get_nodes_request,
};
use common_lib::{
    transport_api::{
        v0::{BlockDevices, Nodes},
        ReplyError, ResourceKind,
    },
    types::v0::{
        store::node::NodeSpec,
        transport::{
            BlockDevice, Filesystem, Filter, GetBlockDevices, Node, NodeId, NodeState, NodeStatus,
            Partition,
        },
    },
};
use std::convert::TryFrom;

/// Trait implemented by services which support node operations.
#[tonic::async_trait]
pub trait NodeOperations: Send + Sync {
    /// Get nodes based on the filters
    async fn get(&self, filter: Filter, ctx: Option<Context>) -> Result<Nodes, ReplyError>;
    /// Liveness probe for node service
    async fn probe(&self, ctx: Option<Context>) -> Result<bool, ReplyError>;
    /// Get the all or usable blockdevices from a particular node
    async fn get_block_devices(
        &self,
        get_blockdevice: &dyn GetBlockDeviceInfo,
        ctx: Option<Context>,
    ) -> Result<BlockDevices, ReplyError>;
    /// Cordon the node with the given ID and associate the label with the cordoned node.
    async fn cordon(&self, id: NodeId, label: String) -> Result<Node, ReplyError>;
    /// Uncordon the node with the given ID by removing the associated label.
    async fn uncordon(&self, id: NodeId, label: String) -> Result<Node, ReplyError>;
    /// Drain the node with the given ID and associate the label with the draining node.
    async fn drain(&self, id: NodeId, label: String) -> Result<Node, ReplyError>;
}

impl TryFrom<node::Node> for Node {
    type Error = ReplyError;
    fn try_from(node_grpc_type: node::Node) -> Result<Self, Self::Error> {
        let node_spec = node_grpc_type.spec.map(|spec| {
            NodeSpec::new(
                spec.node_id.into(),
                spec.endpoint,
                spec.labels.unwrap_or_default().value,
                Some(spec.cordon_labels),
                Some(spec.drain_labels),
                Some(spec.is_draining),
            )
        });
        let node_state = match node_grpc_type.state {
            Some(state) => {
                let status: NodeStatus = match node::NodeStatus::from_i32(state.status) {
                    Some(status) => status.into(),
                    None => {
                        return Err(ReplyError::invalid_argument(
                            ResourceKind::Node,
                            "node.state.status",
                            "".to_string(),
                        ))
                    }
                };
                Some(NodeState::new(state.node_id.into(), state.endpoint, status))
            }
            None => None,
        };
        Ok(Node::new(
            node_grpc_type.node_id.into(),
            node_spec,
            node_state,
        ))
    }
}

impl From<Node> for node::Node {
    fn from(node: Node) -> Self {
        let node_spec = node.spec().map(|spec| node::NodeSpec {
            node_id: spec.id().to_string(),
            endpoint: spec.endpoint().to_string(),
            labels: Some(crate::common::StringMapValue {
                value: spec.labels().clone(),
            }),
            cordon_labels: spec.cordon_labels(),
            drain_labels: spec.drain_labels(),
            is_draining: spec.is_draining(),
        });
        let node_state = match node.state() {
            None => None,
            Some(state) => {
                let status: node::NodeStatus = state.status.clone().into();
                Some(node::NodeState {
                    node_id: state.id.to_string(),
                    endpoint: state.grpc_endpoint.to_string(),
                    status: status as i32,
                })
            }
        };
        node::Node {
            node_id: node.id().to_string(),
            spec: node_spec,
            state: node_state,
        }
    }
}

impl TryFrom<node::Nodes> for Nodes {
    type Error = ReplyError;
    fn try_from(grpc_nodes: node::Nodes) -> Result<Self, Self::Error> {
        let mut nodes: Vec<Node> = vec![];
        for node in grpc_nodes.nodes {
            nodes.push(Node::try_from(node)?)
        }
        Ok(Nodes(nodes))
    }
}

impl From<Nodes> for node::Nodes {
    fn from(nodes: Nodes) -> Self {
        node::Nodes {
            nodes: nodes
                .into_inner()
                .into_iter()
                .map(|node| node.into())
                .collect(),
        }
    }
}

impl From<get_nodes_request::Filter> for Filter {
    fn from(filter: get_nodes_request::Filter) -> Self {
        match filter {
            get_nodes_request::Filter::Node(node_filter) => {
                Filter::Node(node_filter.node_id.into())
            }
        }
    }
}

impl From<node::NodeStatus> for NodeStatus {
    fn from(src: node::NodeStatus) -> Self {
        match src {
            node::NodeStatus::Unknown => Self::Unknown,
            node::NodeStatus::Online => Self::Online,
            node::NodeStatus::Offline => Self::Offline,
        }
    }
}

impl From<NodeStatus> for node::NodeStatus {
    fn from(src: NodeStatus) -> Self {
        match src {
            NodeStatus::Unknown => Self::Unknown,
            NodeStatus::Online => Self::Online,
            NodeStatus::Offline => Self::Offline,
        }
    }
}

/// GetBlockDeviceInfo trait for the getblockdevices
/// operation
pub trait GetBlockDeviceInfo: Send + Sync {
    /// id of the IoEngine instance
    fn node_id(&self) -> NodeId;
    /// specifies whether to get all devices or only usable devices
    fn all(&self) -> bool;
}

impl GetBlockDeviceInfo for GetBlockDevices {
    fn node_id(&self) -> NodeId {
        self.node.clone()
    }

    fn all(&self) -> bool {
        self.all
    }
}

impl GetBlockDeviceInfo for GetBlockDevicesRequest {
    fn node_id(&self) -> NodeId {
        self.node_id.clone().into()
    }

    fn all(&self) -> bool {
        self.all
    }
}

impl From<&dyn GetBlockDeviceInfo> for GetBlockDevices {
    fn from(data: &dyn GetBlockDeviceInfo) -> Self {
        Self {
            node: data.node_id(),
            all: data.all(),
        }
    }
}

impl From<&dyn GetBlockDeviceInfo> for GetBlockDevicesRequest {
    fn from(data: &dyn GetBlockDeviceInfo) -> Self {
        Self {
            node_id: data.node_id().to_string(),
            all: data.all(),
        }
    }
}

impl From<BlockDevice> for blockdevice::BlockDevice {
    fn from(bd: BlockDevice) -> Self {
        Self {
            devname: bd.devname,
            devtype: bd.devtype,
            devmajor: bd.devmajor,
            devminor: bd.devminor,
            model: bd.model,
            devpath: bd.devpath,
            devlinks: bd.devlinks,
            size: bd.size,
            partition: Some(blockdevice::Partition {
                parent: bd.partition.parent,
                number: bd.partition.number,
                name: bd.partition.name,
                scheme: bd.partition.scheme,
                typeid: bd.partition.typeid,
                uuid: bd.partition.uuid,
            }),
            filesystem: Some(blockdevice::Filesystem {
                fstype: bd.filesystem.fstype,
                label: bd.filesystem.label,
                uuid: bd.filesystem.uuid,
                mountpoint: bd.filesystem.mountpoint,
            }),
            available: bd.available,
        }
    }
}

impl TryFrom<blockdevice::BlockDevice> for BlockDevice {
    type Error = ReplyError;
    fn try_from(bd: blockdevice::BlockDevice) -> Result<Self, Self::Error> {
        Ok(Self {
            devname: bd.devname,
            devtype: bd.devtype,
            devmajor: bd.devmajor,
            devminor: bd.devminor,
            model: bd.model,
            devpath: bd.devpath,
            devlinks: bd.devlinks,
            size: bd.size,
            partition: match bd.partition {
                Some(partition) => Partition {
                    parent: partition.parent,
                    number: partition.number,
                    name: partition.name,
                    scheme: partition.scheme,
                    typeid: partition.typeid,
                    uuid: partition.uuid,
                },
                None => {
                    return Err(ReplyError::invalid_argument(
                        ResourceKind::Block,
                        "bd.partition",
                        "".to_string(),
                    ))
                }
            },
            filesystem: match bd.filesystem {
                Some(filesystem) => Filesystem {
                    fstype: filesystem.fstype,
                    label: filesystem.label,
                    uuid: filesystem.uuid,
                    mountpoint: filesystem.mountpoint,
                },
                None => {
                    return Err(ReplyError::invalid_argument(
                        ResourceKind::Block,
                        "bd.partition",
                        "".to_string(),
                    ))
                }
            },
            available: bd.available,
        })
    }
}

impl TryFrom<blockdevice::BlockDevices> for BlockDevices {
    type Error = ReplyError;
    fn try_from(bds: blockdevice::BlockDevices) -> Result<Self, Self::Error> {
        let mut blockdevices: Vec<BlockDevice> = vec![];
        for bd in bds.blockdevices {
            blockdevices.push(BlockDevice::try_from(bd)?)
        }
        Ok(BlockDevices(blockdevices))
    }
}

impl From<BlockDevices> for blockdevice::BlockDevices {
    fn from(blockdevices: BlockDevices) -> Self {
        blockdevice::BlockDevices {
            blockdevices: blockdevices
                .into_inner()
                .into_iter()
                .map(|bd| bd.into())
                .collect(),
        }
    }
}
