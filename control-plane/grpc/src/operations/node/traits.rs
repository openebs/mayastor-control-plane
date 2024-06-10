use crate::{
    blockdevice, blockdevice::GetBlockDevicesRequest, context::Context, node,
    node::get_nodes_request,
};
use std::{collections::HashMap, convert::TryFrom, str::FromStr};
use stor_port::{
    transport_api::{
        v0::{BlockDevices, Nodes},
        ReplyError, ResourceKind,
    },
    types::v0::{
        store::node::{CordonDrainState, CordonedState, DrainState, NodeSpec},
        transport::{
            BlockDevice, Filesystem, Filter, GetBlockDevices, Node, NodeId, NodeState, NodeStatus,
            Partition,
        },
    },
    TryIntoOption,
};

/// Trait implemented by services which support node operations.
#[tonic::async_trait]
pub trait NodeOperations: Send + Sync {
    /// Get nodes based on the filters.
    async fn get(
        &self,
        filter: Filter,
        ignore_notfound: bool,
        ctx: Option<Context>,
    ) -> Result<Nodes, ReplyError>;
    /// Liveness probe for node service.
    async fn probe(&self, ctx: Option<Context>) -> Result<bool, ReplyError>;
    /// Get the all or usable blockdevices from a particular node.
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
    /// Associate the labels with the given node.
    async fn label(
        &self,
        id: NodeId,
        label: HashMap<String, String>,
        overwrite: bool,
    ) -> Result<Node, ReplyError>;
    /// Remove label from the a given node.
    async fn unlabel(&self, id: NodeId, label: String) -> Result<Node, ReplyError>;
}

impl TryFrom<node::Node> for Node {
    type Error = ReplyError;
    fn try_from(node_grpc_type: node::Node) -> Result<Self, Self::Error> {
        let node_spec = match node_grpc_type.spec {
            Some(spec) => Some(NodeSpec::new(
                spec.node_id.into(),
                std::net::SocketAddr::from_str(&spec.endpoint).map_err(|e| {
                    Self::Error::invalid_argument(
                        ResourceKind::Node,
                        "node.spec.endpoint",
                        e.to_string(),
                    )
                })?,
                spec.labels.unwrap_or_default().value,
                match spec.cordon_drain_state {
                    Some(state) => match state.cordondrainstate {
                        Some(node::cordon_drain_state::Cordondrainstate::Cordoned(state)) => {
                            let type_v0_cordoned_state = CordonedState {
                                cordonlabels: state.cordon_labels,
                            };
                            Some(CordonDrainState::Cordoned(type_v0_cordoned_state))
                        }
                        Some(node::cordon_drain_state::Cordondrainstate::Draining(state)) => {
                            let type_v0_draining_state = DrainState {
                                cordonlabels: state.cordon_labels,
                                drainlabels: state.drain_labels,
                            };
                            Some(CordonDrainState::Draining(type_v0_draining_state))
                        }
                        Some(node::cordon_drain_state::Cordondrainstate::Drained(state)) => {
                            let type_v0_drained_state = DrainState {
                                cordonlabels: state.cordon_labels,
                                drainlabels: state.drain_labels,
                            };
                            Some(CordonDrainState::Drained(type_v0_drained_state))
                        }
                        None => None,
                    },
                    None => None,
                },
                spec.node_nqn.try_into_opt()?,
                None,
                None,
                None,
            )),
            None => None,
        };
        let node_state = match node_grpc_type.state {
            Some(state) => {
                let status: NodeStatus = match node::NodeStatus::try_from(state.status) {
                    Ok(status) => Ok(status.into()),
                    Err(error) => Err(Self::Error::invalid_argument(
                        ResourceKind::Node,
                        "node.state.status",
                        error,
                    )),
                }?;
                // todo: pass proper apiversion on the upper layer once openapi has the changes
                Some(NodeState::new(
                    state.node_id.into(),
                    std::net::SocketAddr::from_str(&state.endpoint).map_err(|e| {
                        Self::Error::invalid_argument(
                            ResourceKind::Node,
                            "node.state.endpoint",
                            e.to_string(),
                        )
                    })?,
                    status,
                    None,
                    state.node_nqn.try_into_opt()?,
                ))
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
    fn from(types_v0_node: Node) -> Self {
        let grpc_node_spec = types_v0_node.spec().map(|types_v0_spec| node::NodeSpec {
            node_id: types_v0_spec.id().to_string(),
            endpoint: types_v0_spec.endpoint().to_string(),
            labels: Some(crate::common::StringMapValue {
                value: types_v0_spec.labels().clone(),
            }),
            cordon_drain_state: match types_v0_spec.cordon_drain_state() {
                Some(e_types_v0_ds) => {
                    let grpc_str_drainstate = node::CordonDrainState {
                        cordondrainstate: match e_types_v0_ds {
                            CordonDrainState::Cordoned(state) => {
                                let grpc_cordoned_state = node::CordonedState {
                                    cordon_labels: state.cordonlabels.clone(),
                                };
                                Some(node::cordon_drain_state::Cordondrainstate::Cordoned(
                                    grpc_cordoned_state,
                                ))
                            }
                            CordonDrainState::Draining(state) => {
                                let grpc_draining_state = node::DrainState {
                                    cordon_labels: state.cordonlabels.clone(),
                                    drain_labels: state.drainlabels.clone(),
                                };
                                Some(node::cordon_drain_state::Cordondrainstate::Draining(
                                    grpc_draining_state,
                                ))
                            }
                            CordonDrainState::Drained(state) => {
                                let grpc_drained_state = node::DrainState {
                                    cordon_labels: state.cordonlabels.clone(),
                                    drain_labels: state.drainlabels.clone(),
                                };
                                Some(node::cordon_drain_state::Cordondrainstate::Drained(
                                    grpc_drained_state,
                                ))
                            }
                        },
                    };
                    Some(grpc_str_drainstate)
                }
                None => None,
            },
            node_nqn: types_v0_spec.node_nqn().as_ref().map(|nqn| nqn.to_string()),
        });
        let grpc_node_state = match types_v0_node.state() {
            None => None,
            Some(types_v0_state) => {
                let grpc_node_status: node::NodeStatus = types_v0_state.status.clone().into();
                Some(node::NodeState {
                    node_id: types_v0_state.id.to_string(),
                    endpoint: types_v0_state.grpc_endpoint.to_string(),
                    status: grpc_node_status as i32,
                    node_nqn: types_v0_state.node_nqn.as_ref().map(|nqn| nqn.to_string()),
                })
            }
        };
        node::Node {
            node_id: types_v0_node.id().to_string(),
            spec: grpc_node_spec,
            state: grpc_node_state,
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

// from grpc type to stored version
impl From<node::CordonedState> for CordonedState {
    fn from(src: node::CordonedState) -> Self {
        Self {
            cordonlabels: src.cordon_labels,
        }
    }
}
impl From<node::DrainState> for DrainState {
    fn from(src: node::DrainState) -> Self {
        Self {
            cordonlabels: src.cordon_labels,
            drainlabels: src.drain_labels,
        }
    }
}

// from stored version to grpc type
impl From<CordonedState> for node::CordonedState {
    fn from(src: CordonedState) -> Self {
        Self {
            cordon_labels: src.cordonlabels,
        }
    }
}
impl From<DrainState> for node::DrainState {
    fn from(src: DrainState) -> Self {
        Self {
            cordon_labels: src.cordonlabels,
            drain_labels: src.drainlabels,
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
            partition: bd.partition.map(|bd| blockdevice::Partition {
                parent: bd.parent,
                number: bd.number,
                name: bd.name,
                scheme: bd.scheme,
                typeid: bd.typeid,
                uuid: bd.uuid,
            }),
            filesystem: bd.filesystem.map(|bd| blockdevice::Filesystem {
                fstype: bd.fstype,
                label: bd.label,
                uuid: bd.uuid,
                mountpoint: bd.mountpoint,
            }),
            available: bd.available,
            connection_type: bd.connection_type,
            is_rotational: bd.is_rotational,
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
            partition: bd.partition.map(|partition| Partition {
                parent: partition.parent,
                number: partition.number,
                name: partition.name,
                scheme: partition.scheme,
                typeid: partition.typeid,
                uuid: partition.uuid,
            }),
            filesystem: bd.filesystem.map(|filesystem| Filesystem {
                fstype: filesystem.fstype,
                label: filesystem.label,
                uuid: filesystem.uuid,
                mountpoint: filesystem.mountpoint,
            }),
            available: bd.available,
            connection_type: bd.connection_type,
            is_rotational: bd.is_rotational,
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
