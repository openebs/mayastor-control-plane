use super::ResourceUid;
use stor_port::types::v0::{store::node::NodeSpec, transport::NodeId};

impl ResourceUid for NodeSpec {
    type Uid = NodeId;
    fn uid(&self) -> &Self::Uid {
        self.id()
    }
    fn rkind(&self) -> stor_port::transport_api::ResourceKind {
        stor_port::transport_api::ResourceKind::Node
    }
}
