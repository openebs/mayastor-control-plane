use super::ResourceUid;
use stor_port::types::v0::{store::node::NodeSpec, transport::NodeId};

impl ResourceUid for NodeSpec {
    type Uid = NodeId;
    fn uid(&self) -> &Self::Uid {
        self.id()
    }
}
