use super::ResourceUid;
use common_lib::types::v0::{store::node::NodeSpec, transport::NodeId};

impl ResourceUid for NodeSpec {
    type Uid = NodeId;
    fn uid(&self) -> &Self::Uid {
        self.id()
    }
}
