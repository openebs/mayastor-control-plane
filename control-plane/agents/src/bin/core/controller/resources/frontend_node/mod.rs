use crate::controller::resources::ResourceUid;
use stor_port::types::v0::{store::frontend_node::FrontendNodeSpec, transport::FrontendNodeId};

impl ResourceUid for FrontendNodeSpec {
    type Uid = FrontendNodeId;
    fn uid(&self) -> &Self::Uid {
        &self.id
    }
}
