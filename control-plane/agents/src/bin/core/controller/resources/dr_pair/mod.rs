use crate::controller::resources::ResourceUid;
use stor_port::types::v0::{store::dr_pair::DrClusterPairSpec, transport::DrPairId};

impl ResourceUid for DrClusterPairSpec {
    type Uid = DrPairId;
    fn uid(&self) -> &Self::Uid {
        &self.id
    }
}
