use crate::{PlatformInfo, PlatformNS, PlatformUid};

/// No specific platform (deployer cluster using containers).
/// Internally works as `None` platform.
pub(super) struct Deployer(super::none::None);

impl Deployer {
    /// Create a new `Self`.
    pub(super) fn new() -> Self {
        Self(super::none::None::new())
    }
    fn none(&self) -> &super::none::None {
        &self.0
    }
}

impl PlatformInfo for Deployer {
    fn uid(&self) -> PlatformUid {
        self.none().uid()
    }

    fn namespace(&self) -> &PlatformNS {
        self.none().namespace()
    }
}
