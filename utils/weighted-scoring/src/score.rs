use crate::range::Ranged;

/// A "balanced" entry score which as been balanced to a value of 0..100.
#[derive(Debug, Clone, Eq, PartialEq, Copy)]
pub struct Score {
    name: Option<&'static str>,
    value: Ranged,
}
impl Score {
    /// Create a new `Self` const.
    /// The value does not need to be validated because compilation will fail otherwise.
    pub const fn new_const(name: &'static str, value: u64) -> Self {
        Self {
            name: Some(name),
            value: Ranged::new_const(value),
        }
    }
    /// Creates a new `Self`.
    /// As opposed to `Self::new_const`, the provided value must be `Ranged`.
    pub fn new(name: impl Into<Option<&'static str>>, value: Ranged) -> Self {
        let name = name.into();
        Self { name, value }
    }
    /// Get a reference to the inner range value.
    pub fn ranged_val(&self) -> &Ranged {
        &self.value
    }
}
impl From<Ranged> for Score {
    fn from(value: Ranged) -> Self {
        Self { name: None, value }
    }
}
