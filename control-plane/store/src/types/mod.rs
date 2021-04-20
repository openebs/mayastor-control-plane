pub mod v0;

use serde::{Deserialize, Serialize};

/// Enum defining the various states that a resource spec can be in.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum SpecState<T> {
    Creating,
    Created(T),
    Deleting,
    Deleted,
}

impl<T: std::cmp::PartialEq> SpecState<T> {
    pub fn creating(&self) -> bool {
        self == &Self::Creating
    }
    pub fn created(&self) -> bool {
        matches!(self, &Self::Created(_))
    }
    pub fn deleting(&self) -> bool {
        self == &Self::Deleting
    }
    pub fn deleted(&self) -> bool {
        self == &Self::Deleted
    }
}
