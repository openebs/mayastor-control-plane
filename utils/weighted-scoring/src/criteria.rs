use crate::{range::Ranged, score::Score};

/// A Criteria which carries a ranged weight (1..100).
/// The higher the weight the more important the criteria is and the greater effect on the final
/// weighted score it has.
#[derive(Copy, Clone)]
pub struct Criteria {
    _name: Option<&'static str>,
    weight: Ranged,
}
impl Criteria {
    /// Create a new `Criteria` with the given names and weight.
    pub fn new(name: impl Into<&'static str>, weight: Ranged) -> Self {
        let _name = Some(name.into());
        Self { _name, weight }
    }
    /// Weigh the given score according to this criteria's weight.
    pub(crate) fn weigh(&self, entry: &Score) -> u64 {
        self.weight.val() * entry.ranged_val().val()
    }
    /// Get the criteria weight.
    pub fn weight(&self) -> &Ranged {
        &self.weight
    }
}
impl From<Ranged> for Criteria {
    fn from(weight: Ranged) -> Self {
        Self {
            _name: None,
            weight,
        }
    }
}
