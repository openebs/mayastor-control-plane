use crate::{range::Ranged, score::Score};

/// Strategy used to grade values into scores.
#[derive(Copy, Clone)]
pub enum ValueGrading {
    /// The Higher the value, the higher the score.
    Higher,
    /// The higher the value, the lower the score.
    Lower,
}

/// An entry score which has not yet been scored.
/// Before it may be weighted against other entries it must be scored from (0..100).
#[derive(Debug, Copy, Clone)]
pub struct Value {
    name: Option<&'static str>,
    value: u64,
}
impl Value {
    /// Create a new `Self` using the given name and value.
    pub fn new(name: impl Into<&'static str>, value: u64) -> Self {
        let name = Some(name.into());
        Self { name, value }
    }
    /// Scores 2 entries by balancing each other.
    pub fn dual_grade(
        a: impl Into<Self>,
        b: impl Into<Self>,
        strategy: ValueGrading,
    ) -> (Score, Score) {
        let (a, b) = (a.into(), b.into());
        let total = a.value + b.value;
        let percent = |v: &Self| -> u64 {
            match strategy {
                _ if total == 0 => 50,
                ValueGrading::Higher => (v.value * 100) / total,
                ValueGrading::Lower => ((total - v.value) * 100) / total,
            }
        };
        let (score1, score2) = (percent(&a), percent(&b));
        (
            Score::new(a.name, Ranged::new_ranged(score1)),
            Score::new(b.name, Ranged::new_ranged(score2)),
        )
    }
}
impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Self { name: None, value }
    }
}
