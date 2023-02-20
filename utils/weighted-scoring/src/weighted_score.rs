use crate::{
    criteria::Criteria,
    error::Error,
    range::Ranged,
    score::Score,
    value::{Value, ValueGrading},
};

/// A weighted score is calculated by iterating over various criteria and multiplying each
/// weight by the scored data point.
#[derive(Default, Debug)]
pub struct WeightedScore {}
impl WeightedScore {
    /// Builder like pattern for `WeightedScoreSingle`.
    pub fn single() -> WeightedScoreSingle {
        WeightedScoreSingle::default()
    }
    /// Builder like pattern for `DualValWeightedScore`.
    pub fn dual_values() -> DualValWeightedScore {
        DualValWeightedScore::default()
    }
}

/// A weighted score is calculated by iterating over various criteria and multiplying each
/// weight by the scored data point.
#[derive(Default, Debug)]
pub struct WeightedScoreSingle {
    accrued_weights: u64,
    accrued_score: u64,
}
impl WeightedScoreSingle {
    /// Weigh the matching `criteria` and `score` by reference.
    pub fn weigh_ref(mut self, criteria: &Criteria, score: &Score) -> Self {
        self.accrued_score += criteria.weigh(score);
        self.accrued_weights += criteria.weight().val();
        self
    }
    /// Weigh the matching `criteria` and `score`.
    pub fn weigh(self, criteria: impl Into<Criteria>, score: impl Into<Score>) -> Self {
        self.weigh_ref(&criteria.into(), &score.into())
    }
    /// Returns the final score which is the sum of all criteria weighing.
    pub fn score(self) -> Result<Ranged, Error> {
        if self.accrued_weights > 100 {
            return Err(Error::Heavy {
                sum: self.accrued_weights,
            });
        }
        Ok(Ranged(self.accrued_score / 100))
    }
}

/// A weighted score is calculated by iterating over various criteria and multiplying each
/// weight by the scored data point.
/// Same principle as `WeightedScoreSingle` but it operates with "raw" values rather than scores.
/// That means it has to grade each "raw" value into a `Score` in order to be able to calculate
/// each weighted score.
/// Once graded, each weighted score is then calculated using `WeightedScoreSingle`.
#[derive(Default, Debug)]
pub struct DualValWeightedScore {
    accrued_score_1: WeightedScoreSingle,
    accrued_score_2: WeightedScoreSingle,
}
impl DualValWeightedScore {
    /// Weigh the matching `criteria` and scores by reference.
    pub fn weigh_ref(mut self, criteria: &Criteria, score_1: &Score, score_2: &Score) -> Self {
        self.accrued_score_1 = self.accrued_score_1.weigh_ref(criteria, score_1);
        self.accrued_score_2 = self.accrued_score_2.weigh_ref(criteria, score_2);
        self
    }
    /// Weigh the matching `criteria` and values. The values are graded using the given
    /// `ValueGrading` strategy.
    pub fn weigh(
        self,
        criteria: impl Into<Criteria>,
        strategy: ValueGrading,
        value_1: impl Into<Value>,
        value_2: impl Into<Value>,
    ) -> Self {
        let (score_1, score_2) = Value::dual_grade(value_1, value_2, strategy);
        self.weigh_ref(&criteria.into(), &score_1, &score_2)
    }
    /// Returns the final score which is the sum of all criteria weighing.
    pub fn score(self) -> Result<(Ranged, Ranged), Error> {
        Ok((self.accrued_score_1.score()?, self.accrued_score_2.score()?))
    }
}
