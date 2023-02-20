//! A library for weighted scoring.

#![deny(missing_docs)]

mod criteria;
mod error;
mod range;
mod score;
mod value;
mod weighted_score;

/// Exports from the criteria module.
pub use criteria::Criteria;
/// Exports from the error module.
pub use error::Error;
/// Exports from the range module.
pub use range::Ranged;
/// Exports from the score module.
pub use score::Score;
/// Exports from the value module.
pub use value::{Value, ValueGrading};
/// Exports from the weighted_score module.
pub use weighted_score::WeightedScore;

#[cfg(test)]
mod tests {
    use super::{
        criteria::Criteria,
        error::Error,
        range::Ranged,
        score::Score,
        value::{Value, ValueGrading},
        weighted_score::WeightedScore,
    };

    #[test]
    fn value_to_score() {
        let value1 = Value::new("pool1", 100);
        let value2 = Value::new("pool2", 10);

        let (score1, score2) = Value::dual_grade(value1, value2, ValueGrading::Higher);
        assert_eq!(score1, Score::new_const("pool1", 90));
        assert_eq!(score2, Score::new_const("pool2", 9));

        let (score1, score2) = Value::dual_grade(value1, value2, ValueGrading::Lower);
        assert_eq!(score1, Score::new_const("pool1", 9));
        assert_eq!(score2, Score::new_const("pool2", 90));

        let (score1, score2) = Value::dual_grade(value2, value1, ValueGrading::Higher);
        assert_eq!(score1, Score::new_const("pool2", 9));
        assert_eq!(score2, Score::new_const("pool1", 90));
    }

    #[test]
    fn weighted_score() {
        let n_replicas = Criteria::new("n_replicas", Ranged(25));
        let free_space = Criteria::new("free_space", Ranged(40));
        let over_commit = Criteria::new("over_commit", Ranged(35));

        let score = WeightedScore::single()
            .weigh(n_replicas, Score::new("pool1", Ranged(10)))
            .weigh(free_space, Score::new("pool1", Ranged(30)))
            .weigh(over_commit, Score::new("pool1", Ranged(50)))
            .score()
            .unwrap();
        assert_eq!(score.val(), 32);

        let score = WeightedScore::single()
            .weigh(Ranged(25), Ranged(10))
            .weigh(Ranged(40), Ranged(30))
            .weigh(Ranged(35), Ranged(50))
            .score()
            .unwrap();
        assert_eq!(score.val(), 32);
    }

    #[test]
    fn weighted_values() {
        let n_replicas = Criteria::new("n_replicas", Ranged(25));

        let pool1_repl = Value::new("pool1", 100);
        let pool2_repl = Value::new("pool2", 10);

        let (score1, _) = Value::dual_grade(pool1_repl, pool2_repl, ValueGrading::Higher);

        let score = WeightedScore::single()
            .weigh(n_replicas, score1)
            .score()
            .unwrap();
        assert_eq!(score.val(), 22);
    }

    #[test]
    fn dual_weighted_values() {
        let n_replicas = Criteria::new("n_replicas", Ranged(25));
        let free_space = Criteria::new("free_space", Ranged(40));
        let over_commit = Criteria::new("over_commit", Ranged(35));

        let score = WeightedScore::dual_values()
            .weigh(n_replicas, ValueGrading::Lower, 100, 100)
            .weigh(free_space, ValueGrading::Higher, 100, 100)
            .weigh(over_commit, ValueGrading::Lower, 100, 100)
            .score()
            .unwrap();
        assert_eq!(score, (Ranged(50), Ranged(50)));
    }

    #[test]
    fn heavy_weighted() {
        let error = WeightedScore::single()
            .weigh(Ranged(25), Ranged(10))
            .weigh(Ranged(40), Ranged(30))
            .weigh(Ranged(80), Ranged(50))
            .score()
            .expect_err("Too heavy!");
        assert_eq!(error, Error::Heavy { sum: 145 });
    }
}
