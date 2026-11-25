use snafu::Snafu;

/// The verb a store failure happened under.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Operation {
    /// Reading a whole object.
    Get,
    /// Reading what the store knows about a key.
    Head,
    /// Writing a whole object.
    Put,
    /// Listing the keys under a prefix.
    List,
    /// Removing an object.
    Delete,
    /// Removing an object only if it still carries a given tag.
    DeleteIfMatch,
}

impl std::fmt::Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Get => "get",
            Self::Head => "head",
            Self::Put => "put",
            Self::List => "list",
            Self::Delete => "delete",
            Self::DeleteIfMatch => "conditional delete",
        })
    }
}

/// What went wrong talking to the store.
///
/// The variants are the distinctions a caller acts on, not the ones a store reports:
/// absent, refused, lost a precondition, or worth another cycle. Everything above this
/// crate decides what to do from the variant alone, so a second backend does not have
/// to reproduce another store's status codes to be understood.
// The variant says what happened; naming its fields again one line below would only
// repeat it. Same as the agents' `SvcError`.
#[allow(missing_docs)]
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    /// No object under that key.
    #[snafu(display("No object at '{key}'"))]
    NotFound { key: String },

    /// The credentials this store holds do not grant the operation.
    #[snafu(display("Denied {operation} on '{key}': {source}"))]
    Denied {
        operation: Operation,
        key: String,
        source: object_store::Error,
    },

    /// A conditional request was refused because the object had moved on.
    ///
    /// The collector reads this as "somebody referenced the chunk while I was deciding",
    /// which is the whole point of asking conditionally.
    #[snafu(display("'{key}' did not meet the precondition of {operation}"))]
    Precondition { operation: Operation, key: String },

    /// The store could not answer, and the client had already retried.
    ///
    /// Not "try once more" - the backend's own retries are configured and spent by the
    /// time this is returned. It says the failure was the store rather than the request,
    /// so the next cycle may well get through.
    #[snafu(display("The store failed {operation}: {source}"))]
    Transient {
        operation: Operation,
        source: object_store::Error,
    },

    /// An object is there, and is not what its key says it is: it does not parse, or it
    /// describes a different side from the one whose subtree it is in.
    ///
    /// Never read as absent. A peer whose document cannot be read is not a peer that has
    /// not joined, and treating it as one is how a relationship in use gets torn down.
    #[snafu(display("'{key}' is malformed: {reason}"))]
    Malformed { key: String, reason: String },

    /// More than one cluster answers to the name a peer is expected under, and nothing
    /// says which is the live one.
    #[snafu(display("More than one cluster answers as peer '{peer}': {uids:?}"))]
    AmbiguousPeer { peer: String, uids: Vec<String> },

    /// The store could not be built from what it was given, or cannot do what was asked
    /// of it at all.
    #[snafu(display("Store configuration: {reason}"))]
    Config { reason: String },
}

impl Error {
    /// Map what the backend said into the distinction a caller acts on.
    ///
    /// By variant rather than by status code: `object_store` has already told each
    /// backend's errors apart, including the ones that are not http, and repeating that
    /// work here would only approximate it.
    pub(crate) fn from_store(operation: Operation, key: &str, error: object_store::Error) -> Self {
        match error {
            object_store::Error::NotFound { .. } => Self::NotFound {
                key: key.to_string(),
            },
            object_store::Error::PermissionDenied { .. }
            | object_store::Error::Unauthenticated { .. } => Self::Denied {
                operation,
                key: key.to_string(),
                source: error,
            },
            // All three are a precondition that did not hold: one on a delete or an
            // overwrite, one on a create, one on a conditional read.
            object_store::Error::Precondition { .. }
            | object_store::Error::AlreadyExists { .. }
            | object_store::Error::NotModified { .. } => Self::Precondition {
                operation,
                key: key.to_string(),
            },
            // The backend cannot do this at all, which no amount of asking again fixes.
            object_store::Error::NotSupported { .. }
            | object_store::Error::NotImplemented { .. }
            | object_store::Error::InvalidPath { .. } => Self::Config {
                reason: format!("{operation} on '{key}': {error}"),
            },
            _ => Self::Transient {
                operation,
                source: error,
            },
        }
    }

    /// Whether the store, rather than the request, is what failed.
    pub fn transient(&self) -> bool {
        matches!(self, Self::Transient { .. })
    }

    /// Whether the object is simply not there, which several callers treat as an answer
    /// rather than as a failure.
    pub fn not_found(&self) -> bool {
        matches!(self, Self::NotFound { .. })
    }
}

/// A store result.
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::*;

    fn because() -> Box<dyn std::error::Error + Send + Sync> {
        "because".into()
    }

    /// Every variant a store can answer with, against what a caller is told.
    ///
    /// Here rather than against a store, because a store answers with the handful of
    /// variants it happens to produce, and getting one of the others wrong would not
    /// show up until something far away mistook a refusal for an empty relationship.
    #[test]
    fn what_a_caller_is_told() {
        let key = "backups/dr/dr-west/east/uid-1/dr.cfg";
        let mapped = |error| Error::from_store(Operation::Get, key, error);

        let absent = mapped(object_store::Error::NotFound {
            path: key.to_string(),
            source: because(),
        });
        assert!(absent.not_found());
        assert!(!absent.transient(), "absent is an answer, not a bad moment");

        for refused in [
            object_store::Error::PermissionDenied {
                path: key.to_string(),
                source: because(),
            },
            object_store::Error::Unauthenticated {
                path: key.to_string(),
                source: because(),
            },
        ] {
            let error = mapped(refused);
            assert!(matches!(error, Error::Denied { .. }), "{error}");
            // The distinction the whole mapping exists for: a caller that read a
            // refusal as absence would take a bad credential for an empty relationship.
            assert!(!error.not_found(), "{error}");
        }

        // A precondition that did not hold, whichever call carried it.
        for lost in [
            object_store::Error::Precondition {
                path: key.to_string(),
                source: because(),
            },
            object_store::Error::AlreadyExists {
                path: key.to_string(),
                source: because(),
            },
            object_store::Error::NotModified {
                path: key.to_string(),
                source: because(),
            },
        ] {
            let error = mapped(lost);
            assert!(matches!(error, Error::Precondition { .. }), "{error}");
            assert!(!error.transient(), "asking again answers the same");
        }

        // The backend cannot do this at all, which no amount of asking again fixes.
        for refused in [
            object_store::Error::NotSupported { source: because() },
            object_store::Error::NotImplemented {
                operation: "delete_opts".to_string(),
                implementer: "S3".to_string(),
            },
        ] {
            let error = mapped(refused);
            assert!(matches!(error, Error::Config { .. }), "{error}");
            assert!(!error.transient(), "{error}");
        }

        // Anything else is the store having a moment, and the retries are already spent.
        let generic = mapped(object_store::Error::Generic {
            store: "S3",
            source: because(),
        });
        assert!(generic.transient(), "{generic}");
        assert!(!generic.not_found());
    }
}
