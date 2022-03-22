#![warn(missing_docs)]

/// module for all corresponding client, server, traits for nexus rpc transport
pub mod nexus;

/// module for all corresponding client, server, traits for pool rpc transport
pub mod pool;

/// module for all corresponding client, server, traits for replica rpc transport
pub mod replica;

/// module for all corresponding client, server, traits for volume transport
pub mod volume;

/// module for all corresponding client, server, traits for node transport
pub mod node;

/// module for all corresponding client, server, traits for registration transport
pub mod registration;

/// module for all corresponding client, server, traits for registry transport
pub mod registry;

/// The type of max entries.
pub type MaxEntries = u64;

/// The type of the starting token.
pub type StartingToken = u64;

/// Paginated results.
pub struct PaginatedResult<T> {
    // Results
    result: Vec<T>,
    // Indicates whether or not this is the last paginated result.
    last_result: bool,
}

impl<T> PaginatedResult<T> {
    /// Create a new `PaginatedResult` instance.
    pub fn new(result: Vec<T>, last_result: bool) -> Self {
        Self {
            result,
            last_result,
        }
    }

    /// Returns the result vector.
    pub fn result(self) -> Vec<T> {
        self.result
    }

    /// Return whether or not this is the last result.
    pub fn last(&self) -> bool {
        self.last_result
    }

    /// Length of the results vector.
    pub fn len(&self) -> usize {
        self.result.len()
    }

    /// Returns whether or not there are any results.
    pub fn is_empty(&self) -> bool {
        self.result.is_empty()
    }
}

/// Pagination structure to allow multiple requests to retrieve a large number of entries.
#[derive(Clone, Debug)]
pub struct Pagination {
    // Maximum number of entries to return per request.
    max_entries: MaxEntries,
    // The starting entry for each request.
    starting_token: StartingToken,
}

impl Pagination {
    /// Create a new `Pagination` instance.
    pub fn new(max_entries: MaxEntries, starting_token: StartingToken) -> Self {
        Self {
            max_entries,
            starting_token,
        }
    }

    /// Get the max number of entries.
    pub fn max_entries(&self) -> MaxEntries {
        self.max_entries
    }

    /// Get the starting token
    pub fn starting_token(&self) -> StartingToken {
        self.starting_token
    }
}

impl From<Pagination> for crate::common::Pagination {
    fn from(p: Pagination) -> Self {
        Self {
            max_entries: p.max_entries,
            starting_token: p.starting_token,
        }
    }
}

impl From<crate::common::Pagination> for Pagination {
    fn from(p: crate::common::Pagination) -> Self {
        Self {
            max_entries: p.max_entries,
            starting_token: p.starting_token,
        }
    }
}
