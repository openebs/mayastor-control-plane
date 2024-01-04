use crate::{
    types::frontend_node::FrontendNodeId,
    v1,
    v1::common::{ReplyError, ReplyErrorKind, ResourceKind},
};
use serde::{Deserialize, Serialize};

/// Filter for getting resources.
#[derive(Serialize, Deserialize, Debug, Clone, strum_macros::Display)]
pub enum Filter {
    /// All objects.
    None,
    /// Filter by Frontend Node id.
    FrontendNode(FrontendNodeId),
}

impl TryFrom<v1::frontend_node::get_frontend_nodes_request::Filter> for Filter {
    type Error = ReplyError;
    fn try_from(
        filter: v1::frontend_node::get_frontend_nodes_request::Filter,
    ) -> Result<Self, Self::Error> {
        Ok(match filter {
            v1::frontend_node::get_frontend_nodes_request::Filter::FrontendNode(
                frontend_node_filter,
            ) => Filter::FrontendNode(
                FrontendNodeId::try_from(frontend_node_filter.frontend_node_id).map_err(
                    |error| ReplyError {
                        kind: ReplyErrorKind::InvalidArgument as i32,
                        resource: ResourceKind::FrontendNodeKind as i32,
                        source: "frontend_node.id".to_string(),
                        extra: error.to_string(),
                    },
                )?,
            ),
        })
    }
}

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

/// The type of max entries.
pub type MaxEntries = u64;

/// The type of the starting token.
pub type StartingToken = u64;

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

impl From<crate::v1::common::Pagination> for Pagination {
    fn from(p: crate::v1::common::Pagination) -> Self {
        Self {
            max_entries: p.max_entries,
            starting_token: p.starting_token,
        }
    }
}
