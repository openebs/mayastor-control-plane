#![allow(
    clippy::too_many_arguments,
    clippy::new_without_default,
    non_camel_case_types
)]
/*
 * Mayastor RESTful API
 *
 * The version of the OpenAPI document: v0
 *
 * Generated by: https://github.com/openebs/openapi-generator
 */

/// Node : Node information

/// Node information
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Node {
    /// grpc_endpoint of the mayastor instance
    #[serde(rename = "grpcEndpoint")]
    pub grpc_endpoint: String,
    /// id of the mayastor instance
    #[serde(rename = "id")]
    pub id: String,
    #[serde(rename = "state")]
    pub state: crate::models::NodeState,
}

impl Node {
    /// Node using only the required fields
    pub fn new(grpc_endpoint: String, id: String, state: crate::models::NodeState) -> Node {
        Node {
            grpc_endpoint,
            id,
            state,
        }
    }
    /// Node using all fields
    pub fn new_all(grpc_endpoint: String, id: String, state: crate::models::NodeState) -> Node {
        Node {
            grpc_endpoint,
            id,
            state,
        }
    }
}
