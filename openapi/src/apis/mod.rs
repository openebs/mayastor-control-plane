pub use actix_web::http::StatusCode;
pub use url::Url;
pub use uuid::Uuid;

use actix_web::{
    web::{HttpResponse, ServiceConfig},
    FromRequest, ResponseError,
};
use serde::Serialize;
use std::{
    fmt::{self, Debug, Display, Formatter},
    ops,
};

pub mod block_devices_api_handlers;
pub mod children_api_handlers;
pub mod json_grpc_api_handlers;
pub mod nexuses_api_handlers;
pub mod nodes_api_handlers;
pub mod pools_api_handlers;
pub mod replicas_api_handlers;
pub mod specs_api_handlers;
pub mod volumes_api_handlers;
pub mod watches_api_handlers;

/// Rest Error wrapper with a status code and a JSON error
/// Note: Only a single error type for each handler is supported at the moment
pub struct RestError<T: Debug + Serialize> {
    status_code: StatusCode,
    error_response: T,
}

impl<T: Debug + Serialize> RestError<T> {
    pub fn new(status_code: StatusCode, error_response: T) -> Self {
        Self {
            status_code,
            error_response,
        }
    }
}

impl<T: Debug + Serialize> Debug for RestError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RestError")
            .field("status_code", &self.status_code)
            .field("error_response", &self.error_response)
            .finish()
    }
}

impl<T: Debug + Serialize> Display for RestError<T> {
    fn fmt(&self, _: &mut Formatter<'_>) -> fmt::Result {
        unimplemented!()
    }
}

impl<T: Debug + Serialize> ResponseError for RestError<T> {
    fn status_code(&self) -> StatusCode {
        self.status_code
    }

    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code).json2(&self.error_response)
    }
}

/// Wrapper type used as tag to easily distinguish the 3 different parameter types:
/// 1. Path 2. Query 3. Body
/// Example usage:
/// fn delete_resource(Path((p1, p2)): Path<(String, u64)>) { ... }
pub struct Path<T>(pub T);

impl<T> Path<T> {
    /// Deconstruct to an inner value
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> AsRef<T> for Path<T> {
    fn as_ref(&self) -> &T {
        &self.0
    }
}

impl<T> ops::Deref for Path<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T> ops::DerefMut for Path<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

/// Wrapper type used as tag to easily distinguish the 3 different parameter types:
/// 1. Path 2. Query 3. Body
/// Example usage:
/// fn delete_resource(Path((p1, p2)): Path<(String, u64)>) { ... }
pub struct Query<T>(pub T);

impl<T> Query<T> {
    /// Deconstruct to an inner value
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> AsRef<T> for Query<T> {
    fn as_ref(&self) -> &T {
        &self.0
    }
}

impl<T> ops::Deref for Query<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T> ops::DerefMut for Query<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

/// Wrapper type used as tag to easily distinguish the 3 different parameter types:
/// 1. Path 2. Query 3. Body
/// Example usage:
/// fn delete_resource(Path((p1, p2)): Path<(String, u64)>) { ... }
pub struct Body<T>(pub T);

impl<T> Body<T> {
    /// Deconstruct to an inner value
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> AsRef<T> for Body<T> {
    fn as_ref(&self) -> &T {
        &self.0
    }
}

impl<T> ops::Deref for Body<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T> ops::DerefMut for Body<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

/// Configure all actix server handlers
pub fn configure<
    T: BlockDevices
        + Children
        + JsonGrpc
        + Nexuses
        + Nodes
        + Pools
        + Replicas
        + Specs
        + Volumes
        + Watches
        + 'static,
    A: FromRequest + 'static,
>(
    cfg: &mut ServiceConfig,
) {
    block_devices_api_handlers::configure::<T, A>(cfg);
    children_api_handlers::configure::<T, A>(cfg);
    json_grpc_api_handlers::configure::<T, A>(cfg);
    nexuses_api_handlers::configure::<T, A>(cfg);
    nodes_api_handlers::configure::<T, A>(cfg);
    pools_api_handlers::configure::<T, A>(cfg);
    replicas_api_handlers::configure::<T, A>(cfg);
    specs_api_handlers::configure::<T, A>(cfg);
    volumes_api_handlers::configure::<T, A>(cfg);
    watches_api_handlers::configure::<T, A>(cfg);
}

mod block_devices_api;
pub use self::block_devices_api::BlockDevices;
mod children_api;
pub use self::children_api::Children;
mod json_grpc_api;
pub use self::json_grpc_api::JsonGrpc;
mod nexuses_api;
pub use self::nexuses_api::Nexuses;
mod nodes_api;
pub use self::nodes_api::Nodes;
mod pools_api;
pub use self::pools_api::Pools;
mod replicas_api;
pub use self::replicas_api::Replicas;
mod specs_api;
pub use self::specs_api::Specs;
mod volumes_api;
pub use self::volumes_api::Volumes;
mod watches_api;
pub use self::watches_api::Watches;
