#![allow(
    missing_docs,
    trivial_casts,
    unused_variables,
    unused_mut,
    unused_imports,
    unused_extern_crates,
    non_camel_case_types
)]

use actix_web::web::{self, Json, Path, Query};

#[async_trait::async_trait]
pub trait WatchesApi {
    async fn del_watch_volume(
        Path(volume_id): Path<String>,
        callback: url::Url,
    ) -> Result<(), crate::apis::RestError<crate::models::RestJsonError>>;
    async fn get_watch_volume(
        Path(volume_id): Path<String>,
    ) -> Result<
        Json<Vec<crate::models::RestWatch>>,
        crate::apis::RestError<crate::models::RestJsonError>,
    >;
    async fn put_watch_volume(
        Path(volume_id): Path<String>,
        callback: url::Url,
    ) -> Result<(), crate::apis::RestError<crate::models::RestJsonError>>;
}
