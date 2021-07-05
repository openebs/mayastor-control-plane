#![allow(
    missing_docs,
    trivial_casts,
    unused_variables,
    unused_mut,
    unused_imports,
    unused_extern_crates,
    non_camel_case_types
)]

use actix_web::{
    web::{self, Json, Path, Query, ServiceConfig},
    FromRequest, HttpRequest,
};

/// Configure handlers for the ChildrenApi resource
pub fn configure<T: crate::apis::ChildrenApi + 'static, A: FromRequest + 'static>(
    cfg: &mut ServiceConfig,
) {
    cfg.service(
        actix_web::web::resource("/nexuses/{nexus_id}/children/{child_id:.*}")
            .name("del_nexus_child")
            .guard(actix_web::guard::Delete())
            .route(actix_web::web::delete().to(del_nexus_child::<T, A>)),
    )
    .service(
        actix_web::web::resource("/nodes/{node_id}/nexuses/{nexus_id}/children/{child_id:.*}")
            .name("del_node_nexus_child")
            .guard(actix_web::guard::Delete())
            .route(actix_web::web::delete().to(del_node_nexus_child::<T, A>)),
    )
    .service(
        actix_web::web::resource("/nexuses/{nexus_id}/children/{child_id:.*}")
            .name("get_nexus_child")
            .guard(actix_web::guard::Get())
            .route(actix_web::web::get().to(get_nexus_child::<T, A>)),
    )
    .service(
        actix_web::web::resource("/nexuses/{nexus_id}/children")
            .name("get_nexus_children")
            .guard(actix_web::guard::Get())
            .route(actix_web::web::get().to(get_nexus_children::<T, A>)),
    )
    .service(
        actix_web::web::resource("/nodes/{node_id}/nexuses/{nexus_id}/children/{child_id:.*}")
            .name("get_node_nexus_child")
            .guard(actix_web::guard::Get())
            .route(actix_web::web::get().to(get_node_nexus_child::<T, A>)),
    )
    .service(
        actix_web::web::resource("/nodes/{node_id}/nexuses/{nexus_id}/children")
            .name("get_node_nexus_children")
            .guard(actix_web::guard::Get())
            .route(actix_web::web::get().to(get_node_nexus_children::<T, A>)),
    )
    .service(
        actix_web::web::resource("/nexuses/{nexus_id}/children/{child_id:.*}")
            .name("put_nexus_child")
            .guard(actix_web::guard::Put())
            .route(actix_web::web::put().to(put_nexus_child::<T, A>)),
    )
    .service(
        actix_web::web::resource("/nodes/{node_id}/nexuses/{nexus_id}/children/{child_id:.*}")
            .name("put_node_nexus_child")
            .guard(actix_web::guard::Put())
            .route(actix_web::web::put().to(put_node_nexus_child::<T, A>)),
    );
}

async fn del_nexus_child<T: crate::apis::ChildrenApi + 'static, A: FromRequest + 'static>(
    request: HttpRequest,
    _token: A,
    Path((nexus_id, child_id_)): Path<(String, String)>,
) -> Result<Json<()>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::del_nexus_child(request.query_string(), Path((nexus_id, child_id_)))
        .await
        .map(|_| Json(()))
}

async fn del_node_nexus_child<T: crate::apis::ChildrenApi + 'static, A: FromRequest + 'static>(
    request: HttpRequest,
    _token: A,
    Path((node_id, nexus_id, child_id_)): Path<(String, String, String)>,
) -> Result<Json<()>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::del_node_nexus_child(request.query_string(), Path((node_id, nexus_id, child_id_)))
        .await
        .map(|_| Json(()))
}

async fn get_nexus_child<T: crate::apis::ChildrenApi + 'static, A: FromRequest + 'static>(
    request: HttpRequest,
    _token: A,
    Path((nexus_id, child_id_)): Path<(String, String)>,
) -> Result<Json<crate::models::Child>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::get_nexus_child(request.query_string(), Path((nexus_id, child_id_))).await
}

async fn get_nexus_children<T: crate::apis::ChildrenApi + 'static, A: FromRequest + 'static>(
    _token: A,
    Path(nexus_id): Path<String>,
) -> Result<Json<Vec<crate::models::Child>>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::get_nexus_children(Path(nexus_id)).await
}

async fn get_node_nexus_child<T: crate::apis::ChildrenApi + 'static, A: FromRequest + 'static>(
    request: HttpRequest,
    _token: A,
    Path((node_id, nexus_id, child_id_)): Path<(String, String, String)>,
) -> Result<Json<crate::models::Child>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::get_node_nexus_child(request.query_string(), Path((node_id, nexus_id, child_id_))).await
}

async fn get_node_nexus_children<
    T: crate::apis::ChildrenApi + 'static,
    A: FromRequest + 'static,
>(
    _token: A,
    Path((node_id, nexus_id)): Path<(String, String)>,
) -> Result<Json<Vec<crate::models::Child>>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::get_node_nexus_children(Path((node_id, nexus_id))).await
}

async fn put_nexus_child<T: crate::apis::ChildrenApi + 'static, A: FromRequest + 'static>(
    request: HttpRequest,
    _token: A,
    Path((nexus_id, child_id_)): Path<(String, String)>,
) -> Result<Json<crate::models::Child>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::put_nexus_child(request.query_string(), Path((nexus_id, child_id_))).await
}

async fn put_node_nexus_child<T: crate::apis::ChildrenApi + 'static, A: FromRequest + 'static>(
    request: HttpRequest,
    _token: A,
    Path((node_id, nexus_id, child_id_)): Path<(String, String, String)>,
) -> Result<Json<crate::models::Child>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::put_node_nexus_child(request.query_string(), Path((node_id, nexus_id, child_id_))).await
}
