use actix_web::{dev::Handler, web, Error, HttpResponse};
use futures::future::{ok as fut_ok, Ready};
use tinytemplate::TinyTemplate;

async fn get_v0_spec() -> HttpResponse {
    let spec_str = include_str!("../../../openapi-specs/v0_api_spec.yaml");
    match serde_yaml::from_str::<serde_json::Value>(spec_str) {
        Ok(value) => HttpResponse::Ok().json(value),
        Err(error) => HttpResponse::InternalServerError()
            .json(serde_json::json!({ "error": error.to_string() })),
    }
}

pub(super) fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource(&super::spec_uri()).route(web::get().to(get_v0_spec)))
        .service(
            web::resource(&format!("{}/swagger-ui", super::version()))
                .route(web::get().to(GetSwaggerUi(get_swagger_html(&super::spec_uri())))),
        );
}

static TEMPLATE: &str = include_str!("./resources/swagger-ui.html");
fn get_swagger_html(spec_uri: &str) -> Result<String, String> {
    let context = serde_json::json!({ "api_spec_uri": spec_uri });
    let mut template = TinyTemplate::new();
    template
        .add_template("swagger-ui", TEMPLATE)
        .map_err(|e| e.to_string())?;
    template
        .render("swagger-ui", &context)
        .map_err(|e| e.to_string())
}

#[derive(Clone)]
struct GetSwaggerUi(Result<String, String>);

impl Handler<()> for GetSwaggerUi {
    type Output = Result<HttpResponse, Error>;
    type Future = Ready<Self::Output>;

    fn call(&self, _: ()) -> Self::Future {
        match &self.0 {
            Ok(html) => fut_ok(
                HttpResponse::Ok()
                    .content_type("text/html")
                    .body(html.clone()),
            ),
            Err(error) => fut_ok(
                HttpResponse::NotFound()
                    .content_type("application/json")
                    .body(serde_json::json!({ "error_message": error }).to_string()),
            ),
        }
    }
}
