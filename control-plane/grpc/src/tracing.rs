use opentelemetry::{
    global,
    trace::{FutureExt, SpanKind, TraceContextExt, Tracer, TracerProvider},
    Key, KeyValue,
};
use opentelemetry_http::HeaderInjector;
use opentelemetry_semantic_conventions::trace::RPC_GRPC_STATUS_CODE;
use std::{future::Future, pin::Pin};
use tonic::{
    codegen::http::{Request, Response},
    transport::Channel,
};
use tracing_opentelemetry::OpenTelemetrySpanExt;

const HTTP_STATUS_CODE: Key = Key::from_static_str("http.status_code");

/// Add OpenTelemetry Span to the Http Headers
#[derive(Default)]
pub struct OpenTelClient {}
impl OpenTelClient {
    /// Return new `Self
    pub fn new() -> Self {
        Self::default()
    }
}
impl<S> tower::Layer<S> for OpenTelClient {
    type Service = OpenTelClientService<S>;

    fn layer(&self, service: S) -> Self::Service {
        OpenTelClientService::new(service)
    }
}

/// OpenTelemetry Service that injects the current span into the Http Headers
#[derive(Clone)]
pub struct OpenTelClientService<S> {
    service: S,
}
impl<S> OpenTelClientService<S> {
    fn new(service: S) -> Self {
        Self { service }
    }
}

type TonicClientRequest = Request<tonic::body::BoxBody>;
type BoxedFuture<Resp, Err> = Pin<Box<dyn Future<Output = Result<Resp, Err>> + Send>>;

impl tower::Service<TonicClientRequest> for OpenTelClientService<Channel> {
    type Response = <Channel as tower::Service<TonicClientRequest>>::Response;
    type Error = <Channel as tower::Service<TonicClientRequest>>::Error;
    type Future = BoxedFuture<Self::Response, Self::Error>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, mut request: TonicClientRequest) -> Self::Future {
        let tracer = global::tracer("grpc-client");
        let context = tracing::Span::current().context();

        let span = tracer
            .span_builder(format!("{}", request.uri()))
            .with_kind(SpanKind::Client)
            .with_attributes(vec![
                KeyValue::new("rpc.grpc.method", request.method().to_string()),
                KeyValue::new("rpc.grpc.uri", request.uri().to_string()),
                KeyValue::new("rpc.grpc.version", format!("{:?}", request.version())),
            ])
            .start_with_context(&tracer, &context);

        let context = context.with_span(span);
        global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&context, &mut HeaderInjector(request.headers_mut()))
        });
        trace_http_service_call(&mut self.service, request, context)
    }
}

/// Extract OpenTelemetry Spans from Http Headers
#[derive(Clone, Default)]
pub struct OpenTelServer {
    ignored_routes: Vec<&'static str>,
}
impl OpenTelServer {
    /// Return new `Self`
    pub fn new(ignored_routes: Vec<&'static str>) -> Self {
        Self { ignored_routes }
    }
}
impl<S> tower::Layer<S> for OpenTelServer {
    type Service = OpenTelServerService<S>;

    fn layer(&self, service: S) -> Self::Service {
        OpenTelServerService::new(service, self.ignored_routes.clone())
    }
}

/// OpenTelemetry Service that extracts tracing spans from the Http Headers
#[derive(Clone)]
pub struct OpenTelServerService<S> {
    service: S,
    ignored_routes: Vec<&'static str>,
}
impl<S> OpenTelServerService<S> {
    fn new(service: S, ignored_routes: Vec<&'static str>) -> Self {
        Self {
            service,
            ignored_routes,
        }
    }
    fn ignore(&self, request: &TonicServerRequest) -> bool {
        self.ignored_routes.contains(&request.uri().path())
    }
}

type TonicServerRequest = Request<tonic::transport::Body>;
type TonicServerResponse = Response<tonic::body::BoxBody>;
impl<S> tower::Service<TonicServerRequest> for OpenTelServerService<S>
where
    S: tower::Service<TonicServerRequest, Response = TonicServerResponse> + Send + Clone + 'static,
    S::Future: Send,
    S::Error: ToString,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxedFuture<Self::Response, Self::Error>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: TonicServerRequest) -> Self::Future {
        if self.ignore(&request) {
            return http_service_call(&mut self.service, request);
        }

        let tracer = global::tracer_provider().versioned_tracer(
            "grpc-server",
            Some(env!("CARGO_PKG_VERSION")),
            None::<std::borrow::Cow<'static, str>>,
            None,
        );
        let extractor = opentelemetry_http::HeaderExtractor(request.headers());

        let parent_context =
            global::get_text_map_propagator(|propagator| propagator.extract(&extractor));

        let mut builder = tracer.span_builder(request.uri().to_string());
        builder.span_kind = Some(SpanKind::Server);

        let span = tracer.build_with_context(builder, &parent_context);
        let context = parent_context.with_span(span);

        trace_http_service_call(&mut self.service, request, context)
    }
}

/// We cannot simply clone a tower Service as the cloned service may not be ready for calling yet
/// (see `poll_ready` ).
/// The simple solution here is to clone the service but swap the clone with the original, so we can
/// use the original service which is ready.
fn clone_service<
    T: tower::Service<Req, Response = Response<R>, Error = E> + Clone + Send + 'static,
    Req: Send + 'static,
    R,
    E: ToString,
>(
    service: &mut T,
) -> T {
    let service_clone = service.clone();
    std::mem::replace(service, service_clone)
}

fn http_service_call<
    T: tower::Service<Req, Response = Response<R>, Error = E> + Clone + Send + 'static,
    Req: Send + 'static,
    R,
    E: ToString,
>(
    service: &mut T,
    request: Req,
) -> BoxedFuture<Response<R>, E>
where
    <T as tower::Service<Req>>::Future: Send,
{
    let mut service = clone_service(service);
    Box::pin(async move {
        match service.call(request).await {
            Ok(response) => Ok(response),
            Err(error) => Err(error),
        }
    })
}
fn trace_http_service_call<
    T: tower::Service<Req, Response = Response<R>, Error = E> + Clone + Send + 'static,
    Req: Send + 'static,
    R,
    E: ToString,
>(
    service: &mut T,
    request: Req,
    context: opentelemetry::Context,
) -> BoxedFuture<Response<R>, E>
where
    <T as tower::Service<Req>>::Future: Send,
{
    let mut service = clone_service(service);
    Box::pin(async move {
        match service.call(request).with_context(context.clone()).await {
            Ok(response) => {
                update_span_from_response(context, &response);
                Ok(response)
            }
            Err(error) => {
                let span = context.span();
                span.set_status(opentelemetry::trace::Status::error(error.to_string()));
                span.end();
                Err(error)
            }
        }
    })
}
fn update_span_from_response<T>(context: opentelemetry::Context, response: &Response<T>) {
    let span = context.span();
    let grpc_code = match tonic::Status::from_header_map(response.headers()) {
        Some(status) => {
            if status.code() == tonic::Code::Ok {
                span.set_status(opentelemetry::trace::Status::Ok);
            } else {
                span.set_status(opentelemetry::trace::Status::error(status.to_string()));
            }
            status.code()
        }
        None => tonic::Code::Ok,
    };
    span.set_attribute(KeyValue::new(RPC_GRPC_STATUS_CODE, grpc_code as i64));
    span.set_attribute(HTTP_STATUS_CODE.i64(response.status().as_u16() as i64));
    span.end();
}
