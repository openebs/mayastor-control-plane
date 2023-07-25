#[macro_use]
extern crate prettytable;
#[macro_use]
extern crate lazy_static;

pub mod operations;
pub mod resources;
pub mod rest_wrapper;

/// Initialize tracing (including opentelemetry).
pub fn init_tracing(jaeger: Option<&String>) {
    let git_version = option_env!("GIT_VERSION").unwrap_or_else(utils::raw_version_str);
    let tags =
        utils::tracing_telemetry::default_tracing_tags(git_version, env!("CARGO_PKG_VERSION"));

    let fmt_layer = match std::env::var("RUST_LOG") {
        Ok(_) => utils::tracing_telemetry::FmtLayer::Stderr,
        Err(_) => utils::tracing_telemetry::FmtLayer::None,
    };

    utils::tracing_telemetry::init_tracing_ext(
        env!("CARGO_PKG_NAME"),
        tags,
        jaeger,
        fmt_layer,
        None,
    );
}
