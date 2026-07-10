mod authentication;
mod health;
mod v0;

use crate::{
    health::{
        core_state::CachedCoreState,
        handlers::{liveness, readiness},
    },
    v0::{CORE_CLIENT, JSON_GRPC_CLIENT},
};
use actix_service::ServiceFactory;
use actix_web::{
    body::MessageBody,
    dev::{ServiceRequest, ServiceResponse},
    middleware::{self, from_fn, Condition, Next},
    web::Data,
    HttpResponse, HttpServer,
};
use clap::Parser;
use grpc::{client::CoreClient, operations::jsongrpc::client::JsonGrpcClient};
use http::Uri;
use rcgen::generate_simple_self_signed;
use rustls::{
    pki_types::{CertificateDer, PrivateKeyDer},
    server::{danger::ClientCertVerifier, WebPkiClientVerifier},
    RootCertStore, ServerConfig,
};
use rustls_pemfile::{certs, private_key};
use std::{fs::File, io::BufReader, path::PathBuf, sync::Arc, time::Duration};
use stor_port::transport_api::{RequestMinTimeout, TimeoutOptions};
use utils::{
    tracing_telemetry::{FmtLayer, FmtStyle, KeyValue},
    DEFAULT_GRPC_CLIENT_ADDR,
};

#[derive(Debug, Parser)]
#[structopt(name = utils::package_description!(), version = utils::version_info_string!())]
pub(crate) struct CliArgs {
    /// The bind address for the REST interface (with HTTPS).
    #[clap(long, default_value = "[::]:8080")]
    https: String,
    /// The bind address for the REST interface (with HTTP).
    #[clap(long)]
    http: Option<String>,

    /// The bind address for the HTTP liveness and readiness probes interface (with HTTP).
    #[clap(long, conflicts_with = "http")]
    http_probes: Option<String>,

    /// The CORE gRPC Server URL or address to connect to the services.
    #[clap(long, short = 'z', default_value = DEFAULT_GRPC_CLIENT_ADDR)]
    core_grpc: Uri,

    /// Set the frequency of probing the agent-core for a liveness check.
    #[arg(long = "core-health-freq", value_parser = humantime::parse_duration, default_value = "2m")]
    core_liveness_check_frequency: Duration,

    /// The json gRPC Server URL or address to connect to the service.
    #[clap(long, short = 'J')]
    json_grpc: Option<Uri>,

    /// Path to the TLS server certificate chain file.
    #[clap(long = "tls-cert-file", required_unless_present_any = ["dummy_certificates", "auto_tls"])]
    cert_file: Option<PathBuf>,
    /// Path to the TLS server private key file.
    #[clap(long = "tls-key-file", required_unless_present_any = ["dummy_certificates", "auto_tls"])]
    key_file: Option<PathBuf>,

    /// Use dummy HTTPS certificates (for testing).
    #[clap(long, short, required_unless_present_any = ["cert_file", "auto_tls"], conflicts_with_all = ["cert_file", "key_file"])]
    dummy_certificates: bool,

    /// Auto-generate an ephemeral self-signed server certificate for HTTPS.
    #[clap(long, conflicts_with_all = ["dummy_certificates", "cert_file", "key_file"])]
    auto_tls: bool,

    /// Path to the certificate authority (CA) bundle used to authenticate TLS clients.
    /// When specified, TLS client authentication is required.
    #[clap(long = "tls-ca-file", alias = "client-ca-file")]
    client_ca_file: Option<PathBuf>,

    /// Trace rest requests to the Jaeger endpoint agent.
    #[clap(long, short)]
    jaeger: Option<String>,

    /// Path to JSON Web KEY file used for authenticating REST requests.
    #[clap(long, required_unless_present = "no_auth")]
    jwk: Option<PathBuf>,

    /// Don't authenticate REST requests.
    #[clap(long, required_unless_present = "jwk")]
    no_auth: bool,

    /// The default timeout for backend requests issued by the REST Server.
    #[clap(long, short, default_value = utils::DEFAULT_REQ_TIMEOUT)]
    request_timeout: humantime::Duration,

    /// Add process service tags to the traces.
    #[clap(short, long, env = "TRACING_TAGS", value_delimiter=',', value_parser = utils::tracing_telemetry::parse_key_value)]
    tracing_tags: Vec<KeyValue>,

    /// Don't use minimum timeouts for specific requests.
    #[clap(long)]
    no_min_timeouts: bool,

    /// Set number of workers to start.
    /// The value 0 means the number of available physical CPUs is used.
    #[clap(long, short, default_value_t = num_cpus::get_physical())]
    workers: usize,

    /// Set the max number of workers to start.
    /// The value 0 means the number of available physical CPUs is used.
    #[clap(long, short, default_value = utils::DEFAULT_REST_MAX_WORKER_THREADS)]
    max_workers: usize,

    /// Formatting style to be used while logging.
    #[clap(default_value = FmtStyle::Pretty.as_ref(), short, long)]
    fmt_style: FmtStyle,

    /// Use ANSI colors for logs.
    #[clap(long, default_value_t = true, action = clap::ArgAction::Set)]
    ansi_colors: bool,
}
impl CliArgs {
    fn args() -> Self {
        CliArgs::parse()
    }
}

/// default timeout options for every bus request
fn timeout_opts() -> TimeoutOptions {
    let timeout_opts =
        TimeoutOptions::new_no_retries().with_req_timeout(CliArgs::args().request_timeout.into());

    if CliArgs::args().no_min_timeouts {
        timeout_opts.with_min_req_timeout(None)
    } else {
        timeout_opts.with_min_req_timeout(RequestMinTimeout::default())
    }
}

/// Extension trait for actix-web applications.
pub trait OpenApiExt<T> {
    /// configures the App with this version's handlers and openapi generation
    fn configure_api(
        self,
        config: &dyn Fn(actix_web::App<T>) -> actix_web::App<T>,
    ) -> actix_web::App<T>;
}

impl<T, B> OpenApiExt<T> for actix_web::App<T>
where
    B: MessageBody,
    T: ServiceFactory<
        ServiceRequest,
        Config = (),
        Response = ServiceResponse<B>,
        Error = actix_web::Error,
        InitError = (),
    >,
{
    fn configure_api(
        self,
        config: &dyn Fn(actix_web::App<T>) -> actix_web::App<T>,
    ) -> actix_web::App<T> {
        config(self)
    }
}

fn get_certificates() -> anyhow::Result<ServerConfig> {
    let client_ca_file = CliArgs::args().client_ca_file;

    if CliArgs::args().auto_tls {
        get_auto_certificates(client_ca_file)
    } else if CliArgs::args().dummy_certificates {
        get_dummy_certificates()
    } else {
        // guaranteed to be `Some` by the require_unless attribute
        let cert_file = CliArgs::args().cert_file.expect("cert_file is required");
        let key_file = CliArgs::args().key_file.expect("key_file is required");
        let cert_file = &mut BufReader::new(File::open(cert_file)?);
        let key_file = &mut BufReader::new(File::open(key_file)?);
        let mut client_ca = match client_ca_file {
            Some(path) => Some(BufReader::new(File::open(path)?)),
            None => None,
        };

        load_certificates(
            cert_file,
            key_file,
            client_ca
                .as_mut()
                .map(|file| file as &mut dyn std::io::BufRead),
        )
    }
}

fn get_dummy_certificates() -> anyhow::Result<ServerConfig> {
    let cert_file = &mut BufReader::new(&std::include_bytes!("../../certs/rsa/user.chain")[..]);
    let key_file = &mut BufReader::new(&std::include_bytes!("../../certs/rsa/user.rsa")[..]);
    let mut client_ca = BufReader::new(&std::include_bytes!("../../certs/rsa/ca.cert")[..]);

    load_certificates(
        cert_file,
        key_file,
        Some(&mut client_ca as &mut dyn std::io::BufRead),
    )
}

fn get_auto_certificates(client_ca_file: Option<PathBuf>) -> anyhow::Result<ServerConfig> {
    let cert_material = generate_simple_self_signed(vec!["localhost".to_string()])
        .map_err(|error| anyhow::anyhow!("Failed to generate self-signed certificate: {error}"))?;

    let cert_pem = cert_material.cert.pem();
    let key_pem = cert_material.key_pair.serialize_pem();

    let cert_file = &mut BufReader::new(cert_pem.as_bytes());
    let key_file = &mut BufReader::new(key_pem.as_bytes());
    let mut client_ca = match client_ca_file {
        Some(path) => Some(BufReader::new(File::open(path)?)),
        None => None,
    };

    load_certificates(
        cert_file,
        key_file,
        client_ca
            .as_mut()
            .map(|file| file as &mut dyn std::io::BufRead),
    )
}

fn load_certificates(
    cert_file: &mut dyn std::io::BufRead,
    key_file: &mut dyn std::io::BufRead,
    client_ca_file: Option<&mut dyn std::io::BufRead>,
) -> anyhow::Result<ServerConfig> {
    let config = ServerConfig::builder();
    let cert_chain: Vec<CertificateDer<'static>> = certs(cert_file)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| {
            anyhow::anyhow!("Failed to retrieve certificates from the certificate file")
        })?;
    let key: PrivateKeyDer<'static> = private_key(key_file)
        .map_err(|_| anyhow::anyhow!("Failed to retrieve private key from the key file"))?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "No private key found in the key file (expected a PEM key like PKCS#8, PKCS#1, or SEC1)"
            )
        })?;

    let config = config
        .with_client_cert_verifier(client_cert_verifier(client_ca_file)?)
        .with_single_cert(cert_chain, key)?;

    Ok(config)
}

fn client_cert_verifier(
    client_ca_file: Option<&mut dyn std::io::BufRead>,
) -> anyhow::Result<Arc<dyn ClientCertVerifier>> {
    let Some(ca_file) = client_ca_file else {
        return Ok(WebPkiClientVerifier::no_client_auth());
    };
    let client_certs = certs(ca_file)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| anyhow::anyhow!("Failed to retrieve certificates from client CA file"))?;

    if client_certs.is_empty() {
        anyhow::bail!("No certificates found in the client CA file");
    }

    let mut roots = RootCertStore::empty();
    let (valid, invalid) = roots.add_parsable_certificates(client_certs);
    if valid == 0 {
        anyhow::bail!("No valid certificates found in the client CA file");
    }
    if invalid > 0 {
        tracing::warn!(
            ignored = invalid,
            "Some certificates from the client CA file were ignored"
        );
    }

    WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .map_err(|error| {
            anyhow::anyhow!("Failed to configure client certificate verifier: {error}")
        })
}

fn get_jwk_path() -> Option<PathBuf> {
    match CliArgs::args().jwk {
        Some(path) => Some(path),
        None => match CliArgs::args().no_auth {
            true => None,
            false => panic!("Cannot authenticate without a JWK file"),
        },
    }
}

/// Only the liveness (`/live`) and readiness (`/ready`) probes are served over an
/// insecure (plain HTTP) connection.
/// Every other route is rejected with `421 Misdirected Request`, since the full API
/// is only served on the separate HTTPS port.
async fn probes_only_on_insecure(
    req: ServiceRequest,
    next: Next<impl MessageBody + 'static>,
) -> Result<ServiceResponse, actix_web::Error> {
    let is_probe = matches!(req.path(), "/live" | "/ready");
    if req.app_config().secure() || is_probe {
        Ok(next.call(req).await?.map_into_boxed_body())
    } else {
        let response = HttpResponse::MisdirectedRequest().finish();
        Ok(req.into_response(response))
    }
}

fn workers(args: &CliArgs) -> usize {
    let max_workers = match args.max_workers {
        0 => num_cpus::get_physical(),
        max => max,
    };

    let workers = match args.workers {
        0 => num_cpus::get_physical(),
        workers => workers,
    };

    workers.clamp(1, max_workers)
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    utils::print_package_info!();
    let cli_args = CliArgs::args();
    println!("Using options: {:?}", &cli_args);

    utils::tracing_telemetry::TracingTelemetry::builder()
        .with_writer(FmtLayer::Stdout)
        .with_style(cli_args.fmt_style)
        .with_colours(cli_args.ansi_colors)
        .with_jaeger(cli_args.jaeger.clone())
        .with_tracing_tags(cli_args.tracing_tags.clone())
        .init("rest-server");

    // Initialize the core client to be used in rest
    CORE_CLIENT
        .set(CoreClient::new(cli_args.core_grpc, timeout_opts()).await)
        .ok()
        .expect("Expect to be initialised only once");

    let cached_core_state = Data::new(CachedCoreState::new(cli_args.core_liveness_check_frequency));

    let restrict_http_to_probes = cli_args.http_probes.is_some();

    let app = move || {
        actix_web::App::new()
            .app_data(cached_core_state.clone())
            .service(liveness)
            .service(readiness)
            // Restrict everything except the liveness/readiness probes to secure (HTTPS) connections
            .wrap(Condition::new(
                restrict_http_to_probes,
                from_fn(probes_only_on_insecure),
            ))
            .wrap(tracing_actix_web::TracingLogger::default())
            .wrap(middleware::Logger::default())
            .app_data(authentication::init(get_jwk_path()))
            .configure_api(&v0::configure_api)
    };

    // Initialize the json grpc client to be used in rest
    if let Some(json_grpc) = CliArgs::args().json_grpc {
        JSON_GRPC_CLIENT
            .set(JsonGrpcClient::new(json_grpc, timeout_opts()).await)
            .ok()
            .expect("Expect to be initialised only once");
    }

    let server =
        HttpServer::new(app).bind_rustls_0_23(CliArgs::args().https, get_certificates()?)?;
    let result = if let Some(http) = CliArgs::args().http {
        server.bind(http).map_err(anyhow::Error::from)?
    } else if let Some(http_probes) = CliArgs::args().http_probes {
        server.bind(http_probes).map_err(anyhow::Error::from)?
    } else {
        server
    }
    .workers(workers(&CliArgs::args()))
    .run()
    .await;

    utils::tracing_telemetry::flush_traces();

    result.map_err(|e| e.into())
}
