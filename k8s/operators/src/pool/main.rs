//! K8S pool operator watches for pool CRs and creates the pool on the given node.
//! There is a maximum retry limit that will put the pool into a steady error state.
//!
//! Successfully created pools are recreated by the control plane.

pub(crate) mod context;
mod diskpool;
pub(crate) mod error;
mod mayastorpool;

use crate::diskpool::client::{
    create_crd, create_missing_cr, create_v1beta3_cr, dsp_api, runtime_api_version,
};

use context::OperatorContext;
use diskpool::crd::{
    migration::ensure_and_migrate_crd,
    v1beta3::{CrPoolState, DiskPool, DiskPoolSpec, DiskPoolStatus},
};
use error::Error;
use mayastorpool::client::{check_crd, delete, list};
use openapi::clients::{self, tower::Url};
use tracing::{error, info, trace, warn};
use utils::tracing_telemetry::{FmtLayer, FmtStyle};

use chrono::Utc;
use clap::{Arg, ArgAction, ArgMatches};
use futures::StreamExt;
use kube::{
    api::Api,
    runtime::{
        controller::{Action, Controller},
        watcher,
    },
    Client, CustomResourceExt, Resource, ResourceExt,
};
use std::{collections::HashMap, fs::File, io::Write, path::Path, sync::Arc, time::Duration};
use strum_macros::{Display, EnumString};

const PAGINATION_LIMIT: u32 = 100;
const BACKOFF_PERIOD: u64 = 20;
/// Determine what we want to do when dealing with errors from the
/// reconciliation loop
fn error_policy(_object: Arc<DiskPool>, error: &Error, _ctx: Arc<OperatorContext>) -> Action {
    let duration = Duration::from_secs(BACKOFF_PERIOD);

    let when = Utc::now()
        .checked_add_signed(chrono::Duration::from_std(duration).unwrap())
        .unwrap();
    warn!(
        "{}, retry scheduled @{} ({} seconds from now)",
        error,
        when.to_rfc2822(),
        duration.as_secs()
    );
    Action::requeue(duration)
}

/// The main work horse
#[tracing::instrument(fields(name = %dsp.spec.node(), status = ?dsp.status) skip(dsp, ctx))]
async fn reconcile(dsp: Arc<DiskPool>, ctx: Arc<OperatorContext>) -> Result<Action, Error> {
    let dsp = ctx.upsert(ctx.clone(), dsp).await;
    let _ = dsp.finalizer().await;

    if !ctx.inventory_contains(dsp.name_any()).await {
        return Ok(Action::await_change());
    }

    match dsp.status {
        Some(DiskPoolStatus {
            cr_state: CrPoolState::Creating,
            ..
        }) => dsp.create_or_import().await,
        Some(DiskPoolStatus {
            cr_state: CrPoolState::Created,
            ..
        })
        | Some(DiskPoolStatus {
            cr_state: CrPoolState::Terminating,
            ..
        }) => dsp.pool_check().await,
        // We use this state to indicate it's a new CRD however, we could (and
        // perhaps should) use the finalizer callback.
        None => dsp.init_cr().await,
    }
}
/// Previous Api versions for the [`DiskPool`].
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Display, EnumString)]
#[strum(serialize_all = "lowercase")]
pub enum PrevApiVersion {
    /// Represents v1alpha1
    V1Alpha1,
    /// Represents v1beta1
    V1Beta1,
    /// Represents v1beta2
    V1Beta2,
}

/// Current Api versions for the [`DiskPool`].
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub enum ApiVersion {
    Deprecated(PrevApiVersion),
    Latest,
}
impl From<PrevApiVersion> for ApiVersion {
    fn from(value: PrevApiVersion) -> Self {
        Self::Deprecated(value)
    }
}
impl std::fmt::Display for ApiVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            ApiVersion::Deprecated(version) => version.to_string(),
            ApiVersion::Latest => DiskPool::version(&()).to_string(),
        };
        write!(f, "{s}")
    }
}
impl std::str::FromStr for ApiVersion {
    type Err = strum::ParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == Self::Latest.to_string() {
            Ok(Self::Latest)
        } else {
            Ok(s.parse::<PrevApiVersion>()?.into())
        }
    }
}

impl ApiVersion {
    /// The [`ApiVersion`] we want to use.
    /// > It should be the latest version.
    pub fn validate_latest() -> anyhow::Result<()> {
        use kube::Resource;
        let version = DiskPool::version(&());
        let latest: Self = version.parse().map_err(|e| {
            anyhow::anyhow!("Please update ApiVersion to account for {version}: {e}")
        })?;

        anyhow::ensure!(latest == Self::Latest, "Please update ApiVersion::Latest");

        Ok(())
    }
}
async fn pool_controller(args: ArgMatches) -> anyhow::Result<()> {
    let k8s = Client::try_default().await?;
    let namespace = args.get_one::<String>("namespace").unwrap();
    let api_version = runtime_api_version(k8s.clone()).await?;

    ApiVersion::validate_latest()?;

    match api_version {
        Some(version) => {
            ensure_and_migrate_crd(k8s.clone(), namespace, version).await?;
        }
        None => {
            create_crd(k8s.clone()).await?;
        }
    }

    // Migrate the MayastorPool CRs to the DiskPool.
    migrate_and_clean_msps(&k8s, namespace).await?;

    let newdsp: Api<DiskPool> = dsp_api(&k8s, namespace);

    let url = Url::parse(args.get_one::<String>("endpoint").unwrap())
        .expect("endpoint is not a valid URL");

    let timeout: Duration = args
        .get_one::<String>("request-timeout")
        .unwrap()
        .parse::<humantime::Duration>()
        .expect("timeout value is invalid")
        .into();

    let ca_certificate_path: Option<&str> = args
        .get_one::<String>("tls-client-ca-path")
        .map(|x| x.as_str());
    // take in cert path and make pem file
    let cert = match ca_certificate_path {
        Some(path) => {
            let cert = std::fs::read(path).map_err(|error| {
                anyhow::anyhow!("Failed to read certificate file, Error: '{:?}'", error)
            })?;
            Some(cert)
        }
        None => None,
    };
    let cfg = match (url.scheme(), cert) {
        ("https", Some(cert)) => clients::tower::Configuration::new(
            url,
            timeout,
            None,
            Some(cert.as_slice()),
            true,
            None,
        )
        .map_err(|error| {
            anyhow::anyhow!(
                "Failed to create openapi configuration, Error: '{:?}'",
                error
            )
        })?,
        ("https", None) => {
            anyhow::bail!("HTTPS endpoint requires a CA certificate path");
        }
        (_, Some(_path)) => {
            anyhow::bail!("CA certificate path is only supported for HTTPS endpoints");
        }
        _ => clients::tower::Configuration::new(url, timeout, None, None, true, None).map_err(
            |error| {
                anyhow::anyhow!(
                    "Failed to create openapi configuration, Error: '{:?}'",
                    error
                )
            },
        )?,
    };
    let interval = args
        .get_one::<String>("interval")
        .unwrap()
        .parse::<humantime::Duration>()
        .expect("interval value is invalid")
        .as_secs();
    let context = OperatorContext::new(
        k8s.clone(),
        tokio::sync::RwLock::new(HashMap::new()),
        clients::tower::ApiClient::new(cfg.clone()),
        interval,
    );

    create_missing_cr(&k8s, clients::tower::ApiClient::new(cfg.clone()), namespace).await?;

    info!(namespace, "Starting DiskPool Operator (dsp)");

    Controller::new(newdsp, watcher::Config::default())
        .run(reconcile, error_policy, Arc::new(context))
        .for_each(|res| async move {
            match res {
                Ok(o) => {
                    trace!(?o);
                }
                Err(e) => {
                    trace!(?e);
                }
            }
        })
        .await;

    Ok(())
}

/// Write the DiskPool CRD to a file
fn write_diskpool_crd(output_dir: &str) -> anyhow::Result<()> {
    // Generate the CRD
    let crd = DiskPool::crd();

    let str = serde_json::to_string_pretty(&crd)?;

    // Create output directory if it doesn't exist
    std::fs::create_dir_all(output_dir)?;

    // Write to file
    let output_path = Path::new(output_dir).join("diskpools.crd.yaml");
    let mut file = File::create(output_path)?;
    file.write_all(str.as_ref())?;

    println!("DiskPool CRD generated successfully at {output_dir:?}/diskpools.crd.yaml");
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let matches = clap::Command::new(utils::package_description!())
        .version(utils::version_info_str!())
        .arg(
            Arg::new("generate-crd")
                .long("generate-crd")
                .action(ArgAction::SetTrue)
                .help("Generate the DiskPool CRD and write it to a file"),
        )
        .arg(
            Arg::new("output-dir")
                .long("output-dir")
                .default_value("./generated-crds")
                .help("Directory where the generated CRD will be written"),
        )
        .arg(
            Arg::new("interval")
                .short('i')
                .long("interval")
                .env("INTERVAL")
                .default_value(utils::CACHE_POLL_PERIOD)
                .help("specify timer based reconciliation loop"),
        )
        .arg(
            Arg::new("request-timeout")
                .short('t')
                .long("request-timeout")
                .env("REQUEST_TIMEOUT")
                .default_value(utils::DEFAULT_REQ_TIMEOUT)
                .help("the timeout for remote requests"),
        )
        .arg(
            Arg::new("retries")
                .long("retries")
                .short('r')
                .env("RETRIES")
                .value_parser(clap::value_parser!(u32).range(1..))
                .default_value("10")
                .help("the number of retries before we set the resource into the error state"),
        )
        .arg(
            Arg::new("endpoint")
                .long("endpoint")
                .short('e')
                .env("ENDPOINT")
                .default_value("http://ksnode-1:30011")
                .help("an URL endpoint to the control plane's rest endpoint"),
        )
        .arg(
            Arg::new("namespace")
                .long("namespace")
                .short('n')
                .env("NAMESPACE")
                .default_value("mayastor")
                .help("the default namespace we are supposed to operate in"),
        )
        .arg(
            Arg::new("jaeger")
                .short('j')
                .long("jaeger")
                .env("JAEGER_ENDPOINT")
                .help("enable open telemetry and forward to jaeger"),
        )
        .arg(
            Arg::new("disable-device-validation")
                .long("disable-device-validation")
                .action(clap::ArgAction::SetTrue)
                .help("do not attempt to validate the block device prior to pool creation"),
        )
        .arg(
            Arg::new("fmt-style")
                .long("fmt-style")
                .default_value(FmtStyle::Pretty.as_ref())
                .value_parser(clap::value_parser!(FmtStyle))
                .help("Formatting style to be used while logging"),
        )
        .arg(
            Arg::new("ansi-colors")
                .long("ansi-colors")
                .default_value("true")
                .value_parser(clap::value_parser!(bool))
                .help("Enable ansi color for logs"),
        )
        .arg(
            Arg::new("tls-client-ca-path")
                .long("tls-client-ca-path")
                .help("path to the CA certificate file"),
        )
        .get_matches();

    utils::print_package_info!();

    // Check if we should generate the CRD
    if matches.get_flag("generate-crd") {
        let output_dir = matches.get_one::<String>("output-dir").unwrap();
        return write_diskpool_crd(output_dir);
    }

    let tags = utils::tracing_telemetry::default_tracing_tags(
        utils::raw_version_str(),
        env!("CARGO_PKG_VERSION"),
    );

    let fmt_style = matches.get_one::<FmtStyle>("fmt-style").unwrap();
    let ansi_colors = matches.get_flag("ansi-colors");
    utils::tracing_telemetry::TracingTelemetry::builder()
        .with_writer(FmtLayer::Stdout)
        .with_style(*fmt_style)
        .with_colours(ansi_colors)
        .with_jaeger(matches.get_one::<String>("jaeger").cloned())
        .with_tracing_tags(tags)
        .init("dsp-operator");

    pool_controller(matches).await?;
    utils::tracing_telemetry::flush_traces();
    Ok(())
}

/// Migrate from MayastorPool.
pub(crate) async fn migrate_and_clean_msps(k8s: &Client, namespace: &str) -> Result<(), Error> {
    // Check if the MayastorPool CRD is present, and migrate from it if it is.
    match check_crd(k8s).await {
        // Fetch the MayastorPool CRs.
        Ok(true) => match list(k8s, namespace, PAGINATION_LIMIT).await {
            Ok(mut msps) => {
                for msp in msps.iter_mut() {
                    let name = msp.clone().metadata.name.ok_or(Error::InvalidCRField {
                        field: "diskpool.metadata.name".to_string(),
                    })?;
                    let node = msp.spec.node();
                    let disks = msp.spec.disks();
                    // Create the corresponding v1beta3 DiskPool CRs.
                    // Hardcoding encryption and cluster_size to be none as it did not exist at that point.
                    if let Err(error) = create_v1beta3_cr(
                        k8s,
                        namespace,
                        &name,
                        DiskPoolSpec::new(node, disks, None, None, None, None),
                    )
                    .await
                    {
                        error!("Migration failed for {name} with: {error:?}");
                    }
                    // Patch the finalizers and delete the MayastorPool CRs.
                    if let Err(error) = delete(k8s, namespace, msp).await {
                        error!("Deletion failed for {name}  with: {error:?}");
                    }
                }
                info!("Migration and Cleanup of CRs from MayastorPool to DiskPool complete");
            }
            Err(error) => {
                return Err(Error::Generic {
                    message: format!("Failed to list MayastorPool CRs: {error:?}"),
                })
            }
        },
        Ok(false) => info!("MayastorPool CRD was not found in the cluster, skipping migration"),
        Err(error) => {
            return Err(Error::Generic {
                message: format!("Failed to check for MayastorPool CRD: {error:?}"),
            })
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{diskpool::crd::diskpools_name, ApiVersion, PrevApiVersion};
    use k8s_operators::diskpool::crd::DiskPool;
    use kube::{CustomResourceExt, Resource};

    #[test]
    fn test_parse_api_version() {
        assert_eq!(PrevApiVersion::V1Alpha1.to_string(), "v1alpha1");
        assert_eq!(
            "v1alpha1".parse::<ApiVersion>().unwrap(),
            ApiVersion::Deprecated(PrevApiVersion::V1Alpha1)
        );
        assert_eq!(
            ApiVersion::Deprecated(PrevApiVersion::V1Alpha1).to_string(),
            PrevApiVersion::V1Alpha1.to_string()
        );
        let latest_v = DiskPool::version(&());
        assert_eq!(latest_v.parse::<ApiVersion>(), Ok(ApiVersion::Latest));
        assert_eq!(ApiVersion::Latest.to_string(), DiskPool::version(&()));
        assert_eq!(ApiVersion::Latest.to_string(), latest_v);
    }

    #[test]
    fn test_api_version_order() {
        let mut versions = vec![
            ApiVersion::Latest,
            ApiVersion::Deprecated(PrevApiVersion::V1Alpha1),
            ApiVersion::Deprecated(PrevApiVersion::V1Beta2),
        ];
        versions.sort();
        assert_eq!(
            versions,
            vec![
                ApiVersion::Deprecated(PrevApiVersion::V1Alpha1),
                ApiVersion::Deprecated(PrevApiVersion::V1Beta2),
                ApiVersion::Latest,
            ]
        )
    }

    #[test]
    fn test_crd_name() {
        let crd = DiskPool::crd();
        let crd_name = crd.metadata.name.as_ref();
        assert_eq!(Some(&diskpools_name()), crd_name)
    }
}
