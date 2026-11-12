use crate::{
    operations::{Create, Delete, GetWithArgs, ListWithArgs, PluginResult},
    resources::{
        utils,
        utils::{optional_cell, CreateRow, GetHeaderRow},
        DrPairId, Error,
    },
    rest_wrapper::RestClient,
};
use async_trait::async_trait;
use openapi::apis::StatusCode;
use prettytable::Row;

/// Dr cluster pairs resource.
#[derive(clap::Args, Debug)]
pub struct DrPairs {}

/// Dr cluster pair resource.
#[derive(clap::Args, Debug)]
pub struct DrPair {}

/// Arguments used when getting all dr cluster pairs.
#[derive(Debug, Clone, clap::Args)]
pub struct GetDrPairsArgs {
    /// Show the store secrets rather than leaving them out.
    #[clap(long)]
    pub(crate) show_secrets: bool,
}

/// Arguments used when getting a dr cluster pair.
#[derive(Debug, Clone, clap::Args)]
pub struct GetDrPairArgs {
    /// Id of the dr cluster pair.
    pub(crate) pair_id: DrPairId,
    /// Show the store secret rather than leaving it out.
    #[clap(long)]
    pub(crate) show_secrets: bool,
}

/// Arguments used when deleting a dr cluster pair.
#[derive(Debug, Clone, clap::Args)]
pub struct DeleteDrPairArgs {
    /// Id of the dr cluster pair.
    pub(crate) pair_id: DrPairId,
}

/// How a relationship moves data between the two clusters.
///
/// Only `s3` is implemented. It is still named rather than assumed, so the options that
/// belong to a transport are scoped to it and a second one can be added without changing
/// what the existing flags mean.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum DrTransportKind {
    /// An s3-compatible object store both clusters can reach.
    S3,
}

/// Arguments used when creating a dr cluster pair.
#[derive(Debug, Clone, clap::Args)]
pub struct CreateDrPairArgs {
    /// Id of the dr cluster pair.
    pub(crate) pair_id: DrPairId,
    /// Name this cluster is known by within the relationship.
    pub(crate) cluster: String,
    /// Name of the peer cluster.
    #[clap(long)]
    pub(crate) peer: String,
    /// How the relationship moves data between the clusters.
    #[clap(long, value_enum, default_value_t = DrTransportKind::S3)]
    pub(crate) transport: DrTransportKind,
    /// Endpoint of the object store. Required by, and only used by, `--transport s3`.
    #[clap(long, required_if_eq("transport", "s3"))]
    pub(crate) s3_endpoint: Option<String>,
    /// Bucket within the store. Required by, and only used by, `--transport s3`.
    #[clap(long, required_if_eq("transport", "s3"))]
    pub(crate) s3_bucket: Option<String>,
    /// Key prefix within the bucket.
    #[clap(long, requires = "s3_endpoint")]
    pub(crate) s3_prefix: Option<String>,
    /// Region the bucket lives in.
    #[clap(long, requires = "s3_endpoint")]
    pub(crate) s3_region: Option<String>,
    /// Access key id for this store. Left out, the agent uses its own environment.
    #[clap(long, requires = "s3_secret_key")]
    pub(crate) s3_access_key: Option<String>,
    /// Secret access key for this store. Left out, the agent uses its own environment.
    #[clap(long, requires = "s3_access_key")]
    pub(crate) s3_secret_key: Option<String>,
}

// CreateRow being implemented for DrClusterPair creates the rows from the pairs
// returned from the REST call.
impl CreateRow for openapi::models::DrClusterPair {
    fn row(&self) -> Row {
        // S3 is the only transport, so the store columns are its endpoint, bucket and
        // prefix. A transport that is not an object store would want its own columns
        // rather than borrowing these.
        let Some(openapi::models::DrTransport::s3(s3)) = self.transport.as_ref() else {
            // Only s3 exists, so this is unreachable today; when it is not, the columns
            // for a transport that is not an object store belong to that transport.
            return row![
                self.id,
                self.cluster,
                self.cluster_uid,
                self.peer,
                "-",
                "-",
                "-"
            ];
        };
        row![
            self.id,
            self.cluster,
            self.cluster_uid,
            self.peer,
            s3.endpoint,
            s3.bucket,
            optional_cell(s3.prefix.as_ref()),
        ]
    }
}

impl GetHeaderRow for openapi::models::DrClusterPair {
    fn get_header_row(&self) -> Row {
        utils::DR_PAIR_HEADERS.clone()
    }
}

#[async_trait(?Send)]
impl ListWithArgs for DrPairs {
    type Args = GetDrPairsArgs;
    async fn list(args: &Self::Args, output: &utils::OutputFormat) -> PluginResult {
        // The secret is left out by the api unless asked for, so the flag is a request
        // rather than a rendering choice - nothing here could unmask what never arrived.
        match RestClient::client()
            .dr_pairs_api()
            .get_dr_pairs(Some(args.show_secrets))
            .await
        {
            Ok(pairs) => {
                utils::print_table(output, pairs.into_body());
                Ok(())
            }
            Err(source) => Err(Error::ListDrPairsError { source }),
        }
    }
}

#[async_trait(?Send)]
impl GetWithArgs for DrPair {
    type ID = DrPairId;
    type Args = GetDrPairArgs;
    async fn get(id: &Self::ID, args: &Self::Args, output: &utils::OutputFormat) -> PluginResult {
        match RestClient::client()
            .dr_pairs_api()
            .get_dr_pair(id, Some(args.show_secrets))
            .await
        {
            Ok(pair) => {
                utils::print_table(output, pair.into_body());
                Ok(())
            }
            Err(source) => Err(Error::GetDrPairError {
                id: id.to_owned(),
                source,
            }),
        }
    }
}

#[async_trait(?Send)]
impl Delete for DrPair {
    type ID = DrPairId;
    async fn del(
        id: &Self::ID,
        ignore_not_found: bool,
        _output: &utils::OutputFormat,
    ) -> PluginResult {
        match RestClient::client().dr_pairs_api().del_dr_pair(id).await {
            Ok(_) => Ok(()),
            Err(source) if ignore_not_found && source.status() == Some(StatusCode::NOT_FOUND) => {
                Ok(())
            }
            Err(source) => Err(Error::DelDrPairError {
                id: id.to_string(),
                source,
            }),
        }
    }
}

#[async_trait(?Send)]
impl Create for DrPair {
    type Args = CreateDrPairArgs;
    async fn create(args: &Self::Args, output: &utils::OutputFormat) -> PluginResult {
        // `required_if_eq` has already refused a bare `--transport s3`, so the options
        // are present by the time we are here; the match is what keeps that true as
        // transports are added, rather than an unwrap that silently stops being right.
        let transport = match args.transport {
            DrTransportKind::S3 => {
                openapi::models::DrTransport::s3(openapi::models::DrS3Transport::new_all(
                    args.s3_endpoint.clone().unwrap_or_default(),
                    args.s3_bucket.clone().unwrap_or_default(),
                    args.s3_prefix.clone(),
                    args.s3_region.clone(),
                    args.s3_access_key.clone(),
                    args.s3_secret_key.clone(),
                ))
            }
        };
        let body = openapi::models::CreateDrClusterPairBody::new_all(
            args.cluster.clone(),
            args.peer.clone(),
            Some(transport),
        );
        match RestClient::client()
            .dr_pairs_api()
            .put_dr_pair(&args.pair_id, body)
            .await
        {
            Ok(pair) => {
                utils::print_table(output, pair.into_body());
                Ok(())
            }
            Err(source) => Err(Error::CreateDrPairError {
                id: args.pair_id.to_string(),
                source,
            }),
        }
    }
}
