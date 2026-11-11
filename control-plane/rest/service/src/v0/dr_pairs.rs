use super::*;
use grpc::operations::dr_pair::traits::DrPairOperations;
use stor_port::types::v0::transport::{
    CreateDrClusterPair, DestroyDrClusterPair, DrClusterPair, DrS3Transport, DrTransport,
    GetDrClusterPair,
};

fn client() -> impl DrPairOperations {
    core_grpc().dr_pair()
}

/// The transport a put states, as the control plane holds it.
///
/// The schema makes "exactly one transport" a `oneOf`, so a body naming two is rejected
/// before it reaches here. It cannot also make the field required - the generator derives
/// `Default` on every model and a generated enum has none - so the absent case is refused
/// here instead: a relationship with no transport has nowhere to live.
fn transport(src: Option<models::DrTransport>) -> Result<DrTransport, ReplyError> {
    match src {
        Some(models::DrTransport::s3(s3)) => Ok(DrTransport::S3(DrS3Transport {
            endpoint: s3.endpoint,
            bucket: s3.bucket,
            prefix: s3.prefix,
            region: s3.region,
            access_key: s3.access_key,
            secret_key: s3.secret_key,
        })),
        None => Err(ReplyError::missing_argument(
            ResourceKind::DrClusterPair,
            "transport",
        )),
    }
}

/// The pair as the api reports it.
///
/// The secret is withheld unless the caller asked for it, so it is not handed to every
/// reader of a list by accident. Withheld rather than masked: a placeholder would be
/// indistinguishable from a real key to anything reading the response, and `PUT` replaces
/// rather than merges, so a round trip would store the placeholder.
fn to_models(pair: DrClusterPair, show_secrets: bool) -> models::DrClusterPair {
    let DrTransport::S3(s3) = pair.transport;
    models::DrClusterPair::new_all(
        pair.id.to_string(),
        pair.cluster.to_string(),
        pair.cluster_uid.to_string(),
        pair.peer.to_string(),
        Some(models::DrTransport::s3(models::DrS3Transport::new_all(
            s3.endpoint,
            s3.bucket,
            s3.prefix,
            s3.region,
            s3.access_key,
            show_secrets.then_some(s3.secret_key).flatten(),
        ))),
    )
}

#[async_trait::async_trait]
impl apis::actix_server::DrPairs for RestApi {
    async fn del_dr_pair(Path(pair_id): Path<String>) -> Result<(), RestError<RestJsonError>> {
        client()
            .destroy(&DestroyDrClusterPair { id: pair_id.into() }, None)
            .await?;
        Ok(())
    }

    async fn get_dr_pair(
        Path(pair_id): Path<String>,
        Query(show_secrets): Query<Option<bool>>,
    ) -> Result<models::DrClusterPair, RestError<RestJsonError>> {
        let pair = client()
            .get(&GetDrClusterPair { id: pair_id.into() }, None)
            .await?;
        Ok(to_models(pair, show_secrets.unwrap_or_default()))
    }

    async fn get_dr_pairs(
        Query(show_secrets): Query<Option<bool>>,
    ) -> Result<Vec<models::DrClusterPair>, RestError<RestJsonError>> {
        let show_secrets = show_secrets.unwrap_or_default();
        let pairs = client().list(None).await?;
        Ok(pairs
            .entries
            .into_iter()
            .map(|pair| to_models(pair, show_secrets))
            .collect())
    }

    async fn put_dr_pair(
        Path(pair_id): Path<String>,
        Body(body): Body<models::CreateDrClusterPairBody>,
    ) -> Result<models::DrClusterPair, RestError<RestJsonError>> {
        let pair = client()
            .create(
                &CreateDrClusterPair {
                    id: pair_id.into(),
                    cluster: body.cluster.into(),
                    peer: body.peer.into(),
                    transport: transport(body.transport)?,
                },
                None,
            )
            .await?;
        Ok(to_models(pair, false))
    }
}
