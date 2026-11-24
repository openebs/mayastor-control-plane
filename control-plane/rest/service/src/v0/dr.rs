use super::*;
use grpc::operations::dr::traits::DrOperations;
use stor_port::types::v0::transport::{
    CreateDrClusterPair, DestroyDrClusterPair, DrClusterPair, DrS3Credentials, DrS3Keys,
    DrS3Transport, DrTransport, GetDrClusterPair,
};

fn client() -> Result<impl DrOperations + 'static, ReplyError> {
    // Cloned rather than borrowed: the trait is implemented for the client, not a
    // reference to it.
    // The dr agent serves the pair, not core: the pair is its resource and it reads one
    // on every cycle, so an api in front of another agent's store would only add a hop.
    dr_pair_grpc().cloned()
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
            credentials: s3.credentials.map(credentials).transpose()?,
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
/// The credentials a put states.
///
/// An empty secret is what a get answers with when it was not asked to reveal one, so a
/// put carrying it is a pair read back and sent on unchanged. Storing it would replace a
/// working credential with nothing, which is worth refusing rather than doing quietly.
fn credentials(src: models::DrS3Credentials) -> Result<DrS3Credentials, ReplyError> {
    Ok(match src {
        models::DrS3Credentials::keys(keys) if keys.secret_key.is_empty() => {
            return Err(ReplyError::invalid_argument(
                ResourceKind::DrClusterPair,
                "transport.s3.credentials.keys.secretKey",
                "an empty secret key is what a get answers with when it was not asked to \
                 reveal one; state the real key, or name credentials instead"
                    .to_string(),
            ))
        }
        models::DrS3Credentials::keys(keys) => DrS3Credentials::Keys(DrS3Keys {
            access_key: keys.access_key,
            secret_key: keys.secret_key,
        }),
        models::DrS3Credentials::named(name) => DrS3Credentials::Named(name),
    })
}

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
            s3.credentials.map(|credentials| match credentials {
                // Empty, not absent: the field is required so that a put has to state
                // one, and a put carrying this back is refused.
                DrS3Credentials::Keys(keys) => {
                    models::DrS3Credentials::keys(models::DrS3Keys::new(
                        keys.access_key,
                        if show_secrets {
                            keys.secret_key
                        } else {
                            String::new()
                        },
                    ))
                }
                DrS3Credentials::Named(name) => models::DrS3Credentials::named(name),
            }),
        ))),
    )
}

#[async_trait::async_trait]
impl apis::actix_server::DisasterRecovery for RestApi {
    async fn del_dr_pair(Path(pair_id): Path<String>) -> Result<(), RestError<RestJsonError>> {
        client()?
            .destroy_pair(&DestroyDrClusterPair { id: pair_id.into() }, None)
            .await?;
        Ok(())
    }

    async fn get_dr_pair(
        Path(pair_id): Path<String>,
        Query(show_secrets): Query<Option<bool>>,
    ) -> Result<models::DrClusterPair, RestError<RestJsonError>> {
        let pair = client()?
            .get_pair(&GetDrClusterPair { id: pair_id.into() }, None)
            .await?;
        Ok(to_models(pair, show_secrets.unwrap_or_default()))
    }

    async fn get_dr_pairs(
        Query(show_secrets): Query<Option<bool>>,
    ) -> Result<Vec<models::DrClusterPair>, RestError<RestJsonError>> {
        let show_secrets = show_secrets.unwrap_or_default();
        let pairs = client()?.list_pairs(None).await?;
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
        let pair = client()?
            .create_pair(
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
