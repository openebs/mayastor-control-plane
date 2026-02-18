use super::crd::v1beta3::{DiskPool, DiskPoolSpec, EncryptionSource};
use crate::{diskpool::crd::diskpools_name, error::Error, ApiVersion};
use openapi::{apis::StatusCode, clients, models::PoolSpecEncryption};

use crate::diskpool::crd::v1beta3;
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::{
    api::{ListParams, Patch, PatchParams, PostParams},
    Api, Client, CustomResourceExt, ResourceExt,
};
use tracing::{info, warn};

/// Get the DiskPool v1beta3 api.
pub(crate) fn v1beta3_api(client: &Client, namespace: &str) -> Api<DiskPool> {
    Api::namespaced(client.clone(), namespace)
}

/// Create a v1beta3 disk pool CR, with the given name and spec.
pub(crate) async fn create_v1beta3_cr(
    client: &Client,
    namespace: &str,
    name: &str,
    spec: DiskPoolSpec,
) -> Result<(), Error> {
    let post_params = PostParams::default();
    let api = v1beta3_api(client, namespace);
    let new_disk_pool: DiskPool = DiskPool::new(name, spec);
    match api.create(&post_params, &new_disk_pool).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(e)) if e.code == StatusCode::CONFLICT => Ok(()),
        Err(error) => Err(error.into()),
    }
}

/// Create the crd.
pub(crate) async fn create_crd(k8s: Client) -> Result<CustomResourceDefinition, Error> {
    let crd_api: Api<CustomResourceDefinition> = Api::all(k8s.clone());
    let new_crd = DiskPool::crd();
    info!(
        "Creating DiskPool CRD: {}",
        serde_json::to_string(&new_crd).unwrap_or_default()
    );
    let pp = PostParams::default();
    let result = crd_api.create(&pp, &new_crd).await;
    let crd = result.map_err(|e| Error::Kube { source: e })?;
    info!("Created DiskPool CRD");
    Ok(crd)
}

/// This discards older unserved schema from the crd.
pub(crate) async fn discard_older_schema(k8s: &Client) -> Result<(), Error> {
    let crd_api: Api<CustomResourceDefinition> = Api::all(k8s.clone());
    let mut new_crd = DiskPool::crd();
    let crd_name = new_crd
        .metadata
        .name
        .as_ref()
        .ok_or(Error::InvalidCRField {
            field: "diskpool.metadata.name".to_string(),
        })?;
    if crd_api.get(crd_name).await.is_ok() {
        info!(
            "Replacing CRD: {}",
            serde_json::to_string_pretty(&new_crd).unwrap()
        );
        update_stored_version(k8s, crd_name).await?;
        if let Ok(modified_crd) = crd_api.get(crd_name).await {
            new_crd.metadata.resource_version = modified_crd.resource_version();
            let pp = PostParams::default();
            crd_api.replace(crd_name, &pp, &new_crd).await?;
        }
    }
    Ok(())
}

/// Reconciles control-plane pools into CRs by listing pools from the control plane
/// and creating equivalent CRs if respective CR is not present.
pub(crate) async fn create_missing_cr(
    k8s: &Client,
    control_client: clients::tower::ApiClient,
    namespace: &str,
) -> Result<(), Error> {
    if let Ok(pools) = control_client.pools_api().get_pools(None).await {
        let pools_api: Api<DiskPool> = v1beta3_api(k8s, namespace);
        let param = PostParams::default();
        for pool in pools.into_body().iter_mut() {
            match pools_api.get(&pool.id).await {
                Err(kube::Error::Api(e)) if e.code == StatusCode::NOT_FOUND => {
                    if let Some(spec) = &pool.spec {
                        warn!(pool.id, spec.node, "DiskPool CR is missing");
                        let encryption = match spec.encryption.clone() {
                            None => None,
                            Some(config) => match config {
                                PoolSpecEncryption::secret(details) => {
                                    Some(v1beta3::EncryptionConfig {
                                        source: EncryptionSource::Secret(
                                            v1beta3::EncryptionSecretConfig { name: details.name },
                                        ),
                                    })
                                }
                            },
                        };
                        let cr_spec: DiskPoolSpec = DiskPoolSpec::new(
                            spec.node.clone(),
                            spec.disks.clone(),
                            None,
                            encryption,
                            spec.cluster_size
                                .map(|c| utils::bytes::into_human(c as u64)),
                            spec.max_expansion.clone(),
                        );
                        let new_disk_pool: DiskPool = DiskPool::new(&pool.id, cr_spec);
                        if let Err(error) = pools_api.create(&param, &new_disk_pool).await {
                            info!(pool.id, spec.node, %error, "Failed to create CR for missing DiskPool");
                        }
                    }
                }
                _ => continue,
            };
        }
    }
    Ok(())
}

/// Updates stored version in CRD status to the latest version.
/// Please ensure that older versions are served:false, stored:false before calling this method,
/// This would allow us to remove older schema from CRD versions.
/// todo: Shouldn't we just ensure that here rather leave it for chance??
async fn update_stored_version(k8s: &Client, crd_name: &str) -> Result<(), Error> {
    let crd_api: Api<CustomResourceDefinition> = Api::all(k8s.clone());
    if let Ok(mut crd) = crd_api.get_status(crd_name).await {
        let param = PatchParams::apply("status_patch").force();
        if let Some(status) = crd.status.as_mut() {
            status.stored_versions = Some(vec![ApiVersion::Latest.to_string()]);
        } else {
            return Err(Error::CrdFieldMissing {
                name: crd_name.to_string(),
                field: "status".to_string(),
            });
        }
        crd.metadata.managed_fields = None;
        let _ = crd_api
            .patch_status(crd_name, &param, &Patch::Apply(&crd))
            .await?;
    }
    Ok(())
}

/// List of all pools.
pub(crate) async fn list_existing_cr(
    client: &Client,
    namespace: &str,
    pagination_limit: u32,
) -> Result<Vec<DiskPool>, Error> {
    // Create the list params with pagination limit.
    let mut list_params = ListParams::default().limit(pagination_limit);
    // Since v1alpha1/v1beta1/v1beta2 is not served at this stage we cannot use v1alpha1/v1beta1/v1beta2 api client
    // to list existing CRs. Existing CRs which were created and stored as v1alpha1/v1beta1/v1beta2 can
    // be retrieved using v1beta3 client. Kube api server performs the required conversions and
    // returns us the resources.
    let pools_api: Api<DiskPool> = v1beta3_api(client, namespace);
    let mut pools: Vec<DiskPool> = vec![];
    loop {
        let mut result = pools_api.list(&list_params).await?;
        pools.append(&mut result.items);
        // Check for the token, if valid then continue.
        match result.metadata.continue_ {
            Some(token) if !token.is_empty() => {
                list_params = list_params.continue_token(token.as_str())
            }
            _ => break,
        }
    }
    Ok(pools)
}

/// Returns the [`ApiVersion`] of the present crd, if any.
pub(crate) async fn runtime_api_version(k8s: Client) -> Result<Option<ApiVersion>, kube::Error> {
    let crd_api: Api<CustomResourceDefinition> = Api::all(k8s);

    let crd = match crd_api.get_status(&diskpools_name()).await {
        // CRD not installed yet, this is fine.
        Err(kube::Error::Api(r)) if r.code == 404 => return Ok(None),
        _else => _else,
    }?;

    let crd_version = |crd: CustomResourceDefinition| -> Option<ApiVersion> {
        // todo: shouldn't this be an error?
        let status = crd.status?;

        // todo: is this right? Can't we have multiple stored versions during upgrade?
        let stored_version = status.stored_versions.as_ref()?.first()?;
        stored_version.parse().ok()
    };

    Ok(crd_version(crd))
}
