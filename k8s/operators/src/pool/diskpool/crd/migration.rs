use super::{
    v1alpha1::DiskPool as AlphaDiskPool,
    v1beta1::DiskPool as Beta1DiskPool,
    v1beta2::{DiskPool, DiskPoolSpec},
};
use crate::{
    diskpool::client::{discard_older_schema, list_existing_cr, v1beta2_api},
    error::Error,
    ApiVersion,
};
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::{
    api::{Patch, PatchParams, PostParams},
    core::crd::merge_crds,
    Api, Client, CustomResourceExt, ResourceExt,
};
use std::time::Duration;
use tracing::{error, info};

const PAGINATION_LIMIT: u32 = 100;

/// In case of v1alpha1 and v1beta1 check, ensure that crd exist and then migrate to v1beta2.
pub(crate) async fn ensure_and_migrate_crd(
    k8s: Client,
    namespace: &str,
    api_version: &ApiVersion,
    target_schema: &str,
) -> Result<(), Error> {
    match ensure_crd(&k8s, api_version).await {
        Ok(o) => {
            info!(crd = ?o.name_any(), "Created");
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
        Err(error) => {
            error!(%error, "Failed to create CRD");
            tokio::time::sleep(Duration::from_secs(1)).await;
            return Err(error);
        }
    }
    run_crd_migration(k8s.clone(), namespace, api_version, target_schema).await?;
    Ok(())
}

/// Ensure the CRD is installed. This creates a chicken and egg problem. When the CRD is removed,
/// the operator will fail to list the CRD going into a error loop.
///
/// To prevent that, we will simply panic, and hope we can make progress after restart. Keep
/// running is not an option as the operator would be "running" and the only way to know something
/// is wrong would be to consult the logs.
pub(crate) async fn ensure_crd(
    k8s: &Client,
    api_version: &ApiVersion,
) -> Result<CustomResourceDefinition, Error> {
    let crd_api: Api<CustomResourceDefinition> = Api::all(k8s.clone());
    let mut crd = if api_version == &ApiVersion::V1Alpha1 {
        AlphaDiskPool::crd()
    } else {
        Beta1DiskPool::crd()
    };

    let crd_name = crd.metadata.name.as_ref().ok_or(Error::InvalidCRField {
        field: "diskpool.metadata.name".to_string(),
    })?;
    crd.spec.versions[0].served = false;
    let new_crd = DiskPool::crd();
    let all_crds = vec![crd.clone(), new_crd.clone()];
    let new_crd =
        merge_crds(all_crds, "v1beta2").map_err(|source| Error::CrdMergeError { source })?;

    // If diskpool exist then replace it with new generated one.
    let result = match crd_api.get(crd_name).await {
        Ok(_) => {
            info!(
                "Replacing CRD: {}",
                serde_json::to_string_pretty(&new_crd).unwrap_or_default()
            );
            let param = if api_version == &ApiVersion::V1Alpha1 {
                PatchParams::apply("merge_v1alpha1_v1beta2").force()
            } else {
                PatchParams::apply("merge_v1beta1_v1beta2").force()
            };
            crd_api
                .patch(&super::diskpools_name(), &param, &Patch::Apply(&new_crd))
                .await
        }
        Err(err) => return Err(Error::Kube { source: err }),
    };

    let crd = result.map_err(|e| Error::Kube { source: e })?;
    Ok(crd)
}

/// Migrate existing v1alpha1/v1beta1 CR in cluster to v1beta2 CR.
async fn run_crd_migration(
    k8s: Client,
    namespace: &str,
    api_version: &ApiVersion,
    target_schema: &str,
) -> Result<(), Error> {
    match api_version {
        ApiVersion::V1Alpha1 | ApiVersion::V1Beta1 => {
            migrate_to_v1beta2(k8s.clone(), namespace, PAGINATION_LIMIT).await?;
            _ = discard_older_schema(&k8s, target_schema).await;
        }
        ApiVersion::V1Beta2 => {
            info!("CRD has the latest schema. Skipping CRD Operations");
        }
    }
    Ok(())
}

/// Lists existing v1alpha1/v1beta1 CR in cluster and replaces them with v1beta2 CR.
/// This ensures that there is no v1alpha1/v1beta1 stored objects in cluster.
pub(crate) async fn migrate_to_v1beta2(
    k8s: Client,
    ns: &str,
    pagination_limit: u32,
) -> Result<(), Error> {
    if let Ok(mut existing_pools) = list_existing_cr(&k8s, ns, pagination_limit).await {
        for dsp in existing_pools.iter_mut() {
            if let Some(res_ver) = dsp.resource_version() {
                let name = dsp.name_any();
                let node = dsp.spec.node();
                let disk = dsp.spec.disks();
                replace_with_v1beta2(
                    &k8s,
                    &name,
                    ns,
                    Some(res_ver.clone()),
                    DiskPoolSpec::new(node, disk, None),
                )
                .await?;
                info!(crd = ?dsp.name_any(), "CR creation successful");
            } else {
                return Err(Error::CrdFieldMissing {
                    name: dsp.name_any(),
                    field: "resource_version".to_string(),
                });
            }
        }
    } else {
        return Err(Error::Generic {
            message: "Error in listing existing CR".to_string(),
        });
    };
    Ok(())
}

/// Replaces a given disk pool CR with v1beta2 schema CR.
pub(crate) async fn replace_with_v1beta2(
    client: &Client,
    cr_name: &str,
    namespace: &str,
    res_ver: Option<String>,
    spec: DiskPoolSpec,
) -> Result<(), Error> {
    let post_params = PostParams::default();
    let api = v1beta2_api(client, namespace);
    let mut new_disk_pool: DiskPool = DiskPool::new(cr_name, spec);
    new_disk_pool.metadata.resource_version = res_ver;
    info!(
        pool.cr_name = cr_name,
        "Patching existing pool with v1beta2 schema"
    );
    match api.replace(cr_name, &post_params, &new_disk_pool).await {
        Ok(_) => Ok(()),
        Err(error) => {
            error!(
                ?error,
                pool.cr_name = cr_name,
                "Failed to patch pool with v1beta2 schema"
            );
            Err(error.into())
        }
    }
}
