use super::{
    v1alpha1::DiskPool as AlphaDiskPool, v1beta1::DiskPool as Beta1DiskPool,
    v1beta2::DiskPool as Beta2DiskPool, v1beta3::DiskPool,
};
use crate::{
    diskpool::client::{discard_older_schema, dsp_api, list_existing_cr},
    error::Error,
    ApiVersion, PrevApiVersion,
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

/// In case of v1alpha1, v1beta1 and v1beta2 check, ensure that crd exist and then migrate to v1beta3.
pub(crate) async fn ensure_and_migrate_crd(
    k8s: Client,
    namespace: &str,
    api_version: ApiVersion,
) -> Result<(), Error> {
    match ensure_crd(&k8s, api_version).await {
        Ok(o) => {
            info!(crd = ?o.name_any(), "Updated DiskPool CRD");
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
        Err(error) => {
            error!(%error, "Failed to create DiskPool CRD");
            tokio::time::sleep(Duration::from_secs(1)).await;
            return Err(error);
        }
    }
    run_cr_migration(k8s.clone(), namespace).await?;
    Ok(())
}

/// Ensure the CRD is installed. This creates a chicken-and-egg problem. When the CRD is removed,
/// the operator will fail to list the CRD going into an error loop.
///
/// To prevent that, we will simply panic, and hope we can make progress after restart. Keep
/// running is not an option as the operator would be "running" and the only way to know something
/// is wrong would be to consult the logs.
pub(crate) async fn ensure_crd(
    k8s: &Client,
    api_version: ApiVersion,
) -> Result<CustomResourceDefinition, Error> {
    let dsp_name = super::diskpools_name();
    let latest_api_version = ApiVersion::Latest;
    let crd_api: Api<CustomResourceDefinition> = Api::all(k8s.clone());

    let api_version = match api_version {
        ApiVersion::Deprecated(api_version) => api_version,
        ApiVersion::Latest => {
            let manager = format!("merge_{api_version}_{latest_api_version}");
            let param = PatchParams::apply(&manager).force();
            let crd = crd_api
                .patch(&dsp_name, &param, &Patch::Apply(&DiskPool::crd()))
                .await?;
            return Ok(crd);
        }
    };
    let mut crd = match api_version {
        PrevApiVersion::V1Alpha1 => AlphaDiskPool::crd(),
        PrevApiVersion::V1Beta1 => Beta1DiskPool::crd(),
        PrevApiVersion::V1Beta2 => Beta2DiskPool::crd(),
    };

    let crd_name = crd.metadata.name.clone().ok_or(Error::InvalidCRField {
        field: "diskpool.metadata.name".to_string(),
    })?;
    crd.spec.versions[0].served = false;
    let new_crd = DiskPool::crd();
    let all_crds = vec![crd, new_crd];
    let new_crd = merge_crds(all_crds, &latest_api_version.to_string())
        .map_err(|source| Error::CrdMergeError { source })?;

    // If diskpool exist then replace it with new generated one.
    let result = match crd_api.get(&crd_name).await {
        Ok(_) => {
            info!(
                "Merging {api_version} DiskPool CRD with {latest_api_version}: {}",
                serde_json::to_string(&new_crd).unwrap_or_default()
            );
            let manager = format!("merge_{api_version}_{}", ApiVersion::Latest);
            let param = PatchParams::apply(&manager).force();
            crd_api
                .patch(&crd_name, &param, &Patch::Apply(&new_crd))
                .await
        }
        Err(err) => return Err(Error::Kube { source: err }),
    };

    let crd = result.map_err(|e| Error::Kube { source: e })?;
    Ok(crd)
}

/// Migrate existing deprecated CRs in cluster to the latest CR.
async fn run_cr_migration(k8s: Client, namespace: &str) -> Result<(), Error> {
    migrate_to_latest(k8s.clone(), namespace, PAGINATION_LIMIT).await?;
    _ = discard_older_schema(&k8s).await;
    Ok(())
}

/// Lists existing deprecated CRs in cluster and replaces them with the latest CR.
/// This ensures that there is no deprecated stored objects in cluster.
pub(crate) async fn migrate_to_latest(
    k8s: Client,
    ns: &str,
    pagination_limit: u32,
) -> Result<(), Error> {
    let pools = list_existing_cr(&k8s, ns, pagination_limit)
        .await
        .map_err(|_| Error::Generic {
            message: "Error in listing existing CR".to_string(),
        })?;
    for dsp in pools {
        replace_with_latest(&k8s, ns, dsp).await?;
    }
    Ok(())
}

/// Replaces a given disk pool CR with the latest schema CR.
pub(crate) async fn replace_with_latest(
    client: &Client,
    namespace: &str,
    dsp: DiskPool,
) -> Result<(), Error> {
    let post_params = PostParams::default();
    let api = dsp_api(client, namespace);
    let latest_api = ApiVersion::Latest;
    let name = dsp.name_any();

    let new_disk_pool: DiskPool = DiskPool {
        metadata: dsp.metadata,
        spec: dsp.spec,
        status: dsp.status,
    };

    info!(
        pool.cr_name = name,
        "Patching existing pool with {latest_api} schema"
    );
    match api.replace(&name, &post_params, &new_disk_pool).await {
        Ok(_) => Ok(()),
        Err(error) => {
            error!(
                ?error,
                pool.cr_name = name,
                "Failed to patch pool with {latest_api} schema"
            );
            Err(error.into())
        }
    }
}
