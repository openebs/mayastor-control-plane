use super::{
    v1alpha1::DiskPool as AlphaDiskPool,
    v1beta1::{DiskPool, DiskPoolSpec},
};
use crate::error::Error;
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::{
    api::{ListParams, Patch, PatchParams, PostParams},
    core::crd::merge_crds,
    Api, Client, CustomResourceExt, ResourceExt,
};
use openapi::{apis::StatusCode, clients};
use tracing::{error, info, warn};

/// Get the DiskPool v1beta1 api.
pub(crate) fn v1beta1_api(client: &Client, namespace: &str) -> Api<DiskPool> {
    Api::namespaced(client.clone(), namespace)
}

/// Create a v1beta1 disk pool CR, with the given name and spec.
pub(crate) async fn create_v1beta1_cr(
    client: &Client,
    namespace: &str,
    name: &str,
    spec: DiskPoolSpec,
) -> Result<(), Error> {
    let post_params = PostParams::default();
    let api = v1beta1_api(client, namespace);
    let new_disk_pool: DiskPool = DiskPool::new(name, spec);
    match api.create(&post_params, &new_disk_pool).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(e)) if e.code == StatusCode::CONFLICT => Ok(()),
        Err(error) => Err(error.into()),
    }
}

/// Replaces a given disk pool CR with v1beta1 schema CR.
pub(crate) async fn replace_with_v1beta1(
    client: &Client,
    cr_name: &str,
    namespace: &str,
    res_ver: Option<String>,
    spec: DiskPoolSpec,
) -> Result<(), Error> {
    let post_params = PostParams::default();
    let api = v1beta1_api(client, namespace);
    let mut new_disk_pool: DiskPool = DiskPool::new(cr_name, spec);
    new_disk_pool.metadata.resource_version = res_ver;
    info!(
        pool.cr_name = cr_name,
        "Patching existing pool with v1beta1 schema"
    );
    match api.replace(cr_name, &post_params, &new_disk_pool).await {
        Ok(_) => Ok(()),
        Err(error) => {
            error!(
                ?error,
                pool.cr_name = cr_name,
                "Failed to patch pool with v1beta 1schema"
            );
            Err(error.into())
        }
    }
}

pub(crate) async fn update_crd_with_topology(k8s: &Client) -> Result<(), Error> {
    let mut crd_with_topology = DiskPool::crd();
    let crd_api: Api<CustomResourceDefinition> = Api::all(k8s.clone());
    if let Ok(existing) = crd_api.get_status("diskpools.openebs.io").await {
        crd_with_topology.metadata.resource_version = existing.resource_version();
        info!(
            "Replacing CRD: {}",
            serde_json::to_string_pretty(&crd_with_topology).unwrap()
        );
        let pp = PostParams::default();
        _ = crd_api
            .replace("diskpools.openebs.io", &pp, &crd_with_topology)
            .await
    }
    Ok(())
}

/// Ensure the CRD is installed. This creates a chicken and egg problem. When the CRD is removed,
/// the operator will fail to list the CRD going into a error loop.
///
/// To prevent that, we will simply panic, and hope we can make progress after restart. Keep
/// running is not an option as the operator would be "running" and the only way to know something
/// is wrong would be to consult the logs.
pub(crate) async fn ensure_crd(k8s: &Client) -> Result<CustomResourceDefinition, Error> {
    let crd_api: Api<CustomResourceDefinition> = Api::all(k8s.clone());
    // Check if there is an existing dsp diskpool. If yes, Replace it with new generated one.
    // Create new if it doesnt exist.
    let mut crd = AlphaDiskPool::crd();
    let crd_name = crd.metadata.name.as_ref().ok_or(Error::InvalidCRField {
        field: "diskpool.metadata.name".to_string(),
    })?;
    crd.spec.versions[0].served = false;
    let new_crd = DiskPool::crd();
    let all_crds = vec![crd.clone(), new_crd.clone()];
    let new_crd =
        merge_crds(all_crds, "v1beta1").map_err(|source| Error::CrdMergeError { source })?;

    // Ensure if diskpool crd exist to determine api verb.
    let result = if crd_api.get(crd_name).await.is_ok() {
        info!(
            "Replacing CRD: {}",
            serde_json::to_string_pretty(&new_crd).unwrap_or_default()
        );
        let param = PatchParams::apply("merge_v1alpha1_v1beta1").force();
        crd_api
            .patch("diskpools.openebs.io", &param, &Patch::Apply(&new_crd))
            .await
    } else {
        info!(
            "Creating CRD: {}",
            serde_json::to_string_pretty(&new_crd).unwrap_or_default()
        );

        let pp = PostParams::default();
        crd_api.create(&pp, &new_crd).await
    };
    let crd = result.map_err(|e| Error::Kube { source: e })?;
    Ok(crd)
}

/// This discards older unserved schema from the crd.
pub(crate) async fn discard_older_schema(k8s: &Client, new_version: &str) -> Result<(), Error> {
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
        update_stored_version(k8s, crd_name, new_version).await?;
        if let Ok(modified_crd) = crd_api.get(crd_name).await {
            new_crd.metadata.resource_version = modified_crd.resource_version();
            let pp = PostParams::default();
            crd_api.replace(crd_name, &pp, &new_crd).await?;
        }
    } else {
        info!(
            "Creating CRD: {}",
            serde_json::to_string_pretty(&new_crd).unwrap()
        );
        let pp = PostParams::default();
        if let Err(e) = crd_api.create(&pp, &new_crd).await {
            error!("Failed to create v1beta1 CRD: {:?}", e);
        }
    }
    Ok(())
}

/// Lists existing v1alpha1 CR in cluster and replaces them with v1beta1 CR.
/// This ensures that there is no v1alpha1 stored objects in cluster.
pub(crate) async fn migrate_to_v1beta1(
    k8s: Client,
    ns: &str,
    pagination_limit: u32,
) -> Result<(), Error> {
    if let Ok(mut v1_alpha1_pools) = list_existing_cr(&k8s, ns, pagination_limit).await {
        for dsp in v1_alpha1_pools.iter_mut() {
            if let Some(res_ver) = dsp.resource_version() {
                let name = dsp.name_any();
                let node = dsp.spec.node();
                let disk = dsp.spec.disks();
                replace_with_v1beta1(
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
            message: "Error in listing v1alpha1 CR".to_string(),
        });
    };
    Ok(())
}

/// Reconciles control-plane pools into CRs by listing pools from the control plane
/// and creating equivalent CRs if respective CR is not present.
pub(crate) async fn create_missing_cr(
    k8s: &Client,
    control_client: clients::tower::ApiClient,
    namespace: &str,
) -> Result<(), Error> {
    if let Ok(pools) = control_client.pools_api().get_pools().await {
        let pools_api: Api<DiskPool> = v1beta1_api(k8s, namespace);
        let param = PostParams::default();
        for pool in pools.into_body().iter_mut() {
            match pools_api.get(&pool.id).await {
                Err(kube::Error::Api(e)) if e.code == StatusCode::NOT_FOUND => {
                    if let Some(spec) = &pool.spec {
                        warn!(pool.id, spec.node, "DiskPool CR is missing");
                        let cr_spec: DiskPoolSpec =
                            DiskPoolSpec::new(spec.node.clone(), spec.disks.clone(), None);
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

/// Updates stored version in CRD status to the new version passed as arg,
/// Please ensure that older versions are served:false, stored:false before calling this method,
/// This would allow us to remove older schema from CRD versions.
async fn update_stored_version(
    k8s: &Client,
    crd_name: &str,
    new_version: &str,
) -> Result<(), Error> {
    let crd_api: Api<CustomResourceDefinition> = Api::all(k8s.clone());
    if let Ok(mut crd) = crd_api.get_status(crd_name).await {
        let param = PatchParams::apply("status_patch").force();
        if let Some(status) = crd.status.as_mut() {
            status.stored_versions = Some(vec![new_version.to_string()]);
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

/// Fetch list of all mayastor pools.
pub(crate) async fn list_existing_cr(
    client: &Client,
    namespace: &str,
    pagination_limit: u32,
) -> Result<Vec<DiskPool>, Error> {
    // Create the list params with pagination limit.
    let mut list_params = ListParams::default().limit(pagination_limit);
    // Since v1alpha1 is not served at this stage we cannot use v1alpha1 api client
    // to list existing CRs. Existing CRs which were created and stored as v1alpha1 can
    // be retrieved using v1beta1 client. Kube api server performs the required conversions and
    // returns us the resources.
    let pools_api: Api<DiskPool> = v1beta1_api(client, namespace);
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
