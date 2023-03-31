use super::crd::{DiskPool, DiskPoolSpec};
use crate::error::Error;
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::{api::PostParams, Api, Client, CustomResourceExt, ResourceExt};
use openapi::apis::StatusCode;
use tracing::info;

/// Get the DiskPool api.
pub(crate) fn api(client: &Client, namespace: &str) -> Api<DiskPool> {
    Api::namespaced(client.clone(), namespace)
}

/// Create a disk pool CR, with the given name and spec.
pub(crate) async fn create(
    client: &Client,
    namespace: &str,
    name: &str,
    spec: DiskPoolSpec,
) -> Result<(), Error> {
    let post_params = PostParams::default();
    let api = api(client, namespace);
    let new_disk_pool: DiskPool = DiskPool::new(name, spec);
    match api.create(&post_params, &new_disk_pool).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(e)) if e.code == StatusCode::CONFLICT => Ok(()),
        Err(error) => Err(error.into()),
    }
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
    let mut crd = DiskPool::crd();
    let crd_name = crd.metadata.name.as_ref().ok_or(Error::InvalidCRField {
        field: "diskpool.metadata.name".to_string(),
    })?;

    // Ensure the diskpool.
    let result = if let Ok(existing) = crd_api.get(crd_name).await {
        crd.metadata.resource_version = existing.resource_version();
        info!(
            "Replacing CRD: {}",
            serde_json::to_string_pretty(&crd).unwrap()
        );

        let pp = PostParams::default();
        crd_api.replace(crd_name, &pp, &crd).await
    } else {
        info!(
            "Creating CRD: {}",
            serde_json::to_string_pretty(&crd).unwrap()
        );

        let pp = PostParams::default();
        crd_api.create(&pp, &crd).await
    };
    let crd = result.map_err(|e| Error::Kube { source: e })?;
    Ok(crd)
}
