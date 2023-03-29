use crate::{error::Error, mayastorpool::crd::MayastorPool};
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::{
    api::{DeleteParams, ListParams, Patch, PatchParams},
    Api, Client, CustomResourceExt,
};
use openapi::apis::StatusCode;

/// Get the MayastorPool api.
pub(crate) fn api(client: &Client, namespace: &str) -> Api<MayastorPool> {
    Api::namespaced(client.clone(), namespace)
}

/// Fetch list of all mayastor pools.
pub(crate) async fn list(
    client: &Client,
    namespace: &str,
    pagination_limit: u32,
) -> Result<Vec<MayastorPool>, Error> {
    // Create the list params with pagination limit.
    let mut list_params = ListParams::default().limit(pagination_limit);
    let pools_api: Api<MayastorPool> = api(client, namespace);
    let mut pools: Vec<MayastorPool> = vec![];
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

/// Delete the mayastor pool CR forcefully by patching finalizer.
pub(crate) async fn delete(
    client: &Client,
    namespace: &str,
    msp: &mut MayastorPool,
) -> Result<(), Error> {
    // MayastorPool API.
    let pools_api: Api<MayastorPool> = api(client, namespace);

    // Create the api params with defaults.
    let patch_params = PatchParams::default();
    let delete_params = DeleteParams::default();

    // Patch all msps to remove the finalizers.
    msp.metadata.finalizers = Some(vec![]);
    let name = msp.metadata.name.as_ref().ok_or(Error::InvalidCRField {
        field: "diskpool.metadata.name".to_string(),
    })?;
    pools_api
        .patch(name, &patch_params, &Patch::Merge(&msp))
        .await?;

    // Delete the whole msp cr collection from the cluster.
    match pools_api.delete(name, &delete_params).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(e)) if e.code == StatusCode::NOT_FOUND => Ok(()),
        Err(error) => Err(error.into()),
    }
}

/// Check whether the MayastorPool diskpool exists or not.
pub(crate) async fn check_crd(k8s: &Client) -> Result<bool, Error> {
    let crd_api: Api<CustomResourceDefinition> = Api::all(k8s.clone());

    // Check if there is an existing dsp diskpool. If yes, Replace it with new generated one.
    // Create new if it doesnt exist.
    let crd = MayastorPool::crd();
    let crd_name = crd.metadata.name.as_ref().ok_or(Error::InvalidCRField {
        field: "diskpool.metadata.name".to_string(),
    })?;

    match crd_api.get(crd_name).await {
        Ok(_) => Ok(true),
        Err(kube::Error::Api(e)) if e.code == StatusCode::NOT_FOUND => Ok(false),
        Err(err) => Err(err.into()),
    }
}
