pub(crate) mod migration;
/// The DiskPool custom resource definition.
pub(crate) mod v1alpha1;
pub(crate) mod v1beta1;
pub(crate) mod v1beta2;

pub(crate) fn diskpools_name() -> String {
    use utils::constants::DSP_API_NAME;
    format!("diskpools.{DSP_API_NAME}")
}
