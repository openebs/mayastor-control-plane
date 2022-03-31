/// Kubernetes hostname label key
pub(crate) const KUBERNETES_HOST_LABEL_KEY: &str = "kubernetes.io/hostname";

/// Field selector to choose only running pods in given namespace
pub(crate) const RUNNING_FIELD_SELECTOR: &str = "status.phase=Running";
