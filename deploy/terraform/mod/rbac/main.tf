resource "kubernetes_service_account" "mayastor" {
  metadata {
    name      = "mayastor-service-account"
    namespace = "mayastor"
  }
}

resource "kubernetes_cluster_role" "mayastor" {
  depends_on = [null_resource.cleanup_leftovers]

  metadata {
    name = "mayastor-cluster-role"
  }

  rule {
    verbs      = ["create", "list"]
    api_groups = ["apiextensions.k8s.io"]
    resources  = ["customresourcedefinitions"]
  }

  rule {
    verbs      = ["get", "list", "watch"]
    api_groups = ["storage.k8s.io"]
    resources  = ["csinodes"]
  }

  rule {
    verbs      = ["get", "list", "watch", "update", "patch", "replace"]
    api_groups = ["openebs.io"]
    resources  = ["mayastorpools"]
  }

  rule {
    verbs      = ["update", "patch"]
    api_groups = ["openebs.io"]
    resources  = ["mayastorpools/status"]
  }

  rule {
    verbs      = ["get", "list", "watch", "update", "create", "delete", "patch"]
    api_groups = [""]
    resources  = ["persistentvolumes"]
  }

  rule {
    verbs      = ["get", "list", "watch"]
    api_groups = [""]
    resources  = ["nodes"]
  }

  rule {
    verbs      = ["get", "list", "watch", "update"]
    api_groups = [""]
    resources  = ["persistentvolumeclaims"]
  }

  rule {
    verbs      = ["get", "list", "watch"]
    api_groups = ["storage.k8s.io"]
    resources  = ["storageclasses"]
  }

  rule {
    verbs      = ["list", "watch", "create", "update", "patch"]
    api_groups = [""]
    resources  = ["events"]
  }

  rule {
    verbs      = ["get", "list"]
    api_groups = ["snapshot.storage.k8s.io"]
    resources  = ["volumesnapshots"]
  }

  rule {
    verbs      = ["get", "list"]
    api_groups = ["snapshot.storage.k8s.io"]
    resources  = ["volumesnapshotcontents"]
  }

  rule {
    verbs      = ["get", "list", "watch"]
    api_groups = [""]
    resources  = ["nodes"]
  }

  rule {
    verbs      = ["get", ]
    api_groups = [""]
    resources  = ["namespaces"]
  }

  rule {
    verbs      = ["get", "list", "watch", "update", "patch"]
    api_groups = ["storage.k8s.io"]
    resources  = ["volumeattachments"]
  }

  rule {
    verbs      = ["patch"]
    api_groups = ["storage.k8s.io"]
    resources  = ["volumeattachments/status"]
  }
}

resource "kubernetes_cluster_role_binding" "mayastor" {
  depends_on = [null_resource.cleanup_leftovers]

  metadata {
    name = "mayastor-cluster-role-binding"
  }

  subject {
    kind      = "ServiceAccount"
    name      = "mayastor-service-account"
    namespace = "mayastor"
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = "mayastor-cluster-role"
  }
}

## When testing sometimes things get seriously broken and we can't easily use terraform destroy
## Most things are easy to delete by hand by removing the namespace, but these are not namespaced...
resource "null_resource" "cleanup_leftovers" {
  provisioner "local-exec" {
    command    = "kubectl delete clusterroles.rbac.authorization.k8s.io mayastor-cluster-role"
    on_failure = continue
  }
  provisioner "local-exec" {
    command    = "kubectl delete clusterrolebindings.rbac.authorization.k8s.io mayastor-cluster-role-binding"
    on_failure = continue
  }
  triggers = {
    "before" = null_resource.before.id
  }
}

resource "null_resource" "before" {
}