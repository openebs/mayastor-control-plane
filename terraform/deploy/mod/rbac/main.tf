variable "namespace" {}
variable "product_name_prefix" {}
variable "create_sa_secret" {}

resource "kubernetes_service_account" "sa" {
  metadata {
    name      = "${var.product_name_prefix}service-account"
    namespace = var.namespace
  }
}

resource "kubernetes_secret" "secret" {
  count = var.create_sa_secret ? 1 : 0
  metadata {
    name      = "${var.product_name_prefix}service-account-token"
    namespace = var.namespace
    annotations = {
      "kubernetes.io/service-account.name" = "${var.product_name_prefix}service-account"
    }
  }
  type = "kubernetes.io/service-account-token"
}

resource "null_resource" "link_sa_token" {
  count = var.create_sa_secret ? 1 : 0
  provisioner "local-exec" {
    command    = "kubectl -n ${var.namespace} patch serviceaccount ${var.product_name_prefix}service-account -p '{\"secrets\": [{\"name\": \"${var.product_name_prefix}service-account-token\"}]}'"
    on_failure = continue
  }
  triggers = {
    "before" = kubernetes_secret.secret[0].id
  }
}

resource "kubernetes_cluster_role" "cr" {
  depends_on = [null_resource.cleanup_leftovers]

  metadata {
    name = "${var.namespace}-cluster-role"
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
    resources  = ["diskpools"]
  }

  rule {
    verbs      = ["update", "patch"]
    api_groups = ["openebs.io"]
    resources  = ["diskpools/status"]
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
    verbs      = ["get"]
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

resource "kubernetes_cluster_role_binding" "crb" {
  depends_on = [null_resource.cleanup_leftovers]

  metadata {
    name = "${var.namespace}-cluster-role-binding"
  }

  subject {
    kind      = "ServiceAccount"
    name      = "${var.product_name_prefix}service-account"
    namespace = var.namespace
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = "${var.namespace}-cluster-role"
  }
}

## When testing sometimes things get seriously broken and we can't easily use terraform destroy
## Most things are easy to delete by hand by removing the namespace, but these are not namespaced...
resource "null_resource" "cleanup_leftovers" {
  provisioner "local-exec" {
    command    = "kubectl delete clusterroles.rbac.authorization.k8s.io ${var.namespace}-cluster-role"
    on_failure = continue
  }
  provisioner "local-exec" {
    command    = "kubectl delete clusterrolebindings.rbac.authorization.k8s.io ${var.namespace}-cluster-role-binding"
    on_failure = continue
  }
  triggers = {
    "before" = null_resource.before.id
  }
}

resource "null_resource" "before" {}
