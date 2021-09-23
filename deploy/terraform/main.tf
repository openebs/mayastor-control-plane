/*
 * k8s specifics
 */

provider "kubernetes" {
  config_path = "~/.kube/config"
}

resource "kubernetes_namespace" "mayastor_ns" {
  metadata {
    name = "mayastor"
  }
}

resource "kubernetes_secret" "regcred" {
  metadata {
    name      = "regcred"
    namespace = "mayastor"
  }
  data = {
    ".dockerconfigjson" = "${file("~/.docker/config.json")}"
  }

  type = "kubernetes.io/dockerconfigjson"
  depends_on = [
    kubernetes_namespace.mayastor_ns,
  ]
}

module "rbac" {
  source = "./mod/rbac"
}

/*
 * external services
 */

module "nats" {
  source = "./mod/nats"
  depends_on = [
    kubernetes_namespace.mayastor_ns,
    kubernetes_secret.regcred
  ]
  nats_image   = var.nats_image
  control_node = var.control_node
}

module "etcd" {
  source = "./mod/etcd"
  depends_on = [
    kubernetes_namespace.mayastor_ns,
    kubernetes_secret.regcred
  ]
  image        = var.etcd_image
  control_node = var.control_node
}

/*
 * control plane components
 */


module "csi-agent" {
  source         = "./mod/csi-agent"
  image          = var.csi_agent_image
  tag            = var.tag
  registry       = var.registry
  registar_image = var.csi_registar_image
  grace_period   = var.csi_agent_grace_period
}

module "msp-operator" {
  source = "./mod/k8s-operator"
  depends_on = [
    module.rbac,
    module.core,
    module.rest,
    module.mayastor
  ]
  image        = var.msp_operator_image
  registry     = var.registry
  tag          = var.tag
  control_node = var.control_node
  res_limits   = var.control_resource_limits
  res_requests = var.control_resource_requests
  cache_period = var.control_cache_period
  credentials  = kubernetes_secret.regcred.metadata[0].name
}

module "rest" {
  source = "./mod/rest"
  depends_on = [
    kubernetes_secret.regcred,
    kubernetes_namespace.mayastor_ns,
    module.core
  ]
  image        = var.rest_image
  registry     = var.registry
  tag          = var.tag
  control_node = var.control_node
  credentials  = kubernetes_secret.regcred.metadata[0].name
  res_limits   = var.control_resource_limits
  res_requests = var.control_resource_requests
  request_timeout = var.control_request_timeout
}

module "core" {
  source = "./mod/core"
  depends_on = [
    module.nats,
    module.etcd
  ]
  image        = var.core_image
  registry     = var.registry
  tag          = var.tag
  control_node = var.control_node
  res_limits   = var.control_resource_limits
  res_requests = var.control_resource_requests
  request_timeout = var.control_request_timeout
  cache_period = var.control_cache_period
  credentials  = kubernetes_secret.regcred.metadata[0].name
}

module "sc" {
  source = "./mod/sc"
  depends_on = [
  ]
}

/*
 * dataplane
 */

module "mayastor" {
  source = "./mod/mayastor"
  depends_on = [
    module.nats,
    module.etcd
  ]
  hugepages = var.mayastor_hugepages_2Mi
  cpus      = var.mayastor_cpus
  cpu_list  = var.mayastor_cpu_list
  memory    = var.mayastor_memory
  image     = var.mayastor_image
  registry  = var.registry
  tag       = var.tag
}
