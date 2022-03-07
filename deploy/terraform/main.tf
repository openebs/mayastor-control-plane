/*
 * k8s specifics
 */

provider "kubernetes" {
  experiments {
    manifest_resource = true
  }
  config_path = "~/.kube/config"
}
provider "helm" {
  kubernetes {
    config_path = "~/.kube/config"
  }
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

module "jaegertracing" {
  depends_on = [kubernetes_namespace.mayastor_ns, module.rbac]
  source     = "./mod/jaeger"
  count      = var.with_jaeger ? 1 : 0
}

/*
 * external services
 */

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


module "csi-node" {
  source         = "./mod/csi-node"
  image          = var.csi_node_image
  tag            = var.tag
  registry       = var.registry
  registrar_image = var.csi_registar_image
  grace_period   = var.csi_node_grace_period
  rust_log       = var.control_rust_log
  io_queues      = var.mayastor_cpus
}

module "csi-controller" {
  source = "./mod/csi-controller"
  depends_on = [
    module.core,
    module.rest,
    module.mayastor
  ]
  image                 = var.csi_controller_image
  registry              = var.registry
  tag                   = var.tag
  control_node          = var.control_node
  csi_provisioner       = var.csi_provisioner
  csi_attacher_image    = var.csi_attacher_image
  jaeger_agent_argument = local.jaeger_agent_argument
  credentials           = kubernetes_secret.regcred.metadata[0].name
  rust_log              = var.control_rust_log
}

module "msp-operator" {
  source = "./mod/msp-operator"
  depends_on = [
    module.rbac,
    module.core,
    module.rest,
    module.mayastor
  ]
  image                 = var.msp_operator_image
  registry              = var.registry
  tag                   = var.tag
  control_node          = var.control_node
  res_limits            = var.control_resource_limits
  res_requests          = var.control_resource_requests
  cache_period          = var.control_cache_period
  credentials           = kubernetes_secret.regcred.metadata[0].name
  jaeger_agent_argument = local.jaeger_agent_argument
  rust_log              = var.control_rust_log
}

module "rest" {
  source = "./mod/rest"
  depends_on = [
    kubernetes_secret.regcred,
    kubernetes_namespace.mayastor_ns,
    module.core,
  ]
  image                 = var.rest_image
  registry              = var.registry
  tag                   = var.tag
  control_node          = var.control_node
  credentials           = kubernetes_secret.regcred.metadata[0].name
  res_limits            = var.control_resource_limits
  res_requests          = var.control_resource_requests
  request_timeout       = var.control_request_timeout
  jaeger_agent_argument = local.jaeger_agent_argument
  rust_log              = var.control_rust_log
}

module "core" {
  source = "./mod/core"
  depends_on = [
    module.etcd,
    module.jaegertracing
  ]
  image                 = var.core_image
  registry              = var.registry
  tag                   = var.tag
  control_node          = var.control_node
  res_limits            = var.control_resource_limits
  res_requests          = var.control_resource_requests
  request_timeout       = var.control_request_timeout
  cache_period          = var.control_cache_period
  jaeger_agent_argument = local.jaeger_agent_argument
  credentials           = kubernetes_secret.regcred.metadata[0].name
  rust_log              = var.control_rust_log
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
    module.etcd,
    module.core
  ]
  hugepages = var.mayastor_hugepages_2Mi
  cpus      = var.mayastor_cpus
  cpu_list  = var.mayastor_cpu_list
  memory    = var.mayastor_memory
  image     = var.mayastor_image
  registry  = var.registry
  tag       = var.tag
  rust_log  = var.mayastor_rust_log
}

locals {
  jaeger_agent_argument = var.with_jaeger ? [module.jaegertracing[0].jaeger_agent_argument] : []
}