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

resource "kubernetes_namespace" "ns" {
  metadata {
    name = var.namespace
  }
}

resource "kubernetes_secret" "regcred" {
  metadata {
    name      = "regcred"
    namespace = var.namespace
  }
  data = {
    ".dockerconfigjson" = file("~/.docker/config.json")
  }

  type = "kubernetes.io/dockerconfigjson"
  depends_on = [
    kubernetes_namespace.ns,
  ]
}

module "rbac" {
  source              = "./mod/rbac"
  namespace           = var.namespace
  product_name_prefix = local.product_name_prefix
  create_sa_secret    = var.kube_create_sa_secret
}

module "jaegertracing" {
  depends_on = [kubernetes_namespace.ns, module.rbac]
  source     = "./mod/jaeger"
  count      = var.with_jaeger ? 1 : 0
  namespace  = var.namespace
}

/*
 * external services
 */

module "etcd" {
  source = "./mod/etcd"
  depends_on = [
    kubernetes_namespace.ns,
    kubernetes_secret.regcred
  ]
  image        = var.etcd_image
  control_node = var.control_node
  namespace    = var.namespace
  product_name = var.product_name
}

/*
 * control plane components
 */


module "csi-node" {
  source              = "./mod/csi/node"
  image               = format("%s%s", local.image_prefix, var.csi_node_image)
  tag                 = var.tag
  registry            = var.registry
  registrar_image     = var.csi_registar_image
  grace_period        = var.csi_node_grace_period
  rust_log            = var.control_rust_log
  io_queues           = var.io_engine_cpus
  credentials         = kubernetes_secret.regcred.metadata[0].name
  namespace           = var.namespace
  image_pull_policy   = var.image_pull_policy
  product_name_prefix = local.product_name_prefix

  depends_on = [
    module.rbac
  ]
}

module "csi-controller" {
  source = "./mod/csi/controller"
  depends_on = [
    module.agent-core,
    module.rest,
    module.io-engine
  ]
  image                 = format("%s%s", local.image_prefix, var.csi_controller_image)
  registry              = var.registry
  tag                   = var.tag
  control_node          = var.control_node
  csi_provisioner       = var.csi_provisioner
  csi_attacher_image    = var.csi_attacher_image
  jaeger_agent_argument = local.jaeger_agent_argument
  credentials           = kubernetes_secret.regcred.metadata[0].name
  rust_log              = var.control_rust_log
  namespace             = var.namespace
  image_pull_policy     = var.image_pull_policy
  product_name_prefix   = local.product_name_prefix
}

module "operator-diskpool" {
  source = "./mod/operator/diskpool"
  depends_on = [
    module.rbac,
    module.agent-core,
    module.rest,
    module.io-engine
  ]
  image                 = format("%s%s", local.image_prefix, var.operator_diskpool_image)
  registry              = var.registry
  tag                   = var.tag
  control_node          = var.control_node
  res_limits            = var.control_resource_limits
  res_requests          = var.control_resource_requests
  cache_period          = var.control_cache_period
  credentials           = kubernetes_secret.regcred.metadata[0].name
  jaeger_agent_argument = local.jaeger_agent_argument
  rust_log              = var.control_rust_log
  namespace             = var.namespace
  image_pull_policy     = var.image_pull_policy
  product_name_prefix   = local.product_name_prefix
}

module "rest" {
  source = "./mod/rest"
  depends_on = [
    kubernetes_secret.regcred,
    kubernetes_namespace.ns,
    module.agent-core,
  ]
  image                 = format("%s%s", local.image_prefix, var.rest_image)
  registry              = var.registry
  tag                   = var.tag
  control_node          = var.control_node
  credentials           = kubernetes_secret.regcred.metadata[0].name
  res_limits            = var.control_resource_limits
  res_requests          = var.control_resource_requests
  request_timeout       = var.control_request_timeout
  jaeger_agent_argument = local.jaeger_agent_argument
  rust_log              = var.control_rust_log
  namespace             = var.namespace
  image_pull_policy     = var.image_pull_policy
  product_name_prefix   = local.product_name_prefix
}

module "agent-core" {
  source = "./mod/agent/core"
  depends_on = [
    module.etcd,
    module.jaegertracing
  ]
  image                 = format("%s%s", local.image_prefix, var.agent_core_image)
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
  namespace             = var.namespace
  image_pull_policy     = var.image_pull_policy
  product_name_prefix   = local.product_name_prefix
}

module "sc" {
  source              = "./mod/sc"
  namespace           = var.namespace
  product_name_prefix = local.product_name_prefix
}

/*
 * dataplane
 */

module "io-engine" {
  source = "./mod/io-engine"
  depends_on = [
    module.etcd,
    module.agent-core
  ]
  hugepages         = var.io_engine_hugepages_2Mi
  cpus              = var.io_engine_cpus
  cpu_list          = var.io_engine_cpu_list
  memory            = var.io_engine_memory
  image             = format("%s%s", local.image_prefix, var.io_engine_image)
  registry          = var.registry
  tag               = var.tag
  rust_log          = var.io_engine_rust_log
  credentials       = kubernetes_secret.regcred.metadata[0].name
  namespace         = var.namespace
  image_pull_policy = var.image_pull_policy
  product_name      = var.product_name
}

locals {
  jaeger_agent_argument = var.with_jaeger ? [module.jaegertracing[0].jaeger_agent_argument] : []
  image_prefix          = var.image_prefix
  product_name_prefix   = var.product_name == "" ? "" : "${var.product_name}-"
}


resource "null_resource" "label_worker_nodes" {
  provisioner "local-exec" {
    command = "kubectl get node --selector='!node-role.kubernetes.io/master,!node-role.kubernetes.io/control-plane' --no-headers | awk '{print $1}' | xargs -I% kubectl label node % openebs.io/engine=${var.product_name} --overwrite"
  }
  triggers = {
    "before" = kubernetes_namespace.ns.id
  }
}