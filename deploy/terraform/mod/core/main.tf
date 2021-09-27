variable "image" {}
variable "registry" {}
variable "tag" {}
variable "control_node" {}
variable "res_limits" {}
variable "res_requests" {}
variable "request_timeout" {}
variable "cache_period" {}
variable "credentials" {}
variable "jaeger_agent_argument" {}

resource "kubernetes_stateful_set" "core_stateful_set" {
  metadata {
    labels = {
      app = "core"
    }
    name      = "core-agents"
    namespace = "mayastor"
  }
  spec {
    service_name = "core-agents"
    replicas     = 1
    selector {
      match_labels = {
        app = "core-agents"
      }
    }

    template {
      metadata {
        labels = {
          app = "core-agents"
        }
      }
      spec {
        service_account_name = "mayastor-service-account"
        container {
          args = concat([
            "-smayastor-etcd",
            "-nnats",
            "--request-timeout=${var.request_timeout}",
            "--cache-period=${var.cache_period}",
            ],
            var.jaeger_agent_argument
          )
          image             = format("%s/%s:%s", var.registry, var.image, var.tag)
          image_pull_policy = "Always"
          name              = "core-agent"
          resources {
            limits   = var.res_limits
            requests = var.res_requests
          }
        }

        image_pull_secrets {
          name = var.credentials
        }

        affinity {
          node_affinity {
            preferred_during_scheduling_ignored_during_execution {
              weight = 1

              preference {
                match_expressions {
                  key      = "kubernetes.io/hostname"
                  operator = "In"
                  values   = [var.control_node]
                }
              }
            }
          }
        }
      }
    }
  }
}