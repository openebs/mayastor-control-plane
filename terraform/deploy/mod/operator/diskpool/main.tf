variable "image" {}
variable "registry" {}
variable "tag" {}
variable "control_node" {}
variable "credentials" {}
variable "res_limits" {}
variable "res_requests" {}
variable "cache_period" {}
variable "jaeger_agent_argument" {}
variable "rust_log" {}
variable "namespace" {}
variable "product_name" {}
variable "image_pull_policy" {}

resource "kubernetes_deployment" "diskpool_deployment" {
  metadata {
    labels = {
      app = "operator-diskpool"
    }
    name      = "operator-diskpool"
    namespace = var.namespace
  }
  spec {
    replicas = 1
    selector {
      match_labels = {
        app = "operator-diskpool"
      }
    }
    template {
      metadata {
        labels = {
          app = "operator-diskpool"
        }
      }
      spec {
        service_account_name = "${var.product_name}-service-account"
        container {
          args = concat([
            "-e http://api-rest:8081",
            "--interval=${var.cache_period}",
            "-n=${var.namespace}"
            ],
            var.jaeger_agent_argument
          )
          env {
            name  = "RUST_LOG"
            value = var.rust_log
          }
          env {
            name = "MY_POD_NAME"
            value_from {
              field_ref {
                field_path = "metadata.name"
              }
            }
          }
          name = "operator-diskpool"

          image = format("%s/%s:%s", var.registry, var.image, var.tag)

          image_pull_policy = var.image_pull_policy
          resources {
            limits   = var.res_limits
            requests = var.res_requests
          }
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
        image_pull_secrets {
          name = var.credentials
        }
      }
    }
  }
}
