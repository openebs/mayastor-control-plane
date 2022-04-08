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

resource "kubernetes_deployment" "deployment_diskpool_operator" {
  metadata {
    labels = {
      app = "diskpool-operator"
    }
    name      = "diskpool-operator"
    namespace = "mayastor"
  }
  spec {
    replicas = 1
    selector {
      match_labels = {
        app = "diskpool-operator"
      }
    }
    template {
      metadata {
        labels = {
          app = "diskpool-operator"
        }
      }
      spec {
        service_account_name = "mayastor-service-account"
        container {
          args = concat([
            "-e http://rest:8081",
            "--interval=${var.cache_period}"
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
          name = "diskpool-operator"

          image = format("%s/%s:%s", var.registry, var.image, var.tag)

          image_pull_policy = "Always"
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