variable "image" {}
variable "registry" {}
variable "tag" {}
variable "control_node" {}
variable "credentials" {}
variable "res_limits" {}
variable "res_requests" {}
variable "cache_period" {}

resource "kubernetes_deployment" "deployment_msp_operator" {
  metadata {
    labels = {
      app = "msp-operator"
    }
    name      = "msp-operator"
    namespace = "mayastor"
  }
  spec {
    replicas = 1
    selector {
      match_labels = {
        app = "msp-operator"
      }
    }
    template {
      metadata {
        labels = {
          app = "msp-operator"
        }
      }
      spec {
        service_account_name = "mayastor-service-account"
        container {
          args = [
            "-e http://$(REST_SERVICE_HOST):8081",
            "--interval=${var.cache_period}"
          ]
          env {
            name  = "RUST_LOG"
            value = "info,msp_operator=info"
          }
          name = "msp-operator"

          image = format("%s/%s:%s", var.registry, var.image, var.tag)

          image_pull_policy = "Always"
          resources {
            limits = var.res_limits
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