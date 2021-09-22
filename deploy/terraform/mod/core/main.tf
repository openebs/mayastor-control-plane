variable "image" {}
variable "registry" {}
variable "tag" {}
variable "control_node" {}

variable "credentials" {}

resource "kubernetes_deployment" "core_deployment" {
  metadata {
    labels = {
      app = "core"
    }
    name      = "core-agents"
    namespace = "mayastor"
  }
  spec {
    replicas = 1
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
          args = [
            "-smayastor-etcd",
            "-nnats"
          ]
          image             = format("%s/%s:%s", var.registry, var.image, var.tag)
          image_pull_policy = "Always"
          name              = "core"
          resources {
            limits = {
              cpu    = "1000m"
              memory = "1Gi"
            }
            requests = {
              cpu    = "250m"
              memory = "500Mi"
            }
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