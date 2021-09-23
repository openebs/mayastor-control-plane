variable "image" {}
variable "registry" {}
variable "tag" {}
variable "control_node" {}
variable "credentials" {}
resource "kubernetes_service" "service_mayastor_rest" {
  metadata {
    labels = {
      "app" = "rest"
    }
    name      = "rest"
    namespace = "mayastor"
  }
  spec {
    port {
      name        = "https"
      node_port   = 30010
      port        = 8080
      target_port = 8080
    }

    port {
      name        = "http"
      node_port   = 30011
      port        = 8081
      target_port = 8081
    }
    selector = {
      app = "rest"
    }
    type = "NodePort"
  }
}

resource "kubernetes_deployment" "rest_deployment" {
  metadata {
    labels = {
      app = "rest"
    }
    name      = "rest"
    namespace = "mayastor"
  }
  spec {
    replicas = 1
    selector {
      match_labels = {
        app = "rest"
      }
    }
    template {
      metadata {
        labels = {
          app = "rest"
        }
      }
      spec {
        service_account_name = "mayastor-service-account"
        container {
          args = [
            "--dummy-certificates",
            "--no-auth",
            "-nnats",
            "--http=0.0.0.0:8081",
            "--request-timeout=30s",
          ]
          port {
            name           = "https"
            container_port = 8080
          }
          port {
            name           = "http"
            container_port = 8081
          }

          env {
            name  = "RUST_LOG"
            value = "info,msp_operator=info"
          }

          image             = format("%s/%s:%s", var.registry, var.image, var.tag)
          image_pull_policy = "Always"
          name              = "rest-service"

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