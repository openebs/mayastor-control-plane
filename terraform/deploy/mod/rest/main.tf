variable "image" {}
variable "registry" {}
variable "tag" {}
variable "control_node" {}
variable "credentials" {}
variable "res_limits" {}
variable "request_timeout" {}
variable "res_requests" {}
variable "jaeger_agent_argument" {}
variable "rust_log" {}
variable "namespace" {}
variable "product_name_prefix" {}
variable "image_pull_policy" {}

resource "kubernetes_service" "api-rest" {
  metadata {
    labels = {
      "app" = "api-rest"
    }
    name      = "api-rest"
    namespace = var.namespace
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
      app = "api-rest"
    }
    type = "NodePort"
  }
}

resource "kubernetes_deployment" "rest_deployment" {
  metadata {
    labels = {
      app = "api-rest"
    }
    name      = "api-rest"
    namespace = var.namespace
  }
  spec {
    replicas = 1
    selector {
      match_labels = {
        app = "api-rest"
      }
    }
    template {
      metadata {
        labels = {
          app = "api-rest"
        }
      }
      spec {
        service_account_name = "${var.product_name_prefix}service-account"
        container {
          args = concat([
            "--dummy-certificates",
            "--no-auth",
            "--http=0.0.0.0:8081",
            "--request-timeout=${var.request_timeout}",
            "--core-grpc=https://agent-core:50051"
            ],
            var.jaeger_agent_argument
          )
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
            value = var.rust_log
          }

          image             = format("%s/%s:%s", var.registry, var.image, var.tag)
          image_pull_policy = var.image_pull_policy
          name              = "api-rest"

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
                  values   = var.control_node
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