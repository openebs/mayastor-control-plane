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
variable "rust_log" {}
variable "namespace" {}
variable "product_name" {}
variable "image_pull_policy" {}

resource "kubernetes_service" "agent-core" {
  metadata {
    name      = "agent-core"
    namespace = var.namespace

    labels = {
      app = "agent-core"
    }
  }

  spec {
    port {
      name        = "grpc"
      port        = 50051
      target_port = 50051
    }

    selector = {
      app = "agent-core"
    }

    cluster_ip = "None"
  }
}

resource "kubernetes_deployment" "core_deployment" {
  metadata {
    labels = {
      app = "agent-core"
    }
    name      = "agent-core"
    namespace = var.namespace
  }
  spec {
    replicas = 1
    selector {
      match_labels = {
        app = "agent-core"
      }
    }

    template {
      metadata {
        labels = {
          app = "agent-core"
        }
      }
      spec {
        service_account_name = "${var.product_name}-service-account"
        container {
          args = concat([
            "-setcd",
            "--request-timeout=${var.request_timeout}",
            "--cache-period=${var.cache_period}",
            ],
            var.jaeger_agent_argument
          )
          image             = format("%s/%s:%s", var.registry, var.image, var.tag)
          image_pull_policy = var.image_pull_policy
          name              = "agent-core"
          resources {
            limits   = var.res_limits
            requests = var.res_requests
          }
          env {
            name = "MY_POD_NAME"
            value_from {
              field_ref {
                field_path = "metadata.name"
              }
            }
          }
          env {
            name = "MY_POD_NAMESPACE"
            value_from {
              field_ref {
                field_path = "metadata.namespace"
              }
            }
          }
          env {
            name  = "RUST_LOG"
            value = var.rust_log
          }
          port {
            name           = "grpc"
            container_port = 50051
            host_port      = 50051
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