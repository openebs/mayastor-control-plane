variable "image" {}
variable "registry" {}
variable "tag" {}
variable "control_node" {}
variable "credentials" {}
variable "csi_provisioner" {}
variable "csi_attacher_image" {}
variable "jaeger_agent_argument" {}

resource "kubernetes_deployment" "deployment_csi_controller" {
  metadata {
    labels = {
      app = "csi-controller"
    }
    name      = "csi-controller"
    namespace = "mayastor"
  }
  spec {
    replicas = 1
    selector {
      match_labels = {
        app = "csi-controller"
      }
    }
    template {
      metadata {
        labels = {
          app = "csi-controller"
        }
      }
      spec {
        host_network         = true
        service_account_name = "mayastor-service-account"
        dns_policy           = "ClusterFirstWithHostNet"

        volume {
          name = "socket-dir"
          empty_dir {}
        }

        container {
          name  = "csi-provisioner"
          image = var.csi_provisioner
          args = [
            "--v=2",
            "--csi-address=$(ADDRESS)",
            "--feature-gates=Topology=true",
            "--strict-topology=false",
            "--default-fstype=ext4"
          ]
          env {
            name  = "ADDRESS"
            value = "/var/lib/csi/sockets/pluginproxy/csi.sock"
          }

          image_pull_policy = "IfNotPresent"

          volume_mount {
            name       = "socket-dir"
            mount_path = "/var/lib/csi/sockets/pluginproxy/"
          }
        }

        container {
          name  = "csi-attacher"
          image = var.csi_attacher_image
          args = [
            "--v=2",
            "--csi-address=$(ADDRESS)"
          ]
          env {
            name  = "ADDRESS"
            value = "/var/lib/csi/sockets/pluginproxy/csi.sock"
          }
          image_pull_policy = "IfNotPresent"

          volume_mount {
            name       = "socket-dir"
            mount_path = "/var/lib/csi/sockets/pluginproxy/"
          }
        }

        container {
          name              = "csi-controller"
          image             = format("%s/%s:%s", var.registry, var.image, var.tag)
          image_pull_policy = "Always"

          args = concat([
            "--csi-socket=/var/lib/csi/sockets/pluginproxy/csi.sock",
            "--rest-endpoint=http://$(REST_SERVICE_HOST):8081",
          ], var.jaeger_agent_argument)
          env {
            name  = "RUST_LOG"
            value = "info,csi_controller=trace"
          }
          env {
            name  = "ADDRESS"
            value = "/var/lib/csi/sockets/pluginproxy/csi.sock"
          }

          volume_mount {
            name       = "socket-dir"
            mount_path = "/var/lib/csi/sockets/pluginproxy/"
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
