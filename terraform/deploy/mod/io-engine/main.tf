variable "cpus" {}
variable "cpu_list" {}
variable "memory" {}
variable "hugepages" {}
variable "image" {}
variable "registry" {}
variable "tag" {}
variable "credentials" {}
variable "rust_log" {}
variable "namespace" {}
variable "product_name" {}
variable "image_pull_policy" {}

resource "kubernetes_daemonset" "io-engine" {
  metadata {
    name      = "io-engine"
    namespace = var.namespace

    labels = {
      "openebs.io/engine" = "io-engine"
    }
  }

  spec {
    selector {
      match_labels = {
        app = "io-engine"
      }
    }

    template {
      metadata {
        labels = {
          app = "io-engine"
        }
      }

      spec {
        image_pull_secrets {
          name = var.credentials
        }

        volume {
          name = "device"

          host_path {
            path = "/dev"
            type = "Directory"
          }
        }

        volume {
          name = "udev"

          host_path {
            path = "/run/udev"
            type = "Directory"
          }
        }

        volume {
          name = "dshm"

          empty_dir {
            medium = "Memory"
          }
        }

        volume {
          name = "hugepage"

          empty_dir {
            medium = "Memory"
          }
        }

        volume {
          name = "configlocation"

          host_path {
            path = "/var/local/io-engine/"
            type = "DirectoryOrCreate"
          }
        }

        init_container {
          name    = "registration-probe"
          image   = "busybox:latest"
          command = ["sh", "-c", "trap 'exit 1' TERM; until nc -vz agent-core 50051; do echo \"Waiting for registration service...\"; sleep 1; done;"]
        }

        container {
          name = "io-engine"

          image = format("%s/%s:%s", var.registry, var.image, var.tag)
          args = [
            "-N$(MY_NODE_NAME)",
            "-g$(MY_POD_IP)",
            "-Rhttps://agent-core:50051",
            format("-l%s", var.cpu_list),
            "-petcd:2379"
          ]

          port {
            name           = "io-engine"
            container_port = 10124
            protocol       = "TCP"
          }

          env {
            name  = "RUST_LOG"
            value = var.rust_log
          }

          env {
            name  = "NVME_KATO_MS"
            value = "1000"
          }

          env {
            name = "MY_NODE_NAME"

            value_from {
              field_ref {
                field_path = "spec.nodeName"
              }
            }
          }

          env {
            name = "MY_POD_IP"

            value_from {
              field_ref {
                field_path = "status.podIP"
              }
            }
          }

          resources {
            limits = {
              cpu           = var.cpus
              memory        = var.memory
              hugepages-2Mi = var.hugepages
            }


            requests = {
              cpu           = var.cpus
              memory        = var.memory
              hugepages-2Mi = var.hugepages
            }
          }

          volume_mount {
            name       = "device"
            mount_path = "/dev"
          }

          volume_mount {
            name       = "udev"
            mount_path = "/run/udev"
          }

          volume_mount {
            name       = "dshm"
            mount_path = "/dev/shm"
          }

          image_pull_policy = var.image_pull_policy

          security_context {
            privileged = true
          }
        }

        dns_policy = "ClusterFirstWithHostNet"

        node_selector = {
          "kubernetes.io/arch" = "amd64"
          "openebs.io/engine"  = var.product_name
        }

        host_network = true
      }
    }

    strategy {
      type = "RollingUpdate"

      rolling_update {
        max_unavailable = "1"
      }
    }

    min_ready_seconds = 10
  }
}
