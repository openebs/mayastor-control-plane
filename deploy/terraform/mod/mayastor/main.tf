variable "cpus" {}
variable "memory" {}
variable "hugepages" {}

variable "image" {}
variable "registry" {}
variable "tag" {}

resource "kubernetes_daemonset" "mayastor" {
  metadata {
    name      = "mayastor"
    namespace = "mayastor"

    labels = {
      "openebs/engine" = "mayastor"
    }
  }

  spec {
    selector {
      match_labels = {
        app = "mayastor"
      }
    }

    template {
      metadata {
        labels = {
          app = "mayastor"
        }
      }

      spec {
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
            path = "/var/local/mayastor/"
            type = "DirectoryOrCreate"
          }
        }

        init_container {
          name    = "message-bus-probe"
          image   = "busybox:latest"
          command = ["sh", "-c", "until nc -vz nats 4222; do echo \"Waiting for message bus...\"; sleep 1; done;"]
        }

        container {
          name = "mayastor"

          image = format("%s/%s:%s", var.registry, var.image, var.tag)
          args = [
            "-N$(MY_NODE_NAME)",
            "-g$(MY_POD_IP)",
            "-nnats",
            "-l2,3",
            "-pmayastor-etcd"
          ]

          port {
            name           = "mayastor"
            container_port = 10124
            protocol       = "TCP"
          }

          env {
            name  = "RUST_LOG"
            value = "mayastor=trace"
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

          image_pull_policy = "Always"

          security_context {
            privileged = true
          }
        }

        dns_policy = "ClusterFirstWithHostNet"

        node_selector = {
          "kubernetes.io/arch" = "amd64"
          "openebs.io/engine"  = "mayastor"
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
