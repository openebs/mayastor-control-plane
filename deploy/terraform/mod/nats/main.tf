
variable "nats_image" {
}

variable "control_node" {}

resource "kubernetes_config_map" "nats_config" {
  metadata {
    name      = "nats-config"
    namespace = "mayastor"
  }

  data = {
    "nats.conf" = <<CONF
      pid_file: "/var/run/nats/nats.pid"
      http: 8222
      CONF
  }
}

resource "kubernetes_service" "nats" {
  metadata {
    name      = "nats"
    namespace = "mayastor"

    labels = {
      app = "nats"
    }
  }

  spec {
    port {
      name = "client"
      port = 4222
    }

    port {
      name = "cluster"
      port = 6222
    }

    port {
      name = "monitor"
      port = 8222
    }

    port {
      name = "metrics"
      port = 7777
    }

    selector = {
      app = "nats"
    }

    cluster_ip = "None"
  }
}

resource "kubernetes_stateful_set" "nats" {
  metadata {
    name      = "nats"
    namespace = "mayastor"

    labels = {
      app = "nats"
    }
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "nats"
      }
    }

    template {
      metadata {
        labels = {
          app = "nats"
        }
      }

      spec {
        volume {
          name = "config-volume"

          config_map {
            name = "nats-config"
          }
        }

        volume {
          name = "pid"
        }

        container {
          name    = "nats"
          image   = var.nats_image
          command = ["nats-server", "--config", "/etc/nats-config/nats.conf"]

          port {
            name           = "client"
            host_port      = 4222
            container_port = 4222
          }

          port {
            name           = "cluster"
            container_port = 6222
          }

          port {
            name           = "monitor"
            container_port = 8222
          }

          port {
            name           = "metrics"
            container_port = 7777
          }

          env {
            name = "POD_NAME"

            value_from {
              field_ref {
                field_path = "metadata.name"
              }
            }
          }

          env {
            name = "POD_NAMESPACE"

            value_from {
              field_ref {
                field_path = "metadata.namespace"
              }
            }
          }

          env {
            name  = "CLUSTER_ADVERTISE"
            value = "$(POD_NAME).nats.$(POD_NAMESPACE).svc"
          }

          volume_mount {
            name       = "config-volume"
            mount_path = "/etc/nats-config"
          }

          volume_mount {
            name       = "pid"
            mount_path = "/var/run/nats"
          }

          liveness_probe {
            http_get {
              path = "/"
              port = "8222"
            }

            initial_delay_seconds = 10
            timeout_seconds       = 5
          }

          readiness_probe {
            http_get {
              path = "/"
              port = "8222"
            }

            initial_delay_seconds = 10
            timeout_seconds       = 5
          }

          lifecycle {
            pre_stop {
              exec {
                command = ["/bin/sh", "-c", "/nats-server -sl=ldm=/var/run/nats/nats.pid && /bin/sleep 60"]
              }
            }
          }
        }

        container {
          name    = "reloader"
          image   = "connecteverything/nats-server-config-reloader:0.6.0"
          command = ["nats-server-config-reloader", "-pid", "/var/run/nats/nats.pid", "-config", "/etc/nats-config/nats.conf"]

          volume_mount {
            name       = "config-volume"
            mount_path = "/etc/nats-config"
          }

          volume_mount {
            name       = "pid"
            mount_path = "/var/run/nats"
          }
        }

        termination_grace_period_seconds = 60
        share_process_namespace          = true

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

    service_name = "nats"
  }
}