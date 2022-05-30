variable "image" {}
variable "control_node" {}
variable "namespace" {}
variable "product_name" {}

resource "kubernetes_stateful_set" "etcd" {
  metadata {
    name      = "etcd"
    namespace = var.namespace

    labels = {
      "app.kubernetes.io/instance" = var.product_name
      "app.kubernetes.io/name"     = "etcd"
    }
  }

  spec {
    replicas = 1
    selector {
      match_labels = {
        "app.kubernetes.io/instance" = var.product_name
        "app.kubernetes.io/name"     = "etcd"
      }
    }

    template {
      metadata {
        labels = {
          app                          = "etcd"
          "app.kubernetes.io/instance" = var.product_name
          "app.kubernetes.io/name"     = "etcd"
        }
      }

      spec {
        volume {
          name = "data"
        }

        container {
          name  = "etcd"
          image = var.image

          port {
            name           = "client"
            container_port = 2379
            protocol       = "TCP"
          }

          port {
            name           = "peer"
            container_port = 2380
            protocol       = "TCP"
          }

          env {
            name  = "BITNAMI_DEBUG"
            value = "false"
          }

          env {
            name = "MY_POD_IP"

            value_from {
              field_ref {
                field_path = "status.podIP"
              }
            }
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
            name  = "ETCDCTL_API"
            value = "3"
          }

          env {
            name  = "ETCD_ON_K8S"
            value = "yes"
          }

          env {
            name  = "ETCD_START_FROM_SNAPSHOT"
            value = "no"
          }

          env {
            name  = "ETCD_DISASTER_RECOVERY"
            value = "no"
          }

          env {
            name  = "ETCD_NAME"
            value = "$(MY_POD_NAME)"
          }

          env {
            name  = "ETCD_DATA_DIR"
            value = "/bitnami/etcd/data"
          }

          env {
            name  = "ETCD_LOG_LEVEL"
            value = "info"
          }

          env {
            name  = "ALLOW_NONE_AUTHENTICATION"
            value = "yes"
          }

          env {
            name  = "ETCD_ADVERTISE_CLIENT_URLS"
            value = format("http://$(MY_POD_NAME).etcd-headless.%s.svc.cluster.local:2379", var.namespace)
          }

          env {
            name  = "ETCD_LISTEN_CLIENT_URLS"
            value = "http://0.0.0.0:2379"
          }

          env {
            name  = "ETCD_INITIAL_ADVERTISE_PEER_URLS"
            value = format("http://$(MY_POD_NAME).etcd-headless.%s.svc.cluster.local:2380", var.namespace)
          }

          env {
            name  = "ETCD_LISTEN_PEER_URLS"
            value = "http://0.0.0.0:2380"
          }

          volume_mount {
            name       = "data"
            mount_path = "/bitnami/etcd"
          }

          liveness_probe {
            exec {
              command = ["/opt/bitnami/scripts/etcd/healthcheck.sh"]
            }

            initial_delay_seconds = 10
            timeout_seconds       = 5
            period_seconds        = 5
            success_threshold     = 1
            failure_threshold     = 5
          }

          readiness_probe {
            exec {
              command = ["/opt/bitnami/scripts/etcd/healthcheck.sh"]
            }

            initial_delay_seconds = 10
            timeout_seconds       = 5
            period_seconds        = 5
            success_threshold     = 1
            failure_threshold     = 5
          }

          image_pull_policy = "IfNotPresent"

          security_context {
            run_as_user     = 1001
            run_as_non_root = true
          }
        }

        service_account_name = "default"

        security_context {
          fs_group = 1001
        }

        affinity {
          pod_anti_affinity {
            required_during_scheduling_ignored_during_execution {
              label_selector {
                match_labels = {
                  "app.kubernetes.io/instance" = var.product_name
                  "app.kubernetes.io/name"     = "etcd"
                }
              }

              namespaces   = [var.namespace]
              topology_key = "kubernetes.io/hostname"
            }
          }

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
      }
    }

    service_name          = "etcd-headless"
    pod_management_policy = "Parallel"

    update_strategy {
      type = "RollingUpdate"
    }
  }
}

resource "kubernetes_service" "etcd" {
  metadata {
    name      = "etcd"
    namespace = var.namespace

    labels = {
      "app.kubernetes.io/instance" = var.product_name
      "app.kubernetes.io/name"     = "etcd"
    }
  }

  spec {
    port {
      name        = "client"
      port        = 2379
      target_port = "client"
    }

    port {
      name        = "peer"
      port        = 2380
      target_port = "peer"
    }

    selector = {
      "app.kubernetes.io/instance" = var.product_name
      "app.kubernetes.io/name"     = "etcd"
    }

    type = "ClusterIP"
  }
}

resource "kubernetes_service" "etcd_headless" {
  metadata {
    name      = "etcd-headless"
    namespace = var.namespace

    labels = {
      "app.kubernetes.io/instance" = var.product_name
      "app.kubernetes.io/name"     = "etcd"
    }

    annotations = {
      "service.alpha.kubernetes.io/tolerate-unready-endpoints" = "true"
    }
  }

  spec {
    port {
      name        = "client"
      port        = 2379
      target_port = "client"
    }

    port {
      name        = "peer"
      port        = 2380
      target_port = "peer"
    }

    selector = {
      "app.kubernetes.io/instance" = var.product_name
      "app.kubernetes.io/name"     = "etcd"
    }

    cluster_ip                  = "None"
    type                        = "ClusterIP"
    publish_not_ready_addresses = true
  }
}
