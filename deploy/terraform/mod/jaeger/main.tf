resource "null_resource" "before" {
}

resource "null_resource" "jaegertracing_repo" {
  provisioner "local-exec" {
    command = "helm repo add jaegertracing https://jaegertracing.github.io/helm-charts"
  }
  provisioner "local-exec" {
    command = "kubectl replace -f ${path.module}/crd.yaml --force"
  }
  provisioner "local-exec" {
    when       = destroy
    command    = "kubectl delete crd jaegers.jaegertracing.io"
    on_failure = continue
  }

  triggers = {
    "before" = null_resource.before.id
  }
}

resource "helm_release" "jaegertracing-operator" {
  depends_on = [null_resource.jaegertracing_repo]

  name      = "jaeger-operator"
  namespace = "mayastor"

  repository = "https://jaegertracing.github.io/helm-charts"
  chart      = "jaeger-operator"
  version    = "2.25.0"

  set {
    name  = "crd.install"
    value = "false"
  }

  set {
    name  = "jaeger.create"
    value = "false"
  }

  cleanup_on_fail = true
  skip_crds       = true
  wait            = true

  set {
    name  = "rbac.clusterRole"
    value = "true"
  }

  provisioner "local-exec" {
    command = "kubectl replace -f ${path.module}/jaeger.yaml --force"
  }
  provisioner "local-exec" {
    when       = destroy
    command    = "kubectl delete -f ${path.module}/jaeger.yaml"
    on_failure = continue
  }
}

resource "null_resource" "wait_deployment" {
  provisioner "local-exec" {
    command    = "${path.module}/wait_deployment.sh"
    on_failure = continue
  }
  triggers = {
    "jaegertracing-operator" = helm_release.jaegertracing-operator.id
  }
}

resource "kubernetes_service" "jaeger_node_port" {
  depends_on = [helm_release.jaegertracing-operator, null_resource.wait_deployment]
  metadata {
    labels = {
      "app" = "jaeger"
    }
    name      = "mayastor-jaeger-query-np"
    namespace = "mayastor"
  }
  spec {
    port {
      name        = "http-query"
      node_port   = 30012
      port        = 16686
      target_port = 16686
    }
    selector = {
      app = "jaeger"
    }
    type = "NodePort"
  }
}

output "jaeger_agent_argument" {
  value = "--jaeger=mayastor-jaeger-agent:6831"
}
