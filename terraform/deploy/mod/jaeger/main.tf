variable "namespace" {}

resource "null_resource" "jaegertracing_repo" {
  provisioner "local-exec" {
    command = "helm repo add jaegertracing https://jaegertracing.github.io/helm-charts"
  }
}

resource "helm_release" "jaegertracing-operator" {
  depends_on = [null_resource.jaegertracing_repo]

  name      = "jaeger-operator"
  namespace = var.namespace

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
    command = "kubectl replace -f ${path.module}/crd.yaml --force"
  }
  provisioner "local-exec" {
    command = "NAMESPACE=${var.namespace} envsubst < ${path.module}/jaeger.yaml | kubectl replace --force -f -"
  }
  provisioner "local-exec" {
    when       = destroy
    command    = "kubectl -f ${path.module}/jaeger.yaml"
    on_failure = continue
  }
  provisioner "local-exec" {
    when       = destroy
    command    = "kubectl delete crd jaegers.jaegertracing.io"
    on_failure = continue
  }
}

resource "null_resource" "wait_deployment" {
  provisioner "local-exec" {
    command    = "${path.module}/wait_deployment.sh ${var.namespace}"
    on_failure = continue
  }
  triggers = {
    "jaegertracing-operator" = helm_release.jaegertracing-operator.id
  }
}

output "jaeger_agent_argument" {
  value = "--jaeger=jaeger-agent:6831"
}
