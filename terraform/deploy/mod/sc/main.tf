variable "namespace" {}
variable "product_name_prefix" {}

resource "kubernetes_storage_class" "mirror" {
  depends_on = [null_resource.cleanup_leftovers]
  metadata {
    name = "${var.product_name_prefix}nvmf-2"
  }
  storage_provisioner = "io.openebs.csi-mayastor"
  reclaim_policy      = "Delete"
  parameters = {
    repl      = "2"
    protocol  = "nvmf"
    ioTimeout = "30"
  }
}

resource "null_resource" "cleanup_leftovers" {
  provisioner "local-exec" {
    command    = "kubectl delete sc ${var.product_name_prefix}nvmf-2"
    on_failure = continue
  }
}
