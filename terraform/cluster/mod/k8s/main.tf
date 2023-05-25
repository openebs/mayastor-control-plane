variable "k8s_cluster_token" {
  default = "abcdef.1234567890abcdef"
}

variable "overlay_cidr" {}
variable "num_nodes" {}

variable "ssh_user" {}
variable "private_key_path" {}

variable "node_list" {}

variable "nr_hugepages" {}

variable "kubernetes_version" {}
variable "kubernetes_runtime" {}
variable "kubernetes_cni" {}


resource "null_resource" "k8s" {
  count = var.num_nodes

  connection {
    host        = element(var.node_list, count.index)
    user        = var.ssh_user
    private_key = file(var.private_key_path)
  }

  provisioner "file" {
    content     = local.master-configuration
    destination = "/tmp/kubeadm_config.yaml"
  }


  provisioner "remote-exec" {
    inline = [element(local.install, count.index)]
  }

  provisioner "remote-exec" {
    inline = [
      count.index == 0 ? local.master : local.node
    ]
  }
}

locals {
  master-configuration = templatefile("${path.module}/kubeadm_config.yaml", {
    master_ip = element(var.node_list, 0)
    token     = var.k8s_cluster_token
    cert_sans = element(var.node_list, 0)
    pod_cidr  = var.overlay_cidr
  })
  install = [
    for _ in range(var.num_nodes) : templatefile("${path.module}/repo.sh", {
      kube_version = var.kubernetes_version
      kube_runtime = var.kubernetes_runtime
    })
  ]
  master = templatefile("${path.module}/master.sh", {
    cni_url = var.kubernetes_cni
  })
  node = templatefile("${path.module}/node.sh", {
    master_ip    = element(var.node_list, 0)
    token        = var.k8s_cluster_token
    nr_hugepages = var.nr_hugepages
  })
}
