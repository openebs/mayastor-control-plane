module "k8s" {
  source = "./mod/k8s"

  num_nodes          = var.num_nodes
  ssh_user           = local.ssh_user
  private_key_path   = local.ssh_key_priv
  node_list          = module.provider.node_list
  overlay_cidr       = var.overlay_cidr
  nr_hugepages       = var.nr_hugepages
  kubernetes_version = var.kubernetes_version
  kubernetes_cni     = var.kubernetes_cni
  kubernetes_runtime = var.kubernetes_runtime
}

module "provider" {
  #source = "./mod/lxd"
  source = "./mod/libvirt"

  # lxd and libvirt
  ssh_user      = local.ssh_user
  ssh_key       = local.ssh_key_pub
  num_nodes     = var.num_nodes
  worker_memory = var.worker_memory
  worker_vcpu   = var.worker_vcpu
  master_memory = var.master_memory
  master_vcpu   = var.master_vcpu

  # libvirt
  image_path         = var.image_path
  hostname_formatter = var.hostname_formatter
  private_key_path   = local.ssh_key_priv
  disk_size          = var.disk_size
  pooldisk_size      = var.pooldisk_size
  qcow2_image        = local.qcow2_image
  network_mode       = var.network_mode
  bridge_name        = var.bridge_name
}

output "kluster" {
  value = module.provider.ks-cluster-nodes
}

locals {
  ssh_key_pub  = var.ssh_key_pub == "" ? file(pathexpand("~/.ssh/id_rsa.pub")) : file(var.ssh_key_pub)
  ssh_key_priv = var.ssh_key_priv == "" ? pathexpand("~/.ssh/id_rsa") : var.ssh_key_priv
  ssh_user     = var.ssh_user == "" ? data.local_file.current_username.content : var.ssh_user
  qcow2_image  = var.qcow2_image == "" ? pathexpand("~/terraform_images/ubuntu-20.04-server-cloudimg-amd64.img") : pathexpand(var.qcow2_image)
}

resource "null_resource" "generate_current_username" {
  provisioner "local-exec" {
    command = "echo -n $USER > ${path.module}/current_user.txt"
  }
}

data "local_file" "current_username" {
  depends_on = [null_resource.generate_current_username]
  filename   = "${path.module}/current_user.txt"
}

resource "null_resource" "default_kube_config" {
  provisioner "local-exec" {
    command = "terraform output kluster > ${path.module}/ansible-hosts"
  }
  provisioner "local-exec" {
    command = "ansible -i ${path.module}/ansible-hosts -a 'cat ~/.kube/config' master | tail -n+2 >${var.kubeconfig_output}"
  }
  provisioner "local-exec" {
    command = "kubectl get node --selector='!node-role.kubernetes.io/master,!node-role.kubernetes.io/control-plane' --no-headers | awk '{print $1}' | xargs -I% kubectl label node % openebs.io/engine=mayastor --overwrite"
  }
  depends_on = [
    module.k8s
  ]
}
