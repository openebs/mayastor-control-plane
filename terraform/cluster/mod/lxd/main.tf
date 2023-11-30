provider "lxd" {
  generate_client_certificates = true
  accept_remote_certificate    = true
}

variable "num_nodes" {}
variable "worker_memory" {}
variable "worker_vcpu" {}
variable "master_memory" {}
variable "master_vcpu" {}
variable "ssh_user" {}
variable "ssh_key" {}
variable "hostname_formatter" {}
variable "private_key_path" {}

variable "pooldisk_size" {}
variable "network_mode" {}
variable "bridge_name" {}
variable "image_path" {}
variable "disk_size" {}
variable "qcow2_image" {}

resource "lxd_cached_image" "ubuntu" {
  source_remote = "ubuntu"
  source_image  = "jammy/amd64"
}

locals {
  # user data that we pass to cloud init that reads variables from variables.tf and
  # passes them to a template file to be filled in
  user_data = [
    for node_index in range(var.num_nodes) : templatefile("${path.module}/cloud_init.tmpl", {
      ssh_user = var.ssh_user, ssh_key = var.ssh_key, hostname = format(var.hostname_formatter, node_index + 1)
    })
  ]
  # likewise for networking
  network_config = templatefile("${path.module}/network_config.cfg", {})
}

resource "null_resource" "lxd_init" {
  provisioner "local-exec" {
    command = "sudo lxd init --storage-backend=dir --auto || true"
  }
}

resource "null_resource" "lxd_stop_force" {
  provisioner "local-exec" {
    when = destroy
    # todo: should use hostname_formatter
    command = format("lxc stop ksnode-%d --force", count.index + 1)
  }
  count = var.num_nodes
  depends_on = [
    lxd_container.c8s
  ]
}

resource "lxd_container" "c8s" {
  count     = var.num_nodes
  name      = format(var.hostname_formatter, count.index + 1)
  image     = lxd_cached_image.ubuntu.fingerprint
  ephemeral = false

  # be careful with raw.lxc it has to be key=value\nkey=value

  config = {
    "boot.autostart"       = true
    "raw.lxc"              = "lxc.mount.auto = proc:rw cgroup:rw sys:rw\nlxc.apparmor.profile = unconfined\nlxc.cgroup.devices.allow = a\nlxc.cap.drop="
    "linux.kernel_modules" = "ip_tables,ip6_tables,nf_nat,overlay,netlink_diag,br_netfilter,nvme_tcp"
    "security.nesting"     = true
    "security.privileged"  = true
    "cloud-init.user-data"       = local.user_data[count.index]
    "cloud-init.network-config"  = local.network_config
  }

  limits = {
    memory   = format("%dMiB", count.index == 0 ? var.master_memory : var.worker_memory)
    # For the moment this doesn't as io-engine then can't set its core affinity...
    # cpu      = count.index == 0 ? var.master_vcpu : var.worker_vcpu
  }

  device {
    name = "kmsg"
    type = "unix-char"
    properties = {
      path   = "/dev/kmsg"
      source = "/dev/kmsg"
    }
  }

  provisioner "remote-exec" {
    inline = ["cloud-init status --wait"]
    connection {
      type        = "ssh"
      user        = var.ssh_user
      host        = self.ip_address
      private_key = file(var.private_key_path)
    }
  }
  depends_on = [
    null_resource.lxd_init
  ]
}

# generate the inventory template for ansible
output "ks-cluster-nodes" {
  value = <<EOT
[master]
${lxd_container.c8s.0.name} ansible_host=${lxd_container.c8s.0.ip_address} ansible_user=${var.ssh_user} ansible_ssh_private_key_file=${var.private_key_path} ansible_ssh_common_args='-o StrictHostKeyChecking=no'

[nodes]%{for ip in lxd_container.c8s.*~}
%{if ip.name != "${format(var.hostname_formatter, 1)}"}${ip.name} ansible_host=${ip.ip_address} ansible_user=${var.ssh_user} ansible_ssh_private_key_file=${var.private_key_path} ansible_ssh_common_args='-o StrictHostKeyChecking=no'%{endif}
%{endfor~}
EOT
}

output "node_list" {
  value = lxd_container.c8s.*.ip_address
}

terraform {
  required_providers {
    lxd = {
      source = "terraform-lxd/lxd"
    }
  }
}
