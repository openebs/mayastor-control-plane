provider "lxd" {}

variable "num_nodes" {
  type    = number
  default = 3
}

variable "memory" {}

variable "vcpu" {}

resource "lxd_cached_image" "ubuntu" {
  source_remote = "ubuntu"
  source_image  = "bionic/amd64"
}

variable "ssh_key" {}

variable "ssh_user" {}

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

resource "lxd_container" "c8s" {
  count     = var.num_nodes
  name      = format("ksnode-%d", count.index + 1)
  image     = lxd_cached_image.ubuntu.fingerprint
  ephemeral = false

  # be careful with raw.lxc it has to be key=value\nkey=value

  config = {
    "boot.autostart"       = true
    "raw.lxc"              = "lxc.mount.auto = proc:rw cgroup:rw sys:rw\nlxc.apparmor.profile = unconfined\nlxc.cgroup.devices.allow = a\nlxc.cap.drop="
    "linux.kernel_modules" = "ip_tables,ip6_tables,nf_nat,overlay,netlink_diag,br_netfilter,nvme_tcp"
    "security.nesting"     = true
    "security.privileged"  = true
    "user.user-data"       = local.user_data[count.index]
  }

  device {
    name = "kmsg"
    type = "unix-char"
    properties = {
      path   = "/dev/kmsg"
      source = "/dev/kmsg"
    }
  }
}

output "node_list" {
  value = lxd_container.c8s.*.ip_address
}

variable "image_path" {}
variable "hostname_formatter" {}
variable "private_key_path" {}
variable "disk_size" {}
variable "qcow2_image" {}
output "ks-cluster-nodes" {
  value = ""
}
