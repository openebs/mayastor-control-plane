variable "ssh_key_priv" {
  type        = string
  description = "SSH private key path"
  default     = ""
}

variable "ssh_key_pub" {
  type        = string
  description = "SSH pub key path"
  default     = ""
}

variable "ssh_user" {
  type        = string
  description = "The user that should be created and who has sudo power"
  default     = "tiago"
}

variable "image_path" {
  type        = string
  description = "Where the images will be stored"
  default     = "/images"
}

variable "disk_size" {
  type        = number
  description = "The size of the root disk in bytes"
  default     = 10737418240
}

variable "pooldisk_size" {
  type        = number
  description = "The size of the pool disk in bytes"
  default     = 10737418240
}

variable "hostname_formatter" {
  type    = string
  default = "ksnode-%d"
}

variable "num_nodes" {
  type        = number
  default     = 3
  description = "The number of nodes to create (should be > 1)"
}

variable "network_mode" {
  type        = string
  default     = "nat"
  description = "mode can be: nat, bridge, default. If default, the default libvirt network is used."
}

variable "bridge_name" {
  type        = string
  default     = "virbr0"
  description = "Name of the bridge - when using bridge network mode."
}

variable "qcow2_image" {
  type        = string
  description = "Ubuntu image for VMs - only needed for libvirt provider"
  default     = "~/terraform_images/ubuntu-20.04-server-cloudimg-amd64.img"
  #default = "~/terraform_images/ubuntu-22.04-server-cloudimg-amd64.img"
}

variable "overlay_cidr" {
  type        = string
  description = "CIDR, classless inter-domain routing"
  default     = "10.244.0.0/16"
}

variable "nr_hugepages" {
  type        = string
  description = "Number of Huge pages"
  default     = "1024"
}

variable "memory" {
  type        = number
  default     = 6144
  description = "Amount of memory (MiB) allocated to each node - only needed for libvirt provider"
}

variable "vcpu" {
  type        = number
  default     = 3
  description = "Virtual CPUs allocated to each node - only needed for libvirt provider"
}

variable "kubernetes_version" {
  type        = string
  description = "Version of all kubernetes components"
  #default     = "1.24.0-00"
  default = ""
}

variable "kubeconfig_output" {
  type        = string
  description = "Where to copy the kube configuration file to."
  default     = "~/.kube/config"
}
