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
  default     = ""
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
  #default     = "~/terraform_images/ubuntu-20.04-server-cloudimg-amd64.img"
  default = "~/terraform_images/ubuntu-22.04-server-cloudimg-amd64.img"
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

variable "worker_memory" {
  type        = number
  default     = 6144
  description = "Amount of memory (MiB) allocated to each worker node - only needed for libvirt provider"
}

variable "worker_vcpu" {
  type        = number
  default     = 3
  description = "Virtual CPUs allocated to each worker node - only needed for libvirt provider"
}

variable "master_memory" {
  type        = number
  default     = 3192
  description = "Amount of memory (MiB) allocated to the master node - only needed for libvirt provider"
}

variable "master_vcpu" {
  type        = number
  default     = 2
  description = "Virtual CPUs allocated to the master node - only needed for libvirt provider"
}

variable "kubernetes_version" {
  type        = string
  description = "Version of all kubernetes components"
  default     = "1.26.0-00"
}

variable "kubeconfig_output" {
  type        = string
  description = "Where to copy the kube configuration file to."
  default     = "~/.kube/config"
}

variable "kubernetes_runtime" {
  type        = string
  description = "Container runtime (crio or containerd)"
  default     = "containerd"
}

variable "kubernetes_cni" {
  type        = string
  description = "URL containing the CNI plugin yaml"
  # default = "https://raw.githubusercontent.com/cloudnativelabs/kube-router/v1.5.4/daemonset/kubeadm-kuberouter.yaml"
  default     = "https://raw.githubusercontent.com/flannel-io/flannel/v0.21.5/Documentation/kube-flannel.yml"
}
