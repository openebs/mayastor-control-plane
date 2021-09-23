variable "registry" {
  type        = string
  description = "The docker registery to pull from"
  default     = "mayadata"
}

variable "tag" {
  type        = string
  description = "The default docker image tag to use when pulling images, this applies to mayadata images only"
  default     = "develop"
}

variable "etcd_image" {
  type        = string
  default     = "docker.io/bitnami/etcd:3.4.15-debian-10-r43"
  description = "etcd image to use"
}

variable "control_node" {
  type        = string
  default     = "ksnode-1"
  description = "The on which control plane components are scheduled soft requirement"
}

variable "nats_image" {
  type        = string
  description = "amount of hugepages to allocate for mayastor"
  default     = "nats:2.2.6-alpine3.13"
}

variable "msp_operator_image" {
  type        = string
  description = "msp operator image to use"
  default     = "mcp-msp-operator"
}

variable "rest_image" {
  type    = string
  default = "mcp-rest"
}

variable "core_image" {
  type    = string
  default = "mcp-core"
}

variable "mayastor_image" {
  type        = string
  description = "mayastor image to use"
  default     = "mayastor"
}

variable "mayastor_hugepages_2Mi" {
  type        = string
  description = "amount of hugepages to allocate for mayastor"
  default     = "2Gi"
}

variable "mayastor_cpus" {
  type        = string
  description = "number of CPUs to use"
  default     = 2
}

variable "mayastor_memory" {
  type        = string
  description = "number of CPUs to use"
  default     = "4Gi"
}

variable "csi_agent_image" {
  type        = string
  description = "mayastor CSI agent image to use"
  default     = "mayastor-csi"
}

variable "csi_registar_image" {
  type        = string
  default     = "k8s.gcr.io/sig-storage/csi-node-driver-registrar:v2.2.0"
  description = "CSI sidecars to use"
}

variable "csi_attacher_image" {
  type        = string
  default     = "quay.io/k8scsi/csi-attacher:v3.1.0"
  description = "csi-attacher to use"
}

variable "csi_provisioner" {
  type        = string
  default     = "quay.io/k8scsi/csi-provisioner:v2.1.1"
  description = "csi-provisioner to use"
}
