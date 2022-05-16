### Example of how to change variables to use a less bulky local cluster
### terraform apply -var-file=./simple.tfvars
###

# io-engine daemon options
io-engine_hugepages_2Mi = "1Gi"
io-engine_cpus          = 1
io-engine_memory        = "1Gi"
io-engine_cpu_list      = "1"

# global registry and tag options
registry = "192.168.1.137:5000/mayadata"
tag      = "latest"

# control plane configuration
control_node = "ksnode-2"
control_resource_requests = {
  "cpu"    = "100m"
  "memory" = "100Mi"
}
control_resource_limits = {
  "cpu"    = "1000m"
  "memory" = "250Mi"
}

# csi agent configuration
csi_node_grace_period = 2