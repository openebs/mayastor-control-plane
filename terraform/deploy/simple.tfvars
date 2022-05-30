### Example of how to change variables to use a less bulky local cluster
### terraform apply -var-file=./simple.tfvars
###

# io-engine daemon options
io_engine_hugepages_2Mi = "1Gi"
io_engine_cpus          = 1
io_engine_memory        = "1Gi"
io_engine_cpu_list      = "1"

# global registry and tag options
registry = "kvmhost:5000/mayadata"
#tag      = "latest"

# control plane configuration
#control_node = ["ksnode-1", "ksnode-2"]
control_resource_requests = {
  "cpu"    = "100m"
  "memory" = "100Mi"
}
control_resource_limits = {
  "cpu"    = "1000m"
  "memory" = "250Mi"
}

# csi node configuration
csi_node_grace_period = 2