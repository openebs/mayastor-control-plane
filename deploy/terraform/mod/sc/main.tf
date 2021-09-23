resource "kubernetes_storage_class" "mirror" {
  metadata {
    name = "mayastor-nvmf-2"
  }
  storage_provisioner = "io.openebs.csi-mayastor"
  reclaim_policy      = "Delete"
  parameters = {
    repl      = "2"
    protocol  = "nvmf"
    ioTimeout = "30"
    local     = true
  }
}
