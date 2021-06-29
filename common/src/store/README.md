# Control Plane Store

A key-value (kv) store has been chosen to persist control plane information. Such information includes:
- Configuration
- Per-volume policies i.e. replica replacement policy

etcd has been chosen as the kv store due to its wide adoption and familiarity.
