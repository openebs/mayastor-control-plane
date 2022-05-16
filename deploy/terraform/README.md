## Requirements


The terraform scripts here assume that there is a k8s cluster with at least 3
nodes up and running.  The nodes on which to run should be labeled as usual.
Control plane components are to be scheduled on node(s) that have the
label `openebs.io/engine=none`.

The terraform plugin required is Kubernetes 2.4.1. other versions are likely to
work just fine.

A docker login is required at this point. It will read `~/.docker/config.json`
and can be changed if needed.

It's recommended that you copy the whole terraform dir to somewhere outside of
worktree.

Note that it assumes that all the resources it creates are not present. It will
fail otherwise.


## TODO

[ ] use expressions to calculate CPUs and memory of the list of the core
[ ] ask for user name and password if no docker log is provided
[ ] perhaps make namespace configurable
[ ] work out the naming of components
[ ] make service names etc, part of `variables.tf`
