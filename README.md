# Mayastor Control Plane "2.0"

[![CI-basic](https://mayastor-ci.mayadata.io/buildStatus/icon?job=Mayastor-Control-Plane%2Fmaster)](https://mayastor-ci.mayadata.io/blue/organizations/jenkins/Mayastor-Control-Plane/activity/)
[![Slack](https://img.shields.io/badge/JOIN-SLACK-blue)](https://kubernetes.slack.com/messages/openebs)
[![built with nix](https://builtwithnix.org/badge.svg)](https://builtwithnix.org)

## Links

- [Mayastor](https://github.com/openebs/Mayastor)

## :warning: Breaking Changes

On develop, there are breaking changes to the etcd key space which is used by the control-plane components and the
io-engine to retain information about replicas which contain the correct volume data.
An upgrade strategy is yet to be devised, but you may try these steps at your own risk:
1. Stop the mayastor-control-plane components.
1. When the pool operator is no longer running, remove the finalizers and delete all `MayastorPool` CR's
1. Enable compatibility mode with the mayastor data replica information
   ```bash
   $> ETCDCTL_API=3 etcdctl put "/openebs.io/mayastor/apis/v0/clusters/$KUBE_SYSTEM_UID/namespaces/$NAMESPACE/CoreRegistryConfig/db98f8bb-4afc-45d0-85b9-24c99cc443f2"
   '{"id":"db98f8bb-4afc-45d0-85b9-24c99cc443f2","registration":"Automatic", "mayastor_compat_v1": true}'
   ```
1. Move etcd data from `/namespace/$NAMESPACE/control-plane/` to `/openebs.io/mayastor/apis/v0/clusters/$KUBE_SYSTEM_UID/namespaces/$NAMESPACE/`
1. Install the new version of [mayastor-io-engine](https://github.com/openebs/mayastor) and [mayastor-control-plane](https://github.com/openebs/mayastor-control-plane) from [mayastor-extensions](https://github.com/openebs/mayastor-extensions)
1. Recreate the `MayastorPools` with the new CRD type `DiskPool`


## License

Mayastor is developed under Apache 2.0 license at the project level. Some components of the project are derived from
other open source projects and are distributed under their respective licenses.

### Contributions

Unless you explicitly state otherwise, any contribution intentionally submitted for
inclusion in Mayastor by you, as defined in the Apache-2.0 license, licensed as above,
without any additional terms or conditions.
