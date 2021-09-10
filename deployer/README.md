# Control Plane Deployer

Deploying all the `control plane agents` with all the trimmings is not an entirely straightforward exercise as there
are many parts to it, including the additional configuration steps to be able to run multiple `mayastor` instances
alongside each other.

The `deployer` tool facilitates this by creating a composable docker `"cluster"` which allows us to run any number of
`mayastor` instances, the `control plane agents` and any other pluggable components.

## Examples

**Using the help**
```textmate
[nix-shell:~/git/mayastor-control-plane]$ cargo run --bin deployer -- --help
  deployer --help
  agents 0.1.0
  Deployment actions

  USAGE:
      deployer <SUBCOMMAND>

  FLAGS:
      -h, --help       Prints help information
      -V, --version    Prints version information

  SUBCOMMANDS:
      help     Prints this message or the help of the given subcommand(s)
      list     List all running components
      start    Create and start all components
      stop     Stop and delete all components
```
The help can also be used on each subcommand.

**Deploying the cluster with default components**

```textmate
[nix-shell:~/git/mayastor-control-plane]$ cargo run --bin deployer -- start -m 2 -s
    Finished dev [unoptimized + debuginfo] target(s) in 0.10s
     Running `sh /home/tiago/git/myconfigs/maya/test_as_sudo.sh target/debug/deployer start -m 2 -s`
Sep 10 09:45:32.422  INFO deployer: Using options: CliArgs { action: Start(StartOptions { agents: [Core(Core)], kube_config: None, base_image: None, jaeger: false, elastic: false, kibana: false, no_rest: false, mayastors: 2, build: false, build_all: false, dns: false, show_info: true, cluster_label: 'io.mayastor.test'.'cluster', no_etcd: false, no_nats: false, cache_period: None, node_deadline: None, node_req_timeout: None, node_conn_timeout: None, store_timeout: None, reconcile_period: None, reconcile_idle_period: None, wait_timeout: None, reuse_cluster: false, developer_delayed: false }) }
[/mayastor-2] [10.1.0.7] /nix/store/3vxnxa72zdj6nnal3hgx0nnska26s8f4-mayastor/bin/mayastor -N mayastor-2 -g 10.1.0.7:10124 -p etcd.cluster:2379 -n nats.cluster:4222
[/mayastor-1] [10.1.0.6] /nix/store/3vxnxa72zdj6nnal3hgx0nnska26s8f4-mayastor/bin/mayastor -N mayastor-1 -g 10.1.0.6:10124 -p etcd.cluster:2379 -n nats.cluster:4222
[/rest] [10.1.0.5] /home/tiago/git/mayastor-control-plane/target/debug/rest --dummy-certificates --no-auth --https rest:8080 --http rest:8081 -n nats.cluster:4222
[/core] [10.1.0.4] /home/tiago/git/mayastor-control-plane/target/debug/core --store etcd.cluster:2379 -n nats.cluster:4222
[/etcd] [10.1.0.3] /nix/store/r2av08h9shmgqkzs87j7dhhaxxbpnqkd-etcd-3.3.25/bin/etcd --data-dir /tmp/etcd-data --advertise-client-urls http://0.0.0.0:2379 --listen-client-urls http://0.0.0.0:2379
[/nats] [10.1.0.2] /nix/store/ffwvclpayymciz4pdabqk7i3prhlz6ik-nats-server-2.2.1/bin/nats-server -DV
```

Notice the options which are printed out. They can be overridden - more on this later.

We could also use the `deploy` tool to inspect the components:
```textmate
[nix-shell:~/git/mayastor-control-plane]$ cargo run --bin deployer -- list
    Finished dev [unoptimized + debuginfo] target(s) in 0.11s
     Running `sh /home/tiago/git/myconfigs/maya/test_as_sudo.sh target/debug/deployer list`
Sep 10 09:44:45.299  INFO deployer: Using options: CliArgs { action: List(ListOptions { no_docker: false, format: None, cluster_label: 'io.mayastor.test'.'cluster' }) }
CONTAINER ID   IMAGE        COMMAND                  CREATED         STATUS         PORTS                              NAMES
89614520a612   <no image>   "/nix/store/3vxnxa72…"   2 minutes ago   Up 2 minutes                                      mayastor-2
82202a62dff9   <no image>   "/nix/store/3vxnxa72…"   2 minutes ago   Up 2 minutes                                      mayastor-1
9ef521180d3e   <no image>   "/home/tiago/git/chi…"   2 minutes ago   Up 2 minutes   0.0.0.0:8080-8081->8080-8081/tcp   rest
68aef7fae90c   <no image>   "/home/tiago/git/chi…"   2 minutes ago   Up 2 minutes                                      core
09a9530aac4f   <no image>   "/nix/store/r2av08h9…"   2 minutes ago   Up 2 minutes   0.0.0.0:2379-2380->2379-2380/tcp   etcd
60bfe758d371   <no image>   "/nix/store/ffwvclpa…"   2 minutes ago   Up 2 minutes   0.0.0.0:4222->4222/tcp             nats
```

As the previous logs shows, the `rest` server ports are mapped to your host on 8080/8081.
So, we for example list existing `nodes` (aka `mayastor` instances) as such:
```textmate
[nix-shell:~/git/Mayastor]$ curl -k https://localhost:8080/v0/nodes | jq
[
  {
    "id": "mayastor-1",
    "grpcEndpoint": "10.1.0.3:10124",
    "state": "Online"
  },
  {
    "id": "mayastor-2",
    "grpcEndpoint": "10.1.0.4:10124",
    "state": "Online"
  }
]
```

To tear-down the cluster, just run the stop command:
```textmate
[nix-shell:~/git/mayastor-control-plane]$ cargo run --bin deployer -- stop
    Finished dev [unoptimized + debuginfo] target(s) in 0.11s
     Running `sh /home/tiago/git/myconfigs/maya/test_as_sudo.sh target/debug/deployer stop`
Sep 10 09:47:37.497  INFO deployer: Using options: CliArgs { action: Stop(StopOptions { cluster_label: 'io.mayastor.test'.'cluster' }) }
```

For more information, please refer to the help argument on every command/subcommand.

### Debugging a Service

For example, to debug the rest server, we'd create a `cluster` without the rest server:
```textmate
[nix-shell:~/git/mayastor-control-plane]$ cargo run --bin deployer -- start --no-rest --show-info
    Finished dev [unoptimized + debuginfo] target(s) in 0.11s
     Running `sh /home/tiago/git/myconfigs/maya/test_as_sudo.sh target/debug/deployer start --no-rest --show-info`
Sep 10 09:48:06.445  INFO deployer: Using options: CliArgs { action: Start(StartOptions { agents: [Core(Core)], kube_config: None, base_image: None, jaeger: false, elastic: false, kibana: false, no_rest: true, mayastors: 1, build: false, build_all: false, dns: false, show_info: true, cluster_label: 'io.mayastor.test'.'cluster', no_etcd: false, no_nats: false, cache_period: None, node_deadline: None, node_req_timeout: None, node_conn_timeout: None, store_timeout: None, reconcile_period: None, reconcile_idle_period: None, wait_timeout: None, reuse_cluster: false, developer_delayed: false }) }
[/mayastor-1] [10.1.0.5] /nix/store/3vxnxa72zdj6nnal3hgx0nnska26s8f4-mayastor/bin/mayastor -N mayastor-1 -g 10.1.0.5:10124 -p etcd.cluster:2379 -n nats.cluster:4222
[/core] [10.1.0.4] /home/tiago/git/mayastor-control-plane/target/debug/core --store etcd.cluster:2379 -n nats.cluster:4222
[/etcd] [10.1.0.3] /nix/store/r2av08h9shmgqkzs87j7dhhaxxbpnqkd-etcd-3.3.25/bin/etcd --data-dir /tmp/etcd-data --advertise-client-urls http://0.0.0.0:2379 --listen-client-urls http://0.0.0.0:2379
[/nats] [10.1.0.2] /nix/store/ffwvclpayymciz4pdabqk7i3prhlz6ik-nats-server-2.2.1/bin/nats-server -DV
```

As you can see, there is no `rest` server started - go ahead and start your own!
This way you can make changes to this specific server and test them without destroying the state of the cluster.

```textmate
[nix-shell:~/git/mayastor-control-plane]$ cargo run --bin rest -- --dummy-certificates --no-auth
    Finished dev [unoptimized + debuginfo] target(s) in 0.10s
     Running `sh /home/tiago/git/myconfigs/maya/test_as_sudo.sh target/debug/rest --dummy-certificates --no-auth`
Sep 10 09:49:16.635  INFO common_lib::mbus_api::mbus_nats: Connecting to the nats server nats://0.0.0.0:4222...
Sep 10 09:49:17.065  INFO common_lib::mbus_api::mbus_nats: Successfully connected to the nats server nats://0.0.0.0:4222
Sep 10 09:49:17.067  INFO actix_server::builder: Starting 16 workers
Sep 10 09:49:17.069  INFO actix_server::builder: Starting "actix-web-service-0.0.0.0:8080" service on 0.0.0.0:8080
....
[nix-shell:~/git/Mayastor]$ curl -k https://localhost:8080/v0/nodes | jq
[
  {
    "id": "bob's your uncle",
    "grpcEndpoint": "10.1.0.3:10124",
    "state": "Unknown"
  }
]
```
