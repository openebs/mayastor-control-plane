# Kubectl Plugin

## Overview
The kubectl plugin has been created in accordance with the instructions outlined in the [official documentation](https://kubernetes.io/docs/tasks/extend-kubectl/kubectl-plugins/).


The name of the plugin binary dictates how it is used. From the documentation:
> For example, a plugin named `kubectl-foo` provides a command `kubectl foo`.

In our case the name of the binary is specified in the Cargo.toml file as `kubectl-mayastor`, therefore the command is `kubectl mayastor`.

## Usage
**The plugin must be placed in your `PATH` in order for it to be used.**

To make the plugin as intuitive as possible, every attempt has been made to make the usage as similar to that of the standard `kubectl` command line utility as possible.

The general command structure is `kubectl mayastor <operation> <resource>` where the operation defines what should be performed (i.e. `get`, `scale`) and the resource defines what the operation should be performed on (i.e. `volumes`, `pools`).

The plugin needs to be able to connect to the REST server in order to make the appropriate REST calls. The IP address and port number of the REST server can be provided through the use of the `--rest` command line argument. If the `--rest` argument is omitted, the plugin will attempt to make use of the kubeconfig file to determine the IP of the master node of the cluster. Should the kubeconfig file contain multiple clusters, then the first cluster will be selected.


### Examples and Outputs


<details>
<summary> General Resources operations </summary>

1. Get Volumes
```
❯ kubectl mayastor get volumes
 ID                                    REPLICAS TARGET-NODE  ACCESSIBILITY STATUS  SIZE
 18e30e83-b106-4e0d-9fb6-2b04e761e18a  4        mayastor-1   nvmf          Online  10485761
 0c08667c-8b59-4d11-9192-b54e27e0ce0f  4        mayastor-2   <none>        Online  10485761

```
2. Get Volume by ID
```
❯ kubectl mayastor get volume 18e30e83-b106-4e0d-9fb6-2b04e761e18a
 ID                                    REPLICAS TARGET-NODE  ACCESSIBILITY STATUS  SIZE
 18e30e83-b106-4e0d-9fb6-2b04e761e18a  4        mayastor-1   nvmf          Online  10485761

```
3. Get Pools
```
❯ kubectl mayastor get pools
 ID               TOTAL CAPACITY  USED CAPACITY  DISKS                                                     NODE      STATUS  MANAGED
 mayastor-pool-1  5360320512      1111490560     aio:///dev/vdb?uuid=d8a36b4b-0435-4fee-bf76-f2aef980b833  kworker1  Online  true
 mayastor-pool-2  5360320512      2172649472     aio:///dev/vdc?uuid=bb12ec7d-8fc3-4644-82cd-dee5b63fc8c5  kworker1  Online  true
 mayastor-pool-3  5360320512      3258974208     aio:///dev/vdb?uuid=f324edb7-1aca-41ec-954a-9614527f77e1  kworker2  Online  false
```
4. Get Pool by ID
```
❯ kubectl mayastor get pool mayastor-pool-1
 ID               TOTAL CAPACITY  USED CAPACITY  DISKS                                                     NODE      STATUS  MANAGED
 mayastor-pool-1  5360320512      1111490560     aio:///dev/vdb?uuid=d8a36b4b-0435-4fee-bf76-f2aef980b833  kworker1  Online  true
```
5. Get Nodes
```
❯ kubectl mayastor get nodes
 ID          GRPC ENDPOINT   STATUS
 mayastor-2  10.1.0.7:10124  Online
 mayastor-1  10.1.0.6:10124  Online
 mayastor-3  10.1.0.8:10124  Online
```
6. Get Node by ID
```
❯ kubectl mayastor get node mayastor-2
 ID          GRPC ENDPOINT   STATUS
 mayastor-2  10.1.0.7:10124  Online
```

7. Get Volume(s)/Pool(s)/Node(s) to a specific Output Format
```
❯ kubectl mayastor -ojson get volumes
[{"spec":{"num_replicas":2,"size":67108864,"status":"Created","target":{"node":"ksnode-2","protocol":"nvmf"},"uuid":"5703e66a-e5e5-4c84-9dbe-e5a9a5c805db","topology":{"explicit":{"allowed_nodes":["ksnode-1","ksnode-3","ksnode-2"],"preferred_nodes":["ksnode-2","ksnode-3","ksnode-1"]}},"policy":{"self_heal":true}},"state":{"target":{"children":[{"state":"Online","uri":"bdev:///ac02cf9e-8f25-45f0-ab51-d2e80bd462f1?uuid=ac02cf9e-8f25-45f0-ab51-d2e80bd462f1"},{"state":"Online","uri":"nvmf://192.168.122.6:8420/nqn.2019-05.io.openebs:7b0519cb-8864-4017-85b6-edd45f6172d8?uuid=7b0519cb-8864-4017-85b6-edd45f6172d8"}],"deviceUri":"nvmf://192.168.122.234:8420/nqn.2019-05.io.openebs:nexus-140a1eb1-62b5-43c1-acef-9cc9ebb29425","node":"ksnode-2","rebuilds":0,"protocol":"nvmf","size":67108864,"state":"Online","uuid":"140a1eb1-62b5-43c1-acef-9cc9ebb29425"},"size":67108864,"status":"Online","uuid":"5703e66a-e5e5-4c84-9dbe-e5a9a5c805db"}}]

```

```
❯ kubectl mayastor -oyaml get pools
---
- id: mayastor-pool-1
  state:
    capacity: 5360320512
    disks:
      - "aio:///dev/vdb?uuid=d8a36b4b-0435-4fee-bf76-f2aef980b833"
    id: mayastor-pool-1
    node: kworker1
    status: Online
    used: 1111490560
- id: mayastor-pool-2
  state:
    capacity: 5360320512
    disks:
      - "aio:///dev/vdc?uuid=bb12ec7d-8fc3-4644-82cd-dee5b63fc8c5"
    id: mayastor-pool-2
    node: kworker1
    status: Online
    used: 2185232384
- id: mayastor-pool-3
  state:
    capacity: 5360320512
    disks:
      - "aio:///dev/vdb?uuid=f324edb7-1aca-41ec-954a-9614527f77e1"
    id: mayastor-pool-3
    node: kworker2
    status: Online
    used: 3258974208
```
8. Replica topology for a specific volume
```
❯ kubectl mayastor get volume-replica-topology ec4e66fd-3b33-4439-b504-d49aba53da26
 ID                                    NODE      POOL              STATUS
 93b1e1e9-ffcd-4c56-971e-294a530ea5cd  ksnode-2  pool-on-ksnode-2  Online
 88d89a92-40cf-4147-97d4-09e64979f548  ksnode-3  pool-on-ksnode-3  Online
```
</details>

<details>
<summary> Scale Resources operations </summary>

1. Scale Volume by ID
```
❯ kubectl mayastor scale volume 0c08667c-8b59-4d11-9192-b54e27e0ce0f 5
Volume 0c08667c-8b59-4d11-9192-b54e27e0ce0f Scaled Successfully 🚀

```
</details>

<details>
<summary> Support operations </summary>

```sh
kubectl-mayastor-dump
Supportability tool collects state & log information of services and dumps it to a tar file

USAGE:
    kubectl-mayastor dump [OPTIONS] <SUBCOMMAND>

OPTIONS:
    -d, --output-directory-path <OUTPUT_DIRECTORY_PATH>
            Output directory path to store archive file [default: ./]

    -e, --etcd-endpoint <ETCD_ENDPOINT>
            Endpoint of ETCD service, if left empty then will be parsed from the internal service
            name

    -h, --help
            Print help information

    -j, --jaeger <JAEGER>
            Trace rest requests to the Jaeger endpoint agent

    -k, --kube-config-path <KUBE_CONFIG_PATH>
            Path to kubeconfig file

    -l, --loki-endpoint <LOKI_ENDPOINT>
            Endpoint of LOKI service, if left empty then it will try to parse endpoint from Loki
            service(K8s service resource), if the tool is unable to parse from service then logs
            will be collected using Kube-apiserver

    -n, --namespace <NAMESPACE>
            Kubernetes namespace of mayastor service [default: mayastor]

    -o, --output <OUTPUT>
            The Output, viz yaml, json [default: none]

    -r, --rest <REST>
            The rest endpoint to connect to

    -s, --since <SINCE>
            Period states to collect all logs from last specified duration [default: 24h]

    -t, --timeout <TIMEOUT>
            Specifies the timeout value to interact with other modules of system [default: 10s]

SUBCOMMANDS:
    help       Print this message or the help of the given subcommand(s)
    node       Collects information about particular node matching to given node ID
    nodes      Collects information about all nodes
    pool       Collects information about particular pool and its descendants matching to given
                   pool ID
    pools      Collects information about all pools and its descendants (nodes)
    system     Collects entire system information
    volume     Collects information about particular volume and its descendants matching to
                   given volume ID
    volumes    Collects information about all volumes and its descendants (replicas/pools/nodes)
```

**Note**: Each subcommand supports `--help` option to know various other options.


**Examples**:

1. To collect entire mayastor system information into an archive file
   ```sh
   ## Command
   kubectl mayastor dump system -o <output_directory> -n <mayastor_namespace>
   ```

    - Example command while running inside Kubernetes cluster nodes
      ```sh
      kubectl mayastor dump system -o /mayastor-dump -n mayastor
      ```
    - Example command while running outside of Kubernetes cluster nodes
      ```sh
      kubectl mayastor dump system -o /mayastor-dump -l http://127.0.0.1:3100 -e http://127.0.0.1:2379 -n mayastor
      ```

2. To collect information about all mayastor volumes into an archive file
   ```sh
   ## Command
   kubectl mayastor dump volumes -o <output_directory> -n <mayastor_namespace>
   ```

    - Example command while running inside Kubernetes cluster nodes
      ```sh
      kubectl mayastor dump volumes -o /mayastor-dump -n mayastor
      ```
    - Example command while running outside of Kubernetes cluster nodes
      ```sh
      kubectl mayastor dump volumes -o /mayastor-dump -l http://127.0.0.1:3100 -e http://127.0.0.1:2379 -n mayastor
      ```

   **Note**: similarly to dump pools/nodes information then replace `volumes` with an associated resource type(`pools/nodes`).

3. To collect information about particular volume into an archive file
   ```sh
   ## Command
   kubectl mayastor dump volume <volume_name> -o <output_directory> -n <mayastor_namespace>
   ```

    - Example command while running inside Kubernetes cluster nodes
      ```sh
      kubectl mayastor dump volume volume-1 -o /mayastor-dump -n mayastor
      ```
    - Example command while running outside of Kubernetes cluster nodes
      ```sh
      kubectl mayastor dump volume volume-1 -o /mayastor-dump -l http://127.0.0.1:3100 -e http://127.0.0.1:2379 -n mayastor
      ```

**Note**: As of now endpoint(s) of Loki service & mayastor-etcd service is mandatory
only if tool is running outside of cluster nodes.

### Tip
To run `kubectl mayastor` command line tool outside Kubernetes cluster then it is recommended to
make Loki and etcd service available outside the cluster. One way to access applications running inside the cluster is by using [kubectl port-forward](https://kubernetes.io/docs/tasks/access-application-cluster/port-forward-access-application-cluster/)
- Command to forward traffic from local system to Loki service inside the cluster
  ```sh
  kubectl port-forward service/mayastor-loki 3100:3100 -n mayastor
  ```

- Command to forward traffic from local system to etcd service inside the cluster
  ```sh
  kubectl port-forward service/mayastor-etcd 2379:2379 -n mayastor
  ```
</details>