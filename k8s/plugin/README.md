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
 ID          GRPC ENDPOINT   STATUS   CORDONED
 mayastor-2  10.1.0.7:10124  Online   false
 mayastor-1  10.1.0.6:10124  Online   false
 mayastor-3  10.1.0.8:10124  Online   false
```
6. Get Node by ID
```
❯ kubectl mayastor get node mayastor-2
 ID          GRPC ENDPOINT   STATUS  CORDONED
 mayastor-2  10.1.0.7:10124  Online  false
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

9. Get BlockDevices by NodeID
```
❯ kubectl mayastor get block-devices worker-node1 --all
 DEVNAME          DEVTYPE    SIZE       AVAILABLE  MODEL                       DEVPATH                                                         FSTYPE  FSUUID  MOUNTPOINT  PARTTYPE                              MAJOR            MINOR                                     DEVLINKS
 /dev/nvme1n1     disk       4194304    no         Amazon Elastic Block Store  /devices/pci0000:00/0000:00:1f.0/nvme/nvme1/nvme1n1             259     4       ext4        4616cd08-7a7d-49fe-ae6d-908f9e864fc7                                                             "/dev/disk/by-uuid/4616cd08-7a7d-49fe-ae6d-908f9e864fc7", "/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_vol04bfba0a58c4ffdae", "/dev/disk/by-id/nvme-nvme.1d0f-766f6c303462666261306135386334666664
 /dev/nvme4n1     disk       20971520   yes        Amazon Elastic Block Store  /devices/pci0000:00/0000:00:1d.0/nvme/nvme4/nvme4n1             259     12                                                                                                                   "/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_vol06eb486c9593587a9", "/dev/disk/by-id/nvme-nvme.1d0f-766f6c3036656234383663393539333538376139-416d617a6f6e20456c617374696320426c6f636b2053746f7265-00000001", "/dev/disk/by-path/pci-0000:00:1d.0-nvme-1"
 /dev/nvme2n1     disk       104857600  no         Amazon Elastic Block Store  /devices/pci0000:00/0000:00:1e.0/nvme/nvme2/nvme2n1             259     5                                                                                                                    "/dev/disk/by-id/nvme-nvme.1d0f-766f6c3033623636623930363535636636656465-416d617a6f6e20456c617374696320426c6f636b2053746f7265-00000001", "/dev/disk/by-path/pci-0000:00:1e.0-nvme-1", "/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_vol03b66b90655cf6ede"
```
```
❯ kubectl mayastor get block-devices worker-node1
 DEVNAME       DEVTYPE  SIZE      AVAILABLE  MODEL                       DEVPATH                                              MAJOR  MINOR  DEVLINKS
 /dev/nvme4n1  disk     20971520  yes        Amazon Elastic Block Store  /devices/pci0000:00/0000:00:1d.0/nvme/nvme4/nvme4n1  259    12     "/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_vol06eb486c9593587a9", "/dev/disk/by-id/nvme-nvme.1d0f-766f6c3036656234383663393539333538376139-416d617a6f6e20456c617374696320426c6f636b2053746f7265-00000001", "/dev/disk/by-path/pci-0000:00:1d.0-nvme-1"
```
**NOTE: The above command lists usable blockdevices if `--all` flag is not used, but currently since there isn't a way to identify whether the `disk` has a blobstore pool, `disks` not used by `pools` created by `control-plane` are shown as usable if they lack any filesystem uuid.**

</details>

<details>
<summary> Node Cordon And Drain Operations </summary>

1. Node Cordoning
```
❯ kubectl mayastor cordon node node-1-14048 my_cordon_1
Node node-1-14048 cordoned successfully
```
2. Node Uncordoning
```
❯ kubectl mayastor uncordon node node-1-14048 my_cordon_1
Node node-1-14048 successfully uncordoned
```
3. Get Cordon
```
❯ kubectl mayastor get cordon node node-1-14048
 ID            GRPC ENDPOINT        STATUS  CORDONED  CORDON LABELS
 node-1-14048  95.217.158.66:10124  Online  true      my_cordon_1

❯ kubectl mayastor get cordon nodes
 ID            GRPC ENDPOINT        STATUS  CORDONED  CORDON LABELS
 node-2-14048  95.217.152.7:10124   Online  true      my_cordon_2
 node-1-14048  95.217.158.66:10124  Online  true      my_cordon_1
```
4. Node Draining
```
❯ kubectl mayastor drain node io-engine-1 my-drain-label
Node node-1-14048 successfully drained

❯ kubectl mayastor drain node node-1-14048 my-drain-label --drain-timeout 10s
Node node-1-14048 drain command timed out
```
5. Cancel Node Drain (via uncordon)
```
❯ kubectl mayastor uncordon node io-engine-1 my-drain-label
Node io-engine-1 successfully uncordoned
```
6. Get Drain
```
❯ kubectl mayastor get drain node node-2-14048
 ID            GRPC ENDPOINT       STATUS  CORDONED  DRAIN STATE  DRAIN LABELS
 node-2-14048  95.217.152.7:10124  Online  true      Draining     my_drain_2

❯ kubectl-plugin/bin/kubectl-mayastor get drain node node-0-14048
 ID            GRPC ENDPOINT          STATUS  CORDONED  DRAIN STATE   DRAIN LABELS
 node-0-14048  135.181.206.173:10124  Online  false     Not draining

❯ kubectl mayastor get drain nodes
 ID            GRPC ENDPOINT        STATUS  CORDONED  DRAIN STATE  DRAIN LABELS
 node-2-14048  95.217.152.7:10124   Online  true      Draining     my_drain_2
 node-1-14048  95.217.158.66:10124  Online  true      Drained      my_drain_1

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
   kubectl mayastor dump system -d <output_directory> -n <mayastor_namespace>
   ```

    - Example command while running inside Kubernetes cluster nodes / system which
      has access to cluster node ports
      ```sh
      kubectl mayastor dump system -d /mayastor-dump -n mayastor
      ```
    - Example command while running outside of Kubernetes cluster nodes where
      nodes exist in private network (or) node ports are not exposed for outside cluster
      ```sh
      kubectl mayastor dump system -d /mayastor-dump -r http://127.0.0.1:30011 -l http://127.0.0.1:3100 -e http://127.0.0.1:2379 -n mayastor
      ```

2. To collect information about all mayastor volumes into an archive file
   ```sh
   ## Command
   kubectl mayastor dump volumes -d <output_directory> -n <mayastor_namespace>
   ```

    - Example command while running inside Kubernetes cluster nodes / system which
      has access to cluster node ports
      ```sh
      kubectl mayastor dump volumes -d /mayastor-dump -n mayastor
      ```
    - Example command while running outside of Kubernetes cluster nodes where
      nodes exist in private network (or) node ports are not exposed for outside cluster
      ```sh
      kubectl mayastor dump volumes -d /mayastor-dump -r http://127.0.0.1:30011 -l http://127.0.0.1:3100 -e http://127.0.0.1:2379 -n mayastor
      ```

   **Note**: similarly to dump pools/nodes information then replace `volumes` with an associated resource type(`pools/nodes`).

3. To collect information about particular volume into an archive file
   ```sh
   ## Command
   kubectl mayastor dump volume <volume_name> -d <output_directory> -n <mayastor_namespace>
   ```

    - Example command while running inside Kubernetes cluster nodes / system which
      has access to cluster node ports
      ```sh
      kubectl mayastor dump volume volume-1 -d /mayastor-dump -n mayastor
      ```
    - Example command while running outside of Kubernetes cluster nodes where
      nodes exist in private network (or) node ports are not exposed for outside cluster
      ```sh
      kubectl mayastor dump volume volume-1 -d /mayastor-dump -r http://127.0.0.1:30011 -l http://127.0.0.1:3100 -e http://127.0.0.1:2379 -n mayastor
      ```

**Note**: As of now endpoint(s) of Rest, Loki & mayastor-etcd services are mandatory
only if tool is running outside of cluster nodes(where nodes are in private
network/node ports are not exposed outside cluster).

### Tip
To run `kubectl mayastor` command line tool outside Kubernetes cluster then it is recommended to
make Rest, Loki and etcd service available outside the cluster. One way to access applications running inside the cluster is by using [kubectl port-forward](https://kubernetes.io/docs/tasks/access-application-cluster/port-forward-access-application-cluster/)
- Command to forward traffic from local system to Rest service inside the
    cluster
  ```sh
  kubectl port-forward service/rest 8081:8081 -n mayastor
  ```
- Command to forward traffic from local system to Loki service inside the cluster
  ```sh
  kubectl port-forward service/mayastor-loki 3100:3100 -n mayastor
  ```

- Command to forward traffic from local system to etcd service inside the cluster
  ```sh
  kubectl port-forward service/mayastor-etcd 2379:2379 -n mayastor
  ```
</details>
