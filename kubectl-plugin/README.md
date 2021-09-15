# Mayastor kubectl Plugin

## Overview
The Mayastor kubectl plugin has been created in accordance with the instructions outlined in the [official documentation](https://kubernetes.io/docs/tasks/extend-kubectl/kubectl-plugins/).


The name of the plugin binary dictates how it is used. From the documentation:
> For example, a plugin named `kubectl-foo` provides a command `kubectl foo`.

In our case the name of the binary is specified in the Cargo.toml file as `kubectl-mayastor`, therefore the command is `kubectl mayastor`.

## Usage
**The plugin must be placed in your `PATH` in order for it to be used.**

To make the plugin as intuitive as possible, every attempt has been made to make the usage as similar to that of the standard `kubectl` command line utility as possible.

The general command structure is `kubectl mayastor <operation> <resource>` where the operation defines what should be performed (i.e. `get`, `scale`) and the resource defines what the operation should be performed on (i.e. `volumes`, `pools`).

The plugin needs to be able to connect to the REST server in order to make the appropriate REST calls. The IP address and port number of the REST server can be provided through the use of the `--rest` command line argument. If the `--rest` argument is omitted, the plugin will attempt to make use of the kubeconfig file to determine the IP of the master node of the cluster. Should the kubeconfig file contain multiple clusters, then the first cluster will be selected.


### Examples and Outputs

1. Get Volumes
```
❯ kubectl mayastor get volumes
 ID                                    REPLICAS  PROTOCOL  STATUS  SIZE
 18e30e83-b106-4e0d-9fb6-2b04e761e18a  4         none      Online  10485761
 0c08667c-8b59-4d11-9192-b54e27e0ce0f  4         none      Online  10485761

```
2. Get Volume by ID
```
❯ kubectl mayastor get volume 18e30e83-b106-4e0d-9fb6-2b04e761e18a
 ID                                    REPLICAS  PROTOCOL  STATUS  SIZE
 18e30e83-b106-4e0d-9fb6-2b04e761e18a  4         none      Online  10485761

```
3. Get Pools
```
❯ kubectl mayastor get pools
 ID               TOTAL CAPACITY  USED CAPACITY  DISKS                                                     NODE      STATUS
 mayastor-pool-1  5360320512      1111490560     aio:///dev/vdb?uuid=d8a36b4b-0435-4fee-bf76-f2aef980b833  kworker1  Online
 mayastor-pool-2  5360320512      2172649472     aio:///dev/vdc?uuid=bb12ec7d-8fc3-4644-82cd-dee5b63fc8c5  kworker1  Online
 mayastor-pool-3  5360320512      3258974208     aio:///dev/vdb?uuid=f324edb7-1aca-41ec-954a-9614527f77e1  kworker2  Online
```
4. Get Pool by ID
```
❯ kubectl mayastor get pool mayastor-pool-1
 ID               TOTAL CAPACITY  USED CAPACITY  DISKS                                                     NODE      STATUS
 mayastor-pool-1  5360320512      1111490560     aio:///dev/vdb?uuid=d8a36b4b-0435-4fee-bf76-f2aef980b833  kworker1  Online
```
5. Scale Volume by ID
```
❯ kubectl mayastor scale volume 0c08667c-8b59-4d11-9192-b54e27e0ce0f 5
Volume 0c08667c-8b59-4d11-9192-b54e27e0ce0f Scaled Successfully 🚀

```
6. Get Volume(s)/Pool(s) to a specific Output Format
```
❯ kubectl mayastor -ojson get volumes
[{"spec":{"labels":[],"num_paths":1,"num_replicas":4,"protocol":"none","size":10485761,"status":"Created","uuid":"18e30e83-b106-4e0d-9fb6-2b04e761e18a"},"state":{"children":[],"protocol":"none","size":10485761,"status":"Online","uuid":"18e30e83-b106-4e0d-9fb6-2b04e761e18a"}},{"spec":{"labels":[],"num_paths":1,"num_replicas":5,"protocol":"none","size":10485761,"status":"Created","uuid":"0c08667c-8b59-4d11-9192-b54e27e0ce0f"},"state":{"children":[],"protocol":"none","size":10485761,"status":"Online","uuid":"0c08667c-8b59-4d11-9192-b54e27e0ce0f"}}]

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