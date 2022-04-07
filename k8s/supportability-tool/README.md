# Kubectl Plugin

## Overview
The supportability kubectl plugin has been created in accordance with the instructions outlined
in the [official documentation](https://kubernetes.io/docs/tasks/extend-kubectl/kubectl-plugins/). Plugin tool provides mechanism to create support bundles of the current state.

The name of the plugin binary dictates how it is used. From the documentation:
> For example, a plugin named `kubectl-foo` provides a command `kubectl foo`.

In mayastor supportability case the name of the binary is specified in the Cargo.toml file as `kubectl-mayastor_support`, therefore the command is `kubectl mayastor_support`.

## Usage
**The plugin must be placed in your `PATH` in order for it to be used.**

To make the plugin as intuitive as possible, every attempt has been made to make the usage as similar to that of the standard `kubectl` command line utility as possible.

The general command structure is `kubectl mayastor_support <operation> <resource>` where the operation defines what should be performed (i.e. `dump`) and the resource defines operation should be performed on (i.e. `system`).

```sh
mayastor-cli 0.1.0
Types of operations supported by plugin

USAGE:
    kubectl-mayastor_support [OPTIONS] <SUBCOMMAND>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -e, --etcd-endpoint <etcd-endpoint>
            Endpoint of ETCD service, if left empty then will be parsed from the internal service name

    -k, --kube-config-path <kube-config-path>              Path to kubeconfig file
    -l, --loki-endpoint <loki-endpoint>
            Endpoint of LOKI service, if left empty then it will try to parse endpoint from Loki service(K8s service
            resource), if the tool is unable to parse from service then logs will be collected using Kube-apiserver
    -n, --namespace <namespace>
            Kubernetes namespace of mayastor service, defaults to mayastor [default: mayastor]

    -o, --output-directory-path <output-directory-path>    Output directory path to store archive file [default: ./]
    -r, --rest <rest>                                      The rest endpoint, parsed from KUBECONFIG, if left empty
    -s, --since <since>
            Period states to collect all logs from last specified duration [default: 24h]

    -t, --timeout <timeout>
            Specifies the timeout value to interact with other modules of system [default: 10s]


SUBCOMMANDS:
    dump    'Dump' creates an archive by collecting provided resource(s) information
    help    Prints this message or the help of the given subcommand(s)
```

**Note**: Each subcommand supports `--help` option to know various other options.


**Examples**:

1. To collect entire mayastor system information into an archive file
   ```sh
   ## Command
   kubectl mayastor_support dump system -o <output_directory> -n <mayastor_namespace>
   ```

   - Example command while running inside Kubernetes cluster nodes
     ```sh
     kubectl mayastor_support dump system -o /mayastor-dump -n mayastor
     ```
   - Example command while running outside of Kubernetes cluster nodes
     ```sh
     kubectl mayastor_support dump system -o /mayastor-dump -l http://127.0.0.1:3100 -e http://127.0.0.1:2379 -n mayastor
     ```

2. To collect information about all mayastor volumes into an archive file
   ```sh
   ## Command
   kubectl mayastor_support dump volumes -o <output_directory> -n <mayastor_namespace>
   ```

   - Example command while running inside Kubernetes cluster nodes
     ```sh
     kubectl mayastor_support dump volumes -o /mayastor-dump -n mayastor
     ```
   - Example command while running outside of Kubernetes cluster nodes
     ```sh
     kubectl mayastor_support dump volumes -o /mayastor-dump -l http://127.0.0.1:3100 -e http://127.0.0.1:2379 -n mayastor
     ```

    **Note**: similarly to dump pools/nodes information then replace `volumes` with an associated resource type(`pools/nodes`).

3. To collect information about particular volume into an archive file
   ```sh
   ## Command
   kubectl mayastor_support dump volume <volume_name> -o <output_directory> -n <mayastor_namespace>
   ```

   - Example command while running inside Kubernetes cluster nodes
     ```sh
     kubectl mayastor_support dump volume volume-1 -o /mayastor-dump -n mayastor
     ```
   - Example command while running outside of Kubernetes cluster nodes
     ```sh
     kubectl mayastor_support dump volume volume-1 -o /mayastor-dump -l http://127.0.0.1:3100 -e http://127.0.0.1:2379 -n mayastor
     ```

**Note**: As of now endpoint(s) of Loki service & mayastor-etcd service is mandatory
          only if tool is running outside of cluster nodes.

### Tip
To run `kubectl mayastor_support` command line tool outside Kubernetes cluster then it is recommended to
make Loki and etcd service available outside the cluster. One way to access applications running inside the cluster is by using [kubectl port-forward](https://kubernetes.io/docs/tasks/access-application-cluster/port-forward-access-application-cluster/)
- Command to forward traffic from local system to Loki service inside the cluster
  ```sh
  kubectl port-forward service/mayastor-loki 3100:3100 -n mayastor
  ```

- Command to forward traffic from local system to etcd service inside the cluster
  ```sh
  kubectl port-forward service/mayastor-etcd 2379:2379 -n mayastor
  ```
