# Support Kubectl Plugin

## Overview
The supportability kubectl plugin has been created in accordance with the instructions outlined
in the [official documentation](https://kubernetes.io/docs/tasks/extend-kubectl/kubectl-plugins/). Plugin tool provides mechanism to create support bundles of current state.

The name of the plugin binary dictates how it is used. From the documentation:
> For example, a plugin named `kubectl-foo` provides a command `kubectl foo`.

In supportability case the name of the binary is specified in the Cargo.toml file as `kubectl-support`, therefore the command is `kubectl support`.

## Usage
**The plugin must be placed in your `PATH` in order for it to be used.**

To make the plugin as intuitive as possible, every attempt has been made to make the usage as similar to that of the standard `kubectl` command line utility as possible.

The general command structure is `kubectl support <operation> <resource>` where the operation defines what should be performed (i.e. `dump`) and the resource defines operation should be performed on (i.e. `system`).

```sh
kubectl support --help
cli 0.1.0
Types of operations supported by plugin

USAGE:
    kubectl-support [OPTIONS] <SUBCOMMAND>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -k, --kube-config-path <kube-config-path>              Path to kubeconfig file
    -l, --loki-endpoint <loki-endpoint>
            Endpoint of LOKI service, if left empty then logs will be collected from Kube-apiserver

    -n, --namespace <namespace>
            Kubernetes namespace of service, defaults to [default: mayastor]

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

1. Dump topological information of system into an archive file
```sh
 ## Command
 kubectl support dump system -o <output_directory> -r <rest_endpoint> -l <loki endpoint> -n <namespace>
```
 Example command
 ```sh
  ## Sample
  kubectl support dump system -o /dump -r http://192.167.0.5:30011 -l http://192.167.0.5:3100 -n mayastor
 ```

**Note**: As of now endpoint of Loki service is mandatory only if support tool needs to collect
          historic information. Command to get Loki endpoint
          ```sh
          kubectl get svc -n mayastor -l app=loki -o jsonpath='{range .items[0]}{.spec.clusterIP}:{.spec.ports[0].port}'
          ```

