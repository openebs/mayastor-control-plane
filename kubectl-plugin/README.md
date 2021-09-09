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

### Examples

#### Volumes
Getting all volumes:\
`kubectl mayastor get volumes`

Getting a specific volume:\
`kubectl mayastor get volume ec4e66fd-3b33-4439-b504-d49aba53da26`

Scaling a specific volume:\
`kubectl mayastor scale volume ec4e66fd-3b33-4439-b504-d49aba53da26 2`

#### Pools

Getting all pools:\
`kubectl mayastor get pools`

Getting a pool:\
`kubectl mayastor get pool 574ba4c9-fec6-441c-a4d0-d5f3fafe7078`