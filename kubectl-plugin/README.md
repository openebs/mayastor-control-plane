# Mayastor kubectl Plugin

## Overview
The Mayastor kubectl plugin has been created in accordance with the instructions outlined in the [official documentation](https://kubernetes.io/docs/tasks/extend-kubectl/kubectl-plugins/).


The name of the plugin binary dictates how it is used. From the documentation:
> For example, a plugin named `kubectl-foo` provides a command `kubectl foo`.

In our case the name of the binary is specified in the Cargo.toml file as `kubectl-mayastor`, therefore the command is `kubectl mayastor`.

## Usage
**The plugin must be placed in your `PATH` in order for it to be used.**

To make the plugin as intuitive as possible, every attempt has been made to make the usage as similar to that of the standard `kubectl` command line utility as possible.

The general command structure is `kubectl mayastor --rest <rest_endpoint> <operation> <resource>` where the operation defines what should be performed (i.e. `list`, `get`) and the resource defines what the operation should be performed on (i.e. `volumes`, `pools`).

### Examples

#### Volumes
Listing volumes:\
`kubectl mayastor --rest http://192.168.122.142:30011 list volumes`

Getting a volume:\
`kubectl mayastor --rest http://192.168.122.142:30011 get volume ec4e66fd-3b33-4439-b504-d49aba53da26`

Scaling a volume:\
`kubectl mayastor --rest http://192.168.122.142:30011 scale volume ec4e66fd-3b33-4439-b504-d49aba53da26 2`

#### Pools

Listing pools:\
`kubectl mayastor --rest http://192.168.122.142:30011 list pools`

Getting a pool:\
`kubectl mayastor --rest http://192.168.122.142:30011 get pool 574ba4c9-fec6-441c-a4d0-d5f3fafe7078`