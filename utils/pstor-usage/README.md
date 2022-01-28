# Persistent Storage Usage

This `pstore-usage` is used to sample persistent store (ETCD) usage at runtime from a live cluster.
By default, it makes use of the `deployer` library to create a local cluster running on docker.

## Examples

**Using the help**

```textmate
❯ cargo run -q --bin pstor-usage -- --help
pstor-usage version 0.1.0, git hash 756e0175b34e

USAGE:
    pstor-usage [FLAGS] [OPTIONS]

FLAGS:
    -h, --help               Prints help information
        --pool-use-malloc    Use ram based pools instead of files (useful for debugging with small pool allocation).
                             When using files the /tmp/pool directory will be used
        --no-total-stats     Skip the output of the total storage usage after allocation of all resources and also after
                             those resources have been deleted
    -V, --version            Prints version information

OPTIONS:
        --pool-samples <pool-samples>          Number of pool samples [default: 5]
        --pool-size <pool-size>                Size of the pools [default: 20MiB]
    -p, --pools <pools>                        Number of pools per sample [default: 10]
    -r, --rest-url <rest-url>                  The rest endpoint if reusing a cluster
        --vol-samples <vol-samples>            Number of volume samples [default: 10]
        --volume-mods <volume-mods>            Modifies `N` volumes from each volume samples. In other words, we will
                                               publish/unpublish each `N` volumes from each list of samples. Please note
                                               that this can take quite some time; it's very slow to create nexuses with
                                               remote replicas [default: 2]
        --volume-replicas <volume-replicas>    Number of volume replicas [default: 3]
        --volume-size <volume-size>            Size of the volumes [default: 5MiB]
    -v, --volumes <volumes>                    Number of volumes per sample [default: 20]
```

**Sampling the persistent storage usage**

```textmate
❯ cargo run -q --bin pstor-usage
pstor-usage version 0.1.0, git hash 756e0175b34e
┌───────────────┬────────────┐
│ Volumes ~Repl │ Disk Usage │
├───────────────┼────────────┤
│     20 ~3     │   92 KiB   │
├───────────────┼────────────┤
│     40 ~3     │  172 KiB   │
├───────────────┼────────────┤
│     60 ~3     │  252 KiB   │
├───────────────┼────────────┤
│     80 ~3     │  332 KiB   │
├───────────────┼────────────┤
│    100 ~3     │  412 KiB   │
├───────────────┼────────────┤
│    120 ~3     │  488 KiB   │
├───────────────┼────────────┤
│    140 ~3     │  580 KiB   │
├───────────────┼────────────┤
│    160 ~3     │  664 KiB   │
├───────────────┼────────────┤
│    180 ~3     │  740 KiB   │
├───────────────┼────────────┤
│    200 ~3     │  824 KiB   │
└───────────────┴────────────┘
┌───────┬────────────┐
│ Pools │ Disk Usage │
├───────┼────────────┤
│  10   │   8 KiB    │
├───────┼────────────┤
│  20   │   16 KiB   │
├───────┼────────────┤
│  30   │   28 KiB   │
├───────┼────────────┤
│  40   │   36 KiB   │
├───────┼────────────┤
│  50   │   44 KiB   │
└───────┴────────────┘
┌───────────────┬───────┬────────────┐
│ Volumes ~Repl │ Pools │ Disk Usage │
├───────────────┼───────┼────────────┤
│     20 ~3     │  10   │  100 KiB   │
├───────────────┼───────┼────────────┤
│     40 ~3     │  20   │  188 KiB   │
├───────────────┼───────┼────────────┤
│     60 ~3     │  30   │  280 KiB   │
├───────────────┼───────┼────────────┤
│     80 ~3     │  40   │  368 KiB   │
├───────────────┼───────┼────────────┤
│    100 ~3     │  50   │  456 KiB   │
├───────────────┼───────┼────────────┤
│    120 ~3     │  50   │  532 KiB   │
├───────────────┼───────┼────────────┤
│    140 ~3     │  50   │  624 KiB   │
├───────────────┼───────┼────────────┤
│    160 ~3     │  50   │  708 KiB   │
├───────────────┼───────┼────────────┤
│    180 ~3     │  50   │  784 KiB   │
├───────────────┼───────┼────────────┤
│    200 ~3     │  50   │  868 KiB   │
└───────────────┴───────┴────────────┘
┌──────────────────┬────────────┐
│ Volume~Repl Mods │ Disk Usage │
├──────────────────┼────────────┤
│       2~3        │   20 KiB   │
├──────────────────┼────────────┤
│       4~3        │   44 KiB   │
├──────────────────┼────────────┤
│       6~3        │   68 KiB   │
├──────────────────┼────────────┤
│       8~3        │   96 KiB   │
├──────────────────┼────────────┤
│       10~3       │  120 KiB   │
├──────────────────┼────────────┤
│       12~3       │  144 KiB   │
├──────────────────┼────────────┤
│       14~3       │  168 KiB   │
├──────────────────┼────────────┤
│       16~3       │  192 KiB   │
├──────────────────┼────────────┤
│       18~3       │  216 KiB   │
├──────────────────┼────────────┤
│       20~3       │  240 KiB   │
└──────────────────┴────────────┘
┌──────────┬──────────────┬─────────┬───────────────┬───────────────┐
│ Creation │ Modification │ Cleanup │ Total         │ Current       │
├──────────┼──────────────┼─────────┼───────────────┼───────────────┤
│ 868 KiB  │ 240 KiB      │ 532 KiB │ 1 MiB 616 KiB │ 1 MiB 636 KiB │
└──────────┴──────────────┴─────────┴───────────────┴───────────────┘
```

***Sampling only single replica volumes:***

```textmate
❯ cargo run -q --bin pstor-usage -- --pools 0 --volume-replicas 1 --volume-mods 0 --no-total-stats
┌───────────────┬────────────┐
│ Volumes ~Repl │ Disk Usage │
├───────────────┼────────────┤
│     20 ~1     │   40 KiB   │
├───────────────┼────────────┤
│     40 ~1     │   84 KiB   │
├───────────────┼────────────┤
│     60 ~1     │  116 KiB   │
├───────────────┼────────────┤
│     80 ~1     │  160 KiB   │
├───────────────┼────────────┤
│    100 ~1     │  200 KiB   │
├───────────────┼────────────┤
│    120 ~1     │  236 KiB   │
├───────────────┼────────────┤
│    140 ~1     │  276 KiB   │
├───────────────┼────────────┤
│    160 ~1     │  324 KiB   │
├───────────────┼────────────┤
│    180 ~1     │  360 KiB   │
├───────────────┼────────────┤
│    200 ~1     │  408 KiB   │
└───────────────┴────────────┘
```
