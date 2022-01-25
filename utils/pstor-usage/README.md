# Persistent Storage Usage

This `pstore-usage` is used to sample persistent store (ETCD) usage at runtime from a live cluster.
By default, it makes use of the `deployer` library to create a local cluster running on docker.

## Examples

**Using the help**

```textmate
❯ cargo run -q --bin pstor-usage -- --help
pstor-usage version 0.1.0, git hash 4f40cf4681f7

USAGE:
    pstor-usage [FLAGS] [OPTIONS]

FLAGS:
    -h, --help               Prints help information
        --no-total-stats     Skip the output of the total storage usage after allocation of all resources and also after
                             those resources have been deleted
        --pool-use-malloc    Use ram based pools instead of files (useful for debugging with small pool allocation).
                             When using files the /tmp/pool directory will be used
    -V, --version            Prints version information

OPTIONS:
        --pool-samples <pool-samples>          Number of pool samples [default: 5]
        --pool-size <pool-size>                Size of the pools [default: 20MiB]
    -p, --pools <pools>                        Number of pools per sample [default: 10]
    -r, --rest-url <rest-url>                  The rest endpoint if reusing a cluster
        --vol-samples <vol-samples>            Number of volume samples [default: 10]
        --volume-replicas <volume-replicas>    Number of volume replicas [default: 3]
        --volume-size <volume-size>            Size of the volumes [default: 1MiB]
    -v, --volumes <volumes>                    Number of volumes per sample [default: 20]
```

**Sampling the persistent storage usage**

```textmate
❯ cargo run -q --bin pstor-usage
pstor-usage version 0.1.0, git hash 4f40cf4681f7
┌───────────────┬────────────┐
│ Volumes ~Repl │ Disk Usage │
├───────────────┼────────────┤
│     20 ~3     │   92 KiB   │
├───────────────┼────────────┤
│     40 ~3     │  172 KiB   │
├───────────────┼────────────┤
│     60 ~3     │  248 KiB   │
├───────────────┼────────────┤
│     80 ~3     │  332 KiB   │
├───────────────┼────────────┤
│    100 ~3     │  408 KiB   │
├───────────────┼────────────┤
│    120 ~3     │  492 KiB   │
├───────────────┼────────────┤
│    140 ~3     │  584 KiB   │
├───────────────┼────────────┤
│    160 ~3     │  664 KiB   │
├───────────────┼────────────┤
│    180 ~3     │  744 KiB   │
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
│  30   │   24 KiB   │
├───────┼────────────┤
│  40   │   32 KiB   │
├───────┼────────────┤
│  50   │   40 KiB   │
└───────┴────────────┘
┌───────────────┬───────┬────────────┐
│ Volumes ~Repl │ Pools │ Disk Usage │
├───────────────┼───────┼────────────┤
│     20 ~3     │  10   │  100 KiB   │
├───────────────┼───────┼────────────┤
│     40 ~3     │  20   │  188 KiB   │
├───────────────┼───────┼────────────┤
│     60 ~3     │  30   │  272 KiB   │
├───────────────┼───────┼────────────┤
│     80 ~3     │  40   │  364 KiB   │
├───────────────┼───────┼────────────┤
│    100 ~3     │  50   │  448 KiB   │
├───────────────┼───────┼────────────┤
│    120 ~3     │  50   │  532 KiB   │
├───────────────┼───────┼────────────┤
│    140 ~3     │  50   │  624 KiB   │
├───────────────┼───────┼────────────┤
│    160 ~3     │  50   │  704 KiB   │
├───────────────┼───────┼────────────┤
│    180 ~3     │  50   │  784 KiB   │
├───────────────┼───────┼────────────┤
│    200 ~3     │  50   │  864 KiB   │
└───────────────┴───────┴────────────┘
┌────────────────┬───────────────┐
│ After Creation │ After Cleanup │
├────────────────┼───────────────┤
│ 864 KiB        │ 1 MiB 356 KiB │
└────────────────┴───────────────┘
```

***Sampling only single replica volumes:***

```textmate
❯ cargo run -q --bin pstor-usage -- --pools 0 --volume-replicas 1 --no-total-stats
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
