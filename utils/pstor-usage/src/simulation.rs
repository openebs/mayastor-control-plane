use crate::{
    etcd::{Etcd, EtcdSampler},
    pools::PoolMgr,
    printer::{PrettyPrinter, Printer, TabledData},
    resources::{ResourceDelete, ResourceMgr, ResourceSamples, Sampler},
    volumes::VolMgr,
};
use openapi::{
    apis::Url,
    clients::tower::{direct::ApiClient, Configuration},
};

use anyhow::anyhow;
use std::time::Duration;
use structopt::StructOpt;

/// Simulate how much storage a cluster would require based on some parameters.
#[derive(StructOpt, Debug)]
pub(crate) struct Simulation {
    #[structopt(flatten)]
    opts: SimulationOpts,

    /// Skip the output of the total storage usage after allocation of all resources and also after
    /// those resources have been deleted.
    #[structopt(long = "no-total-stats", parse(from_flag = std::ops::Not::not))]
    total_stats: bool,

    #[structopt(skip)]
    print_samples: bool,
}

#[derive(StructOpt, Default, Debug, Clone)]
pub(crate) struct SimulationOpts {
    /// Size of the pools.
    #[structopt(long, parse(try_from_str = parse_size::parse_size), default_value = "20MiB")]
    pool_size: u64,

    /// Size of the volumes.
    #[structopt(long, parse(try_from_str = parse_size::parse_size), default_value = "5MiB")]
    volume_size: u64,

    /// Number of volume replicas.
    #[structopt(long, default_value = "3")]
    volume_replicas: u8,

    /// Number of pools per sample.
    #[structopt(short, long, default_value = "10")]
    pools: u32,

    /// Number of pool samples.
    #[structopt(long, default_value = "5")]
    pool_samples: u32,

    /// Number of volumes per sample.
    #[structopt(short, long, default_value = "20")]
    volumes: u32,

    /// Number of volume samples.
    #[structopt(long, default_value = "10")]
    volume_samples: u32,

    /// Modifies `N` volumes from each volume samples.
    /// In other words, we will publish/unpublish each `N` volumes from each list of samples.
    /// Please note that this can take quite some time; it's very slow to create
    /// nexuses with remote replicas.
    #[structopt(long, default_value = "2")]
    volume_mods: u32,

    /// Use ram based pools instead of files (useful for debugging with small pool allocation).
    /// When using files the /tmp/pool directory will be used.
    #[structopt(long)]
    pool_use_malloc: bool,
}

impl From<SimulationOpts> for Simulation {
    fn from(opts: SimulationOpts) -> Self {
        Self {
            total_stats: true,
            opts,
            print_samples: false,
        }
    }
}

impl Simulation {
    fn print<P, T>(&self, printer: &P, printable: &T)
    where
        P: Printer,
        T: TabledData<Row = P::Row>,
    {
        if self.print_samples {
            printer.print(printable);
        }
    }

    /// Sets whether the simulation prints its samples as it's simulating.
    pub(crate) fn set_print_samples(&mut self, skip: bool) {
        self.print_samples = skip;
    }

    /// Simulate with the current parameters.
    pub(crate) async fn simulate(
        &self,
        external_cluster: &Option<Url>,
    ) -> anyhow::Result<RunStats> {
        let args = &self.opts;

        let (client, _cluster) = match external_cluster {
            None => {
                // cluster will be terminated on drop
                let cluster = testlib::ClusterBuilder::builder()
                    .with_silence_test_traces()
                    .with_build(false)
                    .with_build_all(false)
                    .with_mayastors(args.volume_replicas.into())
                    .build()
                    .await
                    .map_err(|e| anyhow!("Failed to build cluster: {}", e))?;

                (cluster.rest_v00(), Some(cluster))
            }
            Some(rest_url) => {
                let openapi_client_config =
                    Configuration::new(rest_url.clone(), Duration::from_secs(5), None, None, true)
                        .map_err(|e| {
                            anyhow::anyhow!("Failed to create rest client config: '{:?}'", e)
                        })?;
                let client = ApiClient::new(openapi_client_config);
                (client, None)
            }
        };
        let cleanup = _cluster.is_none() || self.total_stats;

        let etcd = Etcd::new(Url::parse("http://0.0.0.0:2379")?).await?;

        // create some pools as backing for the volumes
        let four_mb = 4096 * 1024;
        let mut volume_size = args.volume_size;
        if (volume_size % four_mb) != 0 {
            volume_size += four_mb - (volume_size % four_mb);
        }
        let pools = if args.volumes > 0 && args.volume_samples > 0 {
            args.volume_replicas as u32
        } else {
            0
        };
        let pool_size = args.volume_samples as u64 * args.volumes as u64 * (volume_size + four_mb); // 4M for the metadata
        let pool_mgr = PoolMgr::new_mgr(&client, pool_size, args.pool_use_malloc).await?;
        let vol_pools = pool_mgr.create(&client, pools).await?;

        // capture the initial database size
        let initial = etcd.db_size().await?;
        let printer = PrettyPrinter::new();

        // sample how much space the volumes take up
        let vol_mgr = VolMgr::new_mgr(args.volume_replicas, args.volume_size).await?;
        let (volumes, vol_results) = EtcdSampler::new_sampler(etcd.clone(), args.volume_samples)
            .sample(&client, args.volumes, &vol_mgr)
            .await?;
        self.print(&printer, &vol_results);

        // sample how much space the pools take up
        let pool_mgr = PoolMgr::new_mgr(&client, args.pool_size, args.pool_use_malloc).await?;
        let (pools, mut pool_results) = EtcdSampler::new_sampler(etcd.clone(), args.pool_samples)
            .sample(&client, args.pools, &pool_mgr)
            .await?;
        self.print(&printer, &pool_results);

        // this is the combined pool and volume usage
        if args.pools > 0 && args.volumes > 0 {
            pool_results.extend(vol_results.into_inner());
            self.print(&printer, &pool_results);
        }

        // capture the database size after we've completed allocating new resources
        let after_alloc = etcd.db_size().await?;

        if args.volume_mods > 0 {
            let mod_results = EtcdSampler::new_sampler(etcd.clone(), args.volume_samples)
                .sample_mods(&client, args.volume_mods, &volumes)
                .await?;
            self.print(&printer, &mod_results);
        }

        // capture the database size after we've churned the volumes
        let after_mod = etcd.db_size().await?;

        // clean up created resources
        if cleanup {
            volumes.delete(&client).await?;
            pools.delete(&client).await?;
            vol_pools.delete(&client).await?;
        }

        // capture the database size after we've deleted the resources
        let after_cleanup = etcd.db_size().await?;

        let stats = RunStats::new(initial, after_alloc, after_mod, after_cleanup);

        if self.total_stats {
            self.print(&printer, &stats);
        }

        Ok(stats)
    }
    /// Total number of volumes allocated.
    pub(crate) fn volumes_allocated(&self) -> u64 {
        (self.opts.volumes * self.opts.volume_samples) as u64
    }
    /// Total number of volumes modified.
    pub(crate) fn volumes_modified(&self) -> u64 {
        (self.opts.volume_mods * self.opts.volume_samples) as u64
    }
}

/// Stats collected after running a simulation.
pub(crate) struct RunStats {
    allocation: u64,
    modification: u64,
    cleanup: u64,
    last: u64,
}
impl RunStats {
    fn new(initial: u64, after_alloc: u64, after_mod: u64, after_cleanup: u64) -> Self {
        Self {
            allocation: after_alloc - initial,
            modification: after_mod - after_alloc,
            cleanup: after_cleanup - after_mod,
            last: after_cleanup,
        }
    }
    /// How many bytes were used as a result of resource allocation
    pub(crate) fn allocation(&self) -> u64 {
        self.allocation
    }
    /// How many bytes were used as a result of resource modification
    pub(crate) fn modification(&self) -> u64 {
        self.modification
    }
    /// How many bytes were used as a result of resource deletion
    pub(crate) fn cleanup(&self) -> u64 {
        self.cleanup
    }
    /// Total number of bytes
    fn total(&self) -> u64 {
        self.allocation + self.modification + self.cleanup
    }
}
impl TabledData for RunStats {
    type Row = prettytable::Row;

    fn titles(&self) -> Self::Row {
        prettytable::Row::new(vec![
            prettytable::Cell::new("Creation"),
            prettytable::Cell::new("Modification"),
            prettytable::Cell::new("Cleanup"),
            prettytable::Cell::new("Total"),
            prettytable::Cell::new("Current"),
        ])
    }

    fn rows(&self) -> Vec<Self::Row> {
        vec![prettytable::Row::new(vec![
            prettytable::Cell::new(&Etcd::bytes_to_units(self.allocation)),
            prettytable::Cell::new(&Etcd::bytes_to_units(self.modification)),
            prettytable::Cell::new(&Etcd::bytes_to_units(self.cleanup)),
            prettytable::Cell::new(&Etcd::bytes_to_units(self.total())),
            prettytable::Cell::new(&Etcd::bytes_to_units(self.last)),
        ])]
    }
}

impl TabledData for ResourceSamples {
    type Row = prettytable::Row;

    fn titles(&self) -> Self::Row {
        prettytable::Row::new(
            self.inner()
                .iter()
                .map(|r| prettytable::Cell::new(&r.name()))
                .collect::<Vec<_>>(),
        )
    }

    fn rows(&self) -> Vec<Self::Row> {
        if self.is_empty() {
            return vec![];
        }
        let max_row = self
            .inner()
            .iter()
            .map(|estimation| estimation.points().len())
            .max()
            .unwrap_or(0);

        let mut rows = vec![];
        let mut acc_cache = vec![0; self.len()];
        for row_index in 0 .. max_row {
            let mut row = prettytable::Row::default();
            for (index, estimation) in self.inner().iter().enumerate() {
                let acc = acc_cache.get_mut(index).expect("already validated");

                let point = estimation.points().get(row_index).cloned().unwrap_or(0);
                *acc += point;
                let formatted_point = estimation.format_point(*acc);

                let mut cell = prettytable::Cell::new(&formatted_point);
                cell.align(prettytable::format::Alignment::CENTER);
                row.add_cell(cell);
            }
            rows.push(row);
        }
        rows
    }
}
