extern crate core;

mod etcd;
mod pools;
mod printer;
mod resources;
mod volumes;

use crate::{
    etcd::{Etcd, EtcdSampler},
    pools::PoolMgr,
    printer::{PrettyPrinter, Printer, TabledData},
    resources::{ResourceDelete, ResourceMgr, ResourceSamples, ResourceUpdates, Sampler},
    volumes::VolMgr,
};
use anyhow::anyhow;
use openapi::{
    apis::Url,
    clients::tower::{direct::ApiClient, Configuration},
};
use std::time::Duration;
use structopt::StructOpt;

#[derive(structopt::StructOpt, Debug)]
#[structopt(version = utils::package_info!())]
struct CliArgs {
    /// The rest endpoint if reusing a cluster.
    #[structopt(short, long)]
    rest_url: Option<Url>,

    /// Size of the pools.
    #[structopt(long, parse(try_from_str = parse_size::parse_size), default_value = "20MiB")]
    pub pool_size: u64,

    /// Size of the volumes.
    #[structopt(long, parse(try_from_str = parse_size::parse_size), default_value = "5MiB")]
    pub volume_size: u64,

    /// Number of volume replicas.
    #[structopt(long, default_value = "3")]
    pub volume_replicas: u8,

    /// Number of pools per sample.
    #[structopt(short, long, default_value = "10")]
    pub pools: u32,

    /// Number of pool samples.
    #[structopt(long, default_value = "5")]
    pub pool_samples: u32,

    /// Number of volumes per sample.
    #[structopt(short, long, default_value = "20")]
    pub volumes: u32,

    /// Number of volume samples.
    #[structopt(long, default_value = "10")]
    pub vol_samples: u32,

    /// Modifies `N` volumes from each volume samples.
    /// In other words, we will publish/unpublish each `N` volumes from each list of samples.
    /// Please note that this can take quite some time; it's very slow to create
    /// nexuses with remote replicas.
    #[structopt(long, default_value = "2")]
    pub volume_mods: u32,

    /// Use ram based pools instead of files (useful for debugging with small pool allocation).
    /// When using files the /tmp/pool directory will be used.
    #[structopt(long)]
    pub pool_use_malloc: bool,

    /// Skip the output of the total storage usage after allocation of all resources and also after
    /// those resources have been deleted.
    #[structopt(long = "no-total-stats", parse(from_flag = std::ops::Not::not))]
    pub total_stats: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = CliArgs::from_args();
    utils::print_package_info!();

    let (client, _cluster) = match &args.rest_url {
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
    let cleanup = _cluster.is_none() || args.total_stats;

    let etcd = Etcd::new(Url::parse("http://0.0.0.0:2379")?).await?;

    // create some pools as backing for the volumes
    let four_mb = 4096 * 1024;
    let mut volume_size = args.volume_size;
    if (volume_size % four_mb) != 0 {
        volume_size += four_mb - (volume_size % four_mb);
    }
    let pools = if args.volumes > 0 && args.vol_samples > 0 {
        args.volume_replicas as u32
    } else {
        0
    };
    let pool_size = args.vol_samples as u64 * args.volumes as u64 * (volume_size + four_mb); // 4M for the metadata
    let pool_mgr = PoolMgr::new_mgr(&client, pool_size, args.pool_use_malloc).await?;
    let vol_pools = pool_mgr.create(&client, pools).await?;

    // capture the initial database size
    let initial = etcd.db_size().await?;
    let printer = PrettyPrinter::new();

    // sample how much space the volumes take up
    let vol_mgr = VolMgr::new_mgr(args.volume_replicas, args.volume_size).await?;
    let (volumes, vol_results) = EtcdSampler::new_sampler(etcd.clone(), args.vol_samples)
        .sample(&client, args.volumes, &vol_mgr)
        .await?;
    printer.print(&vol_results);

    // sample how much space the pools take up
    let pool_mgr = PoolMgr::new_mgr(&client, args.pool_size, args.pool_use_malloc).await?;
    let (pools, mut pool_results) = EtcdSampler::new_sampler(etcd.clone(), args.pool_samples)
        .sample(&client, args.pools, &pool_mgr)
        .await?;
    printer.print(&pool_results);

    // this is the combined pool and volume usage
    if args.pools > 0 && args.volumes > 0 {
        pool_results.extend(vol_results.into_inner());
        printer.print(&pool_results);
    }

    // capture the database size after we've completed allocating new resources
    let after_alloc = etcd.db_size().await?;

    if args.volume_mods > 0 {
        let mod_results = EtcdSampler::new_sampler(etcd.clone(), args.vol_samples)
            .sample_mods(&client, args.volume_mods, &volumes)
            .await?;
        printer.print(&mod_results);
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

    if args.total_stats {
        printer.print(&RunStats {
            initial,
            after_alloc,
            after_mod,
            after_cleanup,
        });
    }

    Ok(())
}

struct RunStats {
    initial: u64,
    after_alloc: u64,
    after_mod: u64,
    after_cleanup: u64,
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
            prettytable::Cell::new(&Etcd::bytes_to_units(self.after_alloc - self.initial)),
            prettytable::Cell::new(&Etcd::bytes_to_units(self.after_mod - self.after_alloc)),
            prettytable::Cell::new(&Etcd::bytes_to_units(self.after_cleanup - self.after_mod)),
            prettytable::Cell::new(&Etcd::bytes_to_units(self.after_cleanup - self.initial)),
            prettytable::Cell::new(&Etcd::bytes_to_units(self.after_cleanup)),
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
