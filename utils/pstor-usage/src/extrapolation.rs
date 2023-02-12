use crate::{
    config::{ClusterConfig, ClusterName, ClusterOpts},
    etcd::DiskUsage,
    printer::TabledData,
    resources::{GenericSample, ResourceSample, ResourceSamples},
    simulation::{RunStats, SimulationOpts},
    Etcd, PrettyPrinter, Printer, Simulation, Url,
};

use clap::Parser;
use itertools::Itertools;
use std::collections::HashMap;

/// Extrapolate how much storage a cluster would require if it were to run for a specified
/// number of days.
#[derive(Parser, Debug)]
pub(crate) struct Extrapolation {
    /// Runtime in days to extrapolate.
    #[clap(long, short)]
    days: std::num::NonZeroU64,

    /// Maximum number of table entries to print.
    #[clap(long, default_value = "10")]
    table_entries: std::num::NonZeroU64,

    /// Show only the usage in the stdout output.
    #[clap(long)]
    usage_only: bool,

    /// Show the usage in the stdout output as bytes.
    #[clap(long, requires("usage-only"))]
    usage_bytes: bool,

    /// Extrapolation specific options.
    #[clap(flatten)]
    opts: ExtrapolationDayOpts,

    #[clap(flatten)]
    cluster_opts: ClusterOpts,

    /// Show tabulated simulation output.
    #[clap(long)]
    show_simulation: bool,

    /// Customize simulation options.
    #[clap(subcommand)]
    simulate: Option<Command>,
}

#[derive(Parser, Debug)]
enum Command {
    Simulate(SimulationOpts),
}

#[derive(Parser, Debug)]
struct ExtrapolationDayOpts {
    /// Volume turnover:
    /// how many volumes are created/deleted every day.
    #[clap(long, default_value = "50")]
    volume_turnover: u64,

    /// Volume attach cycle:
    /// how many volume modifications (publish/unpublish) are done every day.
    #[clap(long, default_value = "200")]
    volume_attach_cycle: u64,
}

const DEFAULT_CLUSTER_NAME: &str = "extrapolated";
impl From<SimulationOpts> for ClusterConfig {
    fn from(simul: SimulationOpts) -> Self {
        ClusterConfig::new(
            simul.replicas(),
            simul.volume_turnover(),
            simul.volume_attach_cycles(),
        )
    }
}

impl Extrapolation {
    fn init(&mut self, config: &ClusterConfig) {
        self.opts = ExtrapolationDayOpts {
            volume_turnover: config.turnover(),
            volume_attach_cycle: config.mods(),
        }
    }
    fn clusters(&mut self) -> anyhow::Result<HashMap<ClusterName, ClusterConfig>> {
        Ok(if let Some(config) = self.cluster_opts.cluster()? {
            HashMap::from([config])
        } else {
            self.cluster_opts.clusters().unwrap_or_else(|| {
                HashMap::from([(
                    DEFAULT_CLUSTER_NAME.to_string(),
                    ClusterConfig::from(SimulationOpts::default()),
                )])
            })
        })
    }
    fn config_to_simulation(&self, config: &ClusterConfig) -> Simulation {
        let config_simulation = self.cluster_opts.simulation();
        let opts = match (config_simulation, &self.simulate) {
            (Some(opts), _) => opts,
            (None, Some(Command::Simulate(opts))) => opts.clone(),
            (None, None) => SimulationOpts::default(),
        };
        let opts = opts.with_replicas(config.replicas());
        let mut simulation = Simulation::from(opts);
        simulation.set_print_samples(!self.usage_only && self.show_simulation);
        simulation
    }
    /// Extrapolate based on the current parameters.
    pub(crate) async fn extrapolate(
        &mut self,
        external_cluster: &Option<Url>,
    ) -> anyhow::Result<()> {
        let mut simulations = HashMap::new();
        let mut results = ExtrapolationResults::default();
        for (name, config) in self.clusters()? {
            let simulation = self.config_to_simulation(&config);
            let stats = match simulations.get(&simulation) {
                None => {
                    let stats = simulation.simulate(external_cluster).await?;
                    simulations.insert(simulation.clone(), stats.clone());
                    stats
                }
                Some(stats) => stats.clone(),
            };
            self.init(&config);

            results.push(name, self.extrapolate_from_simulation(simulation, stats));
        }
        if self.usage_only {
            for (_, result) in results
                .results
                .into_iter()
                .sorted_by(|(_, a), (_, b)| a.total_usage.cmp(&b.total_usage))
            {
                if self.usage_bytes {
                    println!("{}", result.total_usage);
                } else {
                    println!("{}", Etcd::bytes_to_units(result.total_usage));
                }
            }
        } else {
            let printer = PrettyPrinter::new();
            printer.print(&results);
        }

        Ok(())
    }
    fn extrapolate_from_simulation(
        &self,
        simulation: Simulation,
        stats: RunStats,
    ) -> ExtrapolationResult {
        let usage_per_vol = (stats.allocation() + stats.cleanup()) / simulation.volumes_allocated();
        let usage_per_mod = stats.modification() / simulation.volumes_modified();

        let daily = usage_per_mod * self.opts.volume_attach_cycle
            + usage_per_vol * self.opts.volume_turnover;
        let days = u64::from(self.days);
        let total_usage = daily * days;

        let samples = if self.usage_only {
            ResourceSamples::default()
        } else {
            let mut max_entries = u64::from(self.table_entries).min(days);
            let days_entry = days / max_entries;
            let mut extra_days = days - days_entry * max_entries;
            if extra_days > 0 {
                max_entries -= 1;
            }

            let mut days = Box::new(GenericSample::new(
                "Days",
                std::iter::repeat(days_entry).take(max_entries as usize),
            ));
            let mut turnover = Box::new(GenericSample::new(
                "Volume Turnover",
                std::iter::repeat(self.opts.volume_turnover * days_entry)
                    .take(max_entries as usize),
            ));
            let mut mods = Box::new(GenericSample::new(
                "Volume Attach/Detach",
                std::iter::repeat(self.opts.volume_attach_cycle * days_entry)
                    .take(max_entries as usize),
            ));
            let mut daily_usage = Box::new(DiskUsage::new(
                std::iter::repeat(days_entry * daily).take(max_entries as usize),
            ));

            if extra_days > 0 {
                extra_days += days_entry;
                days.points_mut().push(extra_days);
                turnover
                    .points_mut()
                    .push(self.opts.volume_turnover * extra_days);
                mods.points_mut()
                    .push(self.opts.volume_attach_cycle * extra_days);
                daily_usage.points_mut().push(daily * extra_days);
            }

            ResourceSamples::new(vec![days, turnover, mods, daily_usage])
        };
        ExtrapolationResult {
            total_usage,
            samples,
        }
    }
}

#[derive(Default)]
struct ExtrapolationResults {
    results: HashMap<ClusterName, ExtrapolationResult>,
}
struct ExtrapolationResult {
    total_usage: u64,
    samples: ResourceSamples,
}

impl ExtrapolationResults {
    fn push(&mut self, name: ClusterName, result: ExtrapolationResult) {
        self.results.insert(name, result);
    }
}

impl TabledData for ExtrapolationResults {
    type Row = prettytable::Row;

    fn titles(&self) -> Self::Row {
        prettytable::Row::new(vec![crate::new_cell("Cluster"), crate::new_cell("Results")])
    }

    fn rows(&self) -> Vec<Self::Row> {
        self.results
            .iter()
            .sorted_by(|(_, a), (_, b)| a.total_usage.cmp(&b.total_usage))
            .map(|(name, result)| {
                let titles = result.samples.titles();
                let rows = result.samples.rows();
                let mut table = prettytable::Table::init(rows);
                table.set_titles(titles);
                table.set_format(*prettytable::format::consts::FORMAT_BOX_CHARS);
                prettytable::Row::new(vec![
                    prettytable::Cell::new(name),
                    prettytable::Cell::new(&table.to_string()),
                ])
            })
            .collect()
    }
}
