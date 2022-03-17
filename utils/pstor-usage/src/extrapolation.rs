use crate::{
    etcd::DiskUsage,
    resources::{GenericSample, ResourceSample, ResourceSamples},
    simulation::SimulationOpts,
    Etcd, PrettyPrinter, Printer, Simulation, Url,
};
use structopt::StructOpt;

/// Extrapolate how much storage a cluster would require if it were to run for a specified
/// number of days.
#[derive(StructOpt, Debug)]
pub(crate) struct Extrapolation {
    /// Runtime in days to extrapolate.
    #[structopt(long, short)]
    days: std::num::NonZeroU64,

    /// Maximum number of tabled entries to print.
    #[structopt(long, default_value = "10")]
    tabled_entries: std::num::NonZeroU64,

    /// Show only the usage in the stdout output.
    #[structopt(long)]
    usage_only: bool,

    /// Show the usage in the stdout output as bytes.
    #[structopt(long, requires("usage-only"))]
    usage_bytes: bool,

    /// Extrapolation specific options.
    #[structopt(flatten)]
    opts: ExtrapolationDayOpts,

    /// Show tabled simulation output.
    #[structopt(long)]
    show_simulation: bool,

    /// Customize simulation options.
    #[structopt(subcommand)]
    simulate: Option<Command>,
}

#[derive(StructOpt, Debug)]
enum Command {
    Simulate(SimulationOpts),
}

#[derive(StructOpt, Debug)]
struct ExtrapolationDayOpts {
    /// Volume turnover:
    /// how many volumes are created/deleted every day.
    #[structopt(long, default_value = "50")]
    volume_turnover: u64,

    /// Volume mods:
    /// how many volume modifications (publish/unpublish) are done every day.
    #[structopt(long, default_value = "200")]
    volume_mods: u64,
}

impl Extrapolation {
    /// Extrapolate based on the current parameters.
    pub(crate) async fn extrapolate(&self, external_cluster: &Option<Url>) -> anyhow::Result<()> {
        let mut simulation = match &self.simulate {
            None => Simulation::from(SimulationOpts::from_iter(Vec::<String>::new())),
            Some(Command::Simulate(opts)) => Simulation::from(opts.clone()),
        };
        simulation.set_print_samples(!self.usage_only && self.show_simulation);

        let stats = simulation.simulate(external_cluster).await?;

        let usage_per_vol = (stats.allocation() + stats.cleanup()) / simulation.volumes_allocated();
        let usage_per_mod = stats.modification() / simulation.volumes_modified();

        let daily =
            usage_per_mod * self.opts.volume_mods + usage_per_vol * self.opts.volume_turnover;
        let days = u64::from(self.days);

        if self.usage_only {
            if self.usage_bytes {
                println!("{}", daily * days);
            } else {
                println!("{}", Etcd::bytes_to_units(daily * days));
            }

            Ok(())
        } else {
            let mut max_entries = u64::from(self.tabled_entries).min(days);
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
                "Volume Mods",
                std::iter::repeat(self.opts.volume_mods * days_entry).take(max_entries as usize),
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
                mods.points_mut().push(self.opts.volume_mods * extra_days);
                daily_usage.points_mut().push(daily * extra_days);
            }

            let samples = ResourceSamples::new(vec![days, turnover, mods, daily_usage]);
            let printer = PrettyPrinter::new();
            printer.print(&samples);

            Ok(())
        }
    }
}
