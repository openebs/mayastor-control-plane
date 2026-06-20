use clap::Parser;
use deployer_lib::{ListOptions, StartOptions, StopOptions};
use std::{collections::HashSet, io::Seek, os::unix::fs::MetadataExt, time::Duration};
use utils::tracing_telemetry::KeyValue;

const RUST_LOG_SILENCE_DEFAULTS: &str = "h2=info,hyper=info,tower_buffer=info,tower=info,rustls=info,reqwest=info,mio=info,tokio_util=info,async_io=info,polling=info,tonic=info,want=info,bollard=info,stor_port=warn";

fn rust_log_add_quiet_defaults(
    current: Option<tracing_subscriber::EnvFilter>,
) -> tracing_subscriber::EnvFilter {
    let main = match current {
        None => {
            format!("info,{RUST_LOG_SILENCE_DEFAULTS}")
        }
        Some(level) => match level.to_string().as_str() {
            "debug" | "trace" => {
                format!("{level},{RUST_LOG_SILENCE_DEFAULTS}")
            }
            _ => return level,
        },
    };
    tracing_subscriber::EnvFilter::try_new(main).unwrap()
}
fn init_tracing() {
    let filter =
        rust_log_add_quiet_defaults(tracing_subscriber::EnvFilter::try_from_default_env().ok());
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

#[derive(Debug, Parser)]
#[clap(name = utils::package_description!(), version = utils::version_info_string!())]
struct CliArgs {
    #[clap(subcommand)]
    action: Action,
}

#[derive(Debug, Parser)]
#[clap(about = "Deployment actions")]
enum Action {
    Start(Box<SimulateOptions>),
    Stop(StopOptions),
    List(ListOptions),
}

#[derive(Debug, Default, Clone, Parser)]
#[clap(about = "Simulate a cluster from a support bundle")]
struct SimulateOptions {
    /// The generic start options, though be careful not to set anything which conflicts with the
    /// support bundle dump.
    #[clap(flatten)]
    opts: StartOptions,
    /// This can either be the support bundle's tar.gz file or the extracted content.
    /// If it's a tar.gz we'll extract to --untar-path.
    #[clap(long)]
    bundle: std::path::PathBuf,
    /// If `--bundle` is a tar.gz file, then you may override the untar location.
    #[clap(long, default_value = "./bundle")]
    untar_path: std::path::PathBuf,
    /// Disable most start-time reconcilers (other than the pstor cleanup).
    #[clap(long)]
    no_start_event: bool,
    #[clap(skip)]
    umbrella: bool,
}

impl SimulateOptions {
    fn etcd_dump_path(&self) -> std::path::PathBuf {
        if self.umbrella {
            self.untar_path.join("mayastor").join("etcd_dump")
        } else {
            self.untar_path.join("etcd_dump")
        }
    }
    fn setup_bundle(&mut self) {
        let umbrella_bundle = self.bundle.join("mayastor");
        self.umbrella = umbrella_bundle.exists() && umbrella_bundle.is_dir();
    }
    async fn simulate(&mut self, root: &std::path::Path) -> anyhow::Result<()> {
        self.extract()?;
        self.setup_bundle();

        let etcd_dump_path = self.etcd_dump_path();
        let etcd_dump = std::fs::File::open(&etcd_dump_path)?;
        anyhow::ensure!(
            etcd_dump.metadata()?.size() > 0,
            "Unable to simulate cluster with an empty etcd dump at '{}'",
            etcd_dump_path.display()
        );

        // Set the cluster id to match the dump, otherwise the core agent will not pick it up
        let (uid, ns) = self.cluster_info(&etcd_dump)?;
        self.opts.cluster_uid = Some(uid);
        self.opts.cluster_ns = Some(ns);
        // We're going to fake the nodes
        self.opts.io_engines = 0;
        self.opts.idle_io_engines = 1;
        self.opts.idle_io_engine_bin =
            Some(root.join("target/debug/io-engine").display().to_string());
        let full = self.bundle.canonicalize()?;
        self.opts.io_engine_devices = vec![full.display().to_string()];
        self.opts.io_engine_env = Some(vec![KeyValue::new(
            "SIM_BUNDLE",
            self.bundle.display().to_string(),
        )]);
        self.opts.agents_env = Some(vec![
            KeyValue::new("SIM_BUNDLE", self.bundle.display().to_string()),
            KeyValue::new("SIMULATION", "true"),
            KeyValue::new("SIM_NO_START_EVENT", self.no_start_event.to_string()),
        ]);
        // todo: we could fake the app nodes in future improvements
        self.opts.app_nodes = None;
        self.opts.csi_node = false;
        // disable core agent timed reconcilers
        self.opts.reconcile_period = Some(Duration::from_secs(u64::MAX).into());
        self.opts.reconcile_idle_period = Some(Duration::from_secs(u64::MAX).into());
        self.opts.wait_timeout = Some(Duration::from_secs(10).into());
        self.opts.node_deadline = Some(Duration::from_secs(u64::MAX).into());
        // disable etcd-health feature? in case nexus entries are missing
        // Start the deployer
        let composer = self
            .opts
            .start()
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        composer.stop("core").await?;
        let sim_socket = composer.container_ip("io-engine-1");
        let sim_socket_p = self.bundle.join("sim_socket");
        std::fs::write(sim_socket_p, format!("{sim_socket}:10124"))?;
        composer.start("io-engine-1").await?;
        // Load the fake etcd data
        self.load_etcd_dump(&etcd_dump).await?;
        // Restart core agent, so it will reload its etcd data
        composer.start("core").await?;

        Ok(())
    }
    async fn load_etcd_dump(&self, mut etcd_dump: &std::fs::File) -> anyhow::Result<()> {
        use etcd_client::Client;
        use std::io::{BufRead, BufReader};

        let mut etcd = Client::connect(["0.0.0.0:2379"], None).await?;
        etcd_dump.rewind()?;
        let reader = BufReader::new(etcd_dump);

        let mut lines = reader.lines();
        // Ignore old nexus health entries, from mayastor v0
        let ignore = "ffd20a56-8d97-4f68-8049-1cae2294a690".len();

        while let Some(Ok(key)) = lines.next() {
            if key.is_empty() {
                continue;
            }
            let Some(Ok(mut value)) = lines.next() else {
                break;
            };

            let (compact, key) = match key.strip_suffix(":") {
                Some(key) => (false, key),
                None => (true, key.as_str()),
            };

            if !compact && value != "null" {
                while let Some(Ok(more)) = lines.next() {
                    value.push('\n');
                    value.push_str(&more);
                    if more == "}" {
                        break;
                    }
                }
                use std::str::FromStr;
                let json_value = serde_json::Value::from_str(&value)?;
                value = json_value.to_string();
            }

            if key.len() == ignore {
                break;
            }
            if key.contains("/AppNodeSpec/") || key.contains("StoreLeaseLock/CoreAgent") {
                continue;
            }
            etcd.put(key, value, None).await?;
        }
        Ok(())
    }
    fn cluster_info(&self, etcd_dump: &std::fs::File) -> anyhow::Result<(String, String)> {
        use std::io::{BufRead, BufReader};
        let mut reader = BufReader::new(etcd_dump);
        let mut first_line = String::new();
        reader.read_line(&mut first_line)?;

        let parts: Vec<&str> = first_line.split('/').collect();
        let cluster_id = parts
            .iter()
            .position(|&x| x == "clusters")
            .and_then(|i| parts.get(i + 1))
            .ok_or(anyhow::anyhow!(
                "No cluster id in the first etcd_dump line: {first_line}"
            ))?;
        let cluster_ns = parts
            .iter()
            .position(|&x| x == "namespaces")
            .and_then(|i| parts.get(i + 1))
            .ok_or(anyhow::anyhow!(
                "No cluster namespace in the first etcd_dump line: {first_line}"
            ))?;
        Ok((cluster_id.to_string(), cluster_ns.to_string()))
    }
    fn extract(&mut self) -> anyhow::Result<()> {
        println!();
        anyhow::ensure!(
            self.bundle.exists() && (self.bundle.is_dir() || self.bundle.is_file()),
            "--bundle argument must specify an existing file or directory"
        );
        if self.bundle.is_file() {
            let file = std::fs::File::open(&self.bundle)?;
            std::fs::create_dir_all(&self.untar_path)?;

            let gz_decoder = flate2::read::GzDecoder::new(file);
            let mut archive = tar::Archive::new(gz_decoder);

            // Define the directories you want to extract
            let target_dirs = vec![
                "topology/node",
                "topology/pool",
                "topology/volume",
                "etcd_dump",
            ];
            let mut unpacked_dirs = HashSet::<&str>::new();

            for entry in archive.entries()? {
                let mut entry = entry?;
                let entry_path = entry.path()?;

                // support umbrella bundles
                let path = match entry_path.strip_prefix("mayastor") {
                    Ok(path) => path,
                    Err(_) => &entry_path,
                };

                // Check if the path starts with any of the target directories
                if let Some(target_dir) = target_dirs.iter().find(|dir| path.starts_with(dir)) {
                    if !unpacked_dirs.contains(target_dir) {
                        tracing::info!("Extracting {}", entry_path.display());
                    }
                    entry.unpack_in(&self.untar_path)?;

                    unpacked_dirs.insert(target_dir);
                }
            }

            if unpacked_dirs.len() != target_dirs.len() {
                let mut target_dirs = target_dirs;
                target_dirs.retain(|t| !unpacked_dirs.contains(t));
                anyhow::bail!("{target_dirs:?} were not found in the bundle file")
            }
            self.bundle = self.untar_path.clone();
        }
        Ok(())
    }
}

impl Action {
    async fn execute(&mut self, root: &std::path::Path) -> anyhow::Result<()> {
        match self {
            Action::Start(options) => options.simulate(root).await,
            Action::Stop(options) => options.stop().await.map_err(|e| anyhow::anyhow!("{e}")),
            Action::List(options) => options.list().await.map_err(|e| anyhow::anyhow!("{e}")),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let mut cli_args = CliArgs::parse();
    tracing::info!("Using options: {:?}", &cli_args);

    let manifest = std::path::PathBuf::from(std::env!("CARGO_MANIFEST_DIR"));
    let root = manifest.parent().and_then(|p| p.parent()).unwrap();

    composer::initialize(root.display().to_string());
    cli_args.action.execute(root).await?;

    Ok(())
}
