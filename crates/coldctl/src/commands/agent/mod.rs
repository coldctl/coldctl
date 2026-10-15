use clap::Subcommand;
use coldctl_core::paths::StatePaths;

#[derive(Subcommand)]
pub enum Command {
    /// Show persisted Cloud sync attempts, failures and last successful upload.
    SyncStatus,
    /// Observe local metrics, durable failures and optional alert deliveries.
    Monitor(super::monitor::MonitorArgs),
    /// Queue and execute long-running archives with durable checkpoint recovery.
    Scheduler(super::scheduler::SchedulerArgs),
    /// Explicitly upload allowlisted lifecycle metadata to your Cloud workspace.
    Sync {
        /// Cloud origin, for example https://your-console.example.com.
        #[arg(long)]
        cloud_url: Option<String>,
        /// Environment variable containing a lifecycle-scoped Cloud token.
        #[arg(long, default_value = "COLDCTL_CLOUD_TOKEN")]
        token_env: String,
        /// Print the exact metadata payload without network access or token lookup.
        #[arg(long)]
        dry_run: bool,
        /// Permit HTTP to a numeric loopback address for local testing only.
        #[arg(long)]
        allow_local_http: bool,
        /// Keep synchronizing until stopped; run under your service manager.
        #[arg(long, conflicts_with = "dry_run")]
        watch: bool,
        /// Seconds between successful uploads in watch mode (30..86400).
        #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u64).range(30..=86400))]
        interval_seconds: u64,
    },
}
pub async fn run(command: Command, paths: &StatePaths) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        Command::SyncStatus => {
            if !paths.data_dir.join("cloud-sync.sqlite").exists() {
                return Err("No Cloud sync history; run agent sync first".into());
            }
            println!(
                "{}",
                serde_json::to_string_pretty(
                    &coldctl_core::cloud_sync::Store::open(paths)?.status()?
                )?
            );
            Ok(())
        }
        Command::Monitor(args) => super::monitor::run(args, paths).await,
        Command::Scheduler(args) => super::scheduler::run(args, paths).await,
        Command::Sync {
            cloud_url,
            token_env,
            dry_run,
            allow_local_http,
            watch,
            interval_seconds,
        } => {
            if dry_run {
                let pages = coldctl_cloud::paged::collect(paths, env!("CARGO_PKG_VERSION"))?;
                println!("{}", serde_json::to_string_pretty(&pages)?);
                return Ok(());
            }
            let url = cloud_url.ok_or("--cloud-url is required unless --dry-run is used")?;
            super::cloud_sync::run(
                paths,
                &url,
                &token_env,
                allow_local_http,
                watch,
                interval_seconds,
            )
            .await
        }
    }
}
