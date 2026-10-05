use clap::Subcommand;
use coldctl_core::paths::StatePaths;

#[derive(Subcommand)]
pub enum Command {
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
    },
}
pub async fn run(command: Command, paths: &StatePaths) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        Command::Sync {
            cloud_url,
            token_env,
            dry_run,
            allow_local_http,
        } => {
            let snapshot = coldctl_cloud::client::collect(paths, env!("CARGO_PKG_VERSION"))?;
            let bytes = coldctl_cloud::client::encode(&snapshot)?;
            if dry_run {
                println!("{}", String::from_utf8(bytes)?);
                return Ok(());
            }
            let url = cloud_url.ok_or("--cloud-url is required unless --dry-run is used")?;
            let token = std::env::var(token_env)
                .map_err(|_| "Cloud token environment variable is missing")?;
            if token.trim() != token || !token.starts_with("coldctl_") || token.len() > 256 {
                return Err("Invalid Cloud token format".into());
            }
            coldctl_cloud::client::sync(&url, &token, allow_local_http, bytes).await?;
            println!(
                "Lifecycle metadata synchronized. Database contents and credentials stayed local."
            );
            Ok(())
        }
    }
}
