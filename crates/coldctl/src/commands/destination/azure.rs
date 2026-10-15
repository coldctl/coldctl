use crate::output::Output;
use clap::Args as ClapArgs;
use coldctl_core::{destination::azure::AzureConfig, paths::StatePaths, state::destinations};
use std::path::PathBuf;

#[derive(ClapArgs)]
pub struct Args {
    #[arg(long)]
    name: String,
    #[arg(long)]
    account: String,
    #[arg(long)]
    container: String,
    #[arg(long, default_value = "coldctl")]
    prefix: String,
    #[arg(long)]
    endpoint: Option<String>,
    /// Durable local staging directory; only pending batches remain here.
    #[arg(long)]
    staging_path: Option<PathBuf>,
    #[arg(long, default_value = "AZURE_STORAGE_ACCOUNT_KEY")]
    account_key_env: String,
    /// Permit HTTP only for an explicitly configured loopback emulator.
    #[arg(long)]
    allow_http: bool,
    /// Use automatically refreshed cloud identity credentials (standard endpoints only).
    #[arg(long)]
    managed_identity: bool,
    /// Optional user-assigned Azure identity client UUID.
    #[arg(long, requires = "managed_identity")]
    managed_identity_client_id: Option<String>,
}
pub fn run(
    args: Args,
    paths: &StatePaths,
    output: Output,
) -> Result<(), Box<dyn std::error::Error>> {
    let staging = args
        .staging_path
        .unwrap_or_else(|| paths.data_dir.join("staging").join(&args.name));
    let destination = destinations::add_remote(
        paths,
        &args.name,
        &staging,
        AzureConfig {
            managed_identity: args.managed_identity,
            managed_identity_client_id: args.managed_identity_client_id,
            account: args.account,
            container: args.container,
            prefix: args.prefix,
            endpoint: args.endpoint,
            account_key_env: args.account_key_env,
            allow_http: args.allow_http,
        }
        .into(),
    )?;
    match output {
        Output::Json => println!("{}", serde_json::to_string_pretty(&destination)?),
        Output::Table => crate::output::success(&format!(
            "Azure Blob destination '{}' configured. Run destination test to check access.",
            destination.name
        )),
    }
    Ok(())
}
